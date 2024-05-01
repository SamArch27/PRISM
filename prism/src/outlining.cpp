#include "outlining.hpp"
#include "cfg_code_generator.hpp"
#include "cfg_to_ast.hpp"
#include "compiler.hpp"
#include "dead_code_elimination.hpp"
#include "file.hpp"
#include "instructions.hpp"
#include "liveness_analysis.hpp"
#include "merge_regions.hpp"
#include "pipeline_pass.hpp"
#include "remove_unused_variable.hpp"
#include "ssa_destruction.hpp"
#include "udf_transpiler_extension.hpp"
#include "utils.hpp"

/**
 * Robust way to find the outgoing branch out of this list of blocks
 */
static Set<BasicBlock *>
getNextBasicBlock(const Vec<BasicBlock *> &basicBlocks) {
  Set<BasicBlock *> blockSet;
  Set<BasicBlock *> nextBlocks;
  for (auto *block : basicBlocks) {
    blockSet.insert(block);
  }
  for (auto *block : basicBlocks) {
    for (auto *succ : block->getSuccessors()) {
      if (blockSet.count(succ) == 0) {
        nextBlocks.insert(succ);
      }
    }
  }
  return nextBlocks;
}

void OutliningPass::outlineFunction(Function &f) {
  drawGraph(f.getCFGString(), "cfg_outlined");
  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<DeadCodeEliminationPass>(), Make<SSADestructionPass>(),
      Make<RemoveUnusedVariablePass>());
  ssaDestructionPipeline->runOnFunction(f);

  if (duckdb::optimizerPassOnMap.at("PrintOutlinedUDF") == true) {
    std::cout << "============================" << std::endl;
    std::cout << "Outlined UDF " + f.getFunctionName() + " :" << std::endl
              << std::endl;
    PLpgSQLGenerator generator(compiler.getConfig());
    auto result = generator.run(f);
    std::cout << result.code << std::endl;
    std::cout << "============================" << std::endl;
  }

  INFO(fmt::format("Transpiling UDF {}...", f.getFunctionName()));
  CFGCodeGenerator codeGenerator(compiler.getConfig());
  auto res = codeGenerator.run(f);

  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  INFO("Compiling the UDF...");
  compileUDF();
  // load the compiled library
  INFO("Installing and loading the UDF...");
  loadUDF(*compiler.getConnection());
  compiler.getUdfCount()++;
}

static bool allBlocksNaive(const Vec<BasicBlock *> &basicBlocks) {
  // check if all the basic blocks are naive (just jmps)
  // check if there is at least a conditional or a loop within the blocks
  for (auto *block : basicBlocks) {
    if (dynamic_cast<ConditionalRegion *>(block->getRegion()) ||
        dynamic_cast<LoopRegion *>(block->getRegion())) {
      return false;
    }
  }
  return true;
}

/**
 * returns outlined or not
 */
bool OutliningPass::outlineBasicBlocks(Vec<BasicBlock *> blocksToOutline,
                                       Function &f) {
  // the first block should not contain phi nodes
  bool changed = true;
  while (changed) {
    if (blocksToOutline.empty()) {
      return false;
    }
    changed = false;
    for (auto &inst : *(blocksToOutline.front())) {
      if (dynamic_cast<PhiNode *>(&inst)) {
        blocksToOutline.erase(blocksToOutline.begin());
        changed = true;
        break;
      }
    }
  }

  if (blocksToOutline.empty()) {
    return false;
  }

  if (allBlocksNaive(blocksToOutline)) {
    return false;
  }

  auto nextBasicBlocks = getNextBasicBlock(blocksToOutline);

  if (nextBasicBlocks.size() > 1) {
    ERROR("Should not have a case where there are multiple next blocks for an "
          "outlined region");
  }

  BasicBlock *nextBasicBlock =
      nextBasicBlocks.empty() ? nullptr : *nextBasicBlocks.begin();

  bool hasReturn = false;
  for (auto *block : blocksToOutline) {
    if (block->getSuccessors().size() == 0) {
      hasReturn = true;
      break;
    }
  }

  if (nextBasicBlock != nullptr && hasReturn) {
    INFO("Control logic does not go to end but has return, this pattern cannot "
         "be outlined for now.");
    return false;
  } else if (nextBasicBlock == nullptr && !hasReturn) {
    EXCEPTION(fmt::format("Control logic goes to end but no return."));
  }
  // rest two cases are both valid
  bool outliningEndRegion = nextBasicBlock == nullptr && hasReturn;

  // get live variable going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();

  auto *regionHeader = blocksToOutline.front();
  auto liveIn = liveness->getBlockLiveIn(regionHeader);

  // get live variable going out of the region
  Set<const Variable *> liveOut;
  Set<const Variable *> returnVars;

  if (!outliningEndRegion) {
    liveOut = liveness->getBlockLiveIn(nextBasicBlock);

    // get the return variables:
    // variables that are live out of the region and defined in the region

    for (auto *var : liveOut) {
      if (liveIn.count(var) == 0) {
        returnVars.insert(var);
      }
    }
    if (returnVars.size() != 1) {
      INFO(fmt::format(
          "Do not support one outlined region to return {} variables",
          returnVars.size()));
      return false;
    }
  }

  auto *returnVariable = returnVars.empty() ? nullptr : *returnVars.begin();

  String newFunctionName =
      fmt::format("{}_outlined_{}", f.getFunctionName(), outlinedCount);
  Vec<const Variable *> newFunctionArgs;
  for (auto *var : liveIn) {
    newFunctionArgs.push_back(var);
  }
  std::sort(newFunctionArgs.begin(), newFunctionArgs.end(),
            [](const Variable *v1, const Variable *v2) {
              return v1->getName() < v2->getName();
            });
  Type returnType =
      outliningEndRegion ? f.getReturnType() : returnVariable->getType();

  Map<BasicBlock *, BasicBlock *> blockMap;
  auto newFunction = f.partialCloneAndRename(
      newFunctionName, newFunctionArgs, returnType, blocksToOutline, blockMap);

  if (!outliningEndRegion) {
    // add explicit return of the return variable to the end of the function
    auto *returnBlock = newFunction->makeBasicBlock("returnBlock");
    returnBlock->addInstruction(Make<ReturnInst>(newFunction->bindExpression(
        returnVariable->getName(), returnVariable->getType())));

    newFunction->renameBasicBlocks(nextBasicBlock, returnBlock);
    blockMap[nextBasicBlock] = returnBlock;
  }

  if (duckdb::optimizerPassOnMap.at("PrintOutlinedUDF") == true) {
    // find the common root region
    const Region *rootRegion = regionHeader->getRegion();
    // check if all the blocks are in the same region
    for (auto *block : blocksToOutline) {
      Region *parentRegion = block->getRegion();
      while (parentRegion != nullptr) {
        if (parentRegion == rootRegion) {
          break;
        }
        parentRegion = parentRegion->getParentRegion();
      }
      if (parentRegion != rootRegion) {
        EXCEPTION("Blocks to outline are not in the root region. Please do "
                  "pragma disable('PrintOutlinedUDF') to continue.");
      }
    }
    // clone the region
    FunctionCloneAndRenameHelper cloneHelper;
    cloneHelper.basicBlockMap = blockMap;
    auto clonedRegion = cloneHelper.cloneAndRenameRegion(rootRegion);
    auto newRegion = Make<SequentialRegion>(
        newFunction->getEntryBlock(),
        Make<SequentialRegion>(newFunction->getBlockFromLabel("preheader"),
                               std::move(clonedRegion)));
    newFunction->setRegion(std::move(newRegion));
  } else {
    newFunction->setRegion(nullptr);
  }

  outlineFunction(*newFunction);

  String args = "";
  for (auto &arg : newFunctionArgs) {
    if (args != "") {
      args += ", ";
    }
    args += arg->getName();
  }
  auto result = f.bindExpression(newFunctionName + "(" + args + ")",
                                 newFunction->getReturnType());

  if (outliningEndRegion) {
    auto retInst = Make<ReturnInst>(std::move(result));
    ASSERT(nextBasicBlock == nullptr, "Must not have a next basic block!");
    nextBasicBlock = f.makeBasicBlock();
    nextBasicBlock->addInstruction(std::move(retInst));
    nextBasicBlock->setRegion(Make<LeafRegion>(nextBasicBlock).release());
  } else {
    auto assign = Make<Assignment>(returnVariable, std::move(result));
    ASSERT(nextBasicBlock != nullptr, "NextBasicBlock cannot be nullptr!!");
    nextBasicBlock->insertBefore(nextBasicBlock->begin(), std::move(assign));
    nextBasicBlock->getRegion()->getParentRegion()->releaseNestedRegions();
  }

  auto &preds = regionHeader->getPredecessors();
  ASSERT(preds.size() == 1, "Must have exactly one predecessor for region!");
  auto *pred = preds.front();

  // get the predecessor of nextBasicBlock that is in the region
  BasicBlock *nextPred = nullptr;
  Set<BasicBlock *> blocksToOutlineSet;
  for (auto *block : blocksToOutline) {
    blocksToOutlineSet.insert(block);
  }
  for (auto *pred : nextBasicBlock->getPredecessors()) {
    if (blocksToOutlineSet.count(pred) > 0) {
      if (nextPred != nullptr) {
        INFO("Do not support outlined region to have multiple outgoing "
             "branches.");
        return false;
      }
      nextPred = pred;
    }
  }
  pred->renameBasicBlock(regionHeader, nextBasicBlock, nextPred);

  auto *replacement = nextBasicBlock->getRegion();
  auto *currentRegion = regionHeader->getRegion();
  auto *parentRegion = currentRegion->getParentRegion();

  parentRegion->replaceNestedRegion(currentRegion, replacement);

  Set<Region *> regionsToRemove;
  for (auto *block : blocksToOutline) {
    regionsToRemove.insert(block->getRegion());
  }
  parentRegion->removeNestedRegions(regionsToRemove);

  for (auto *block : blocksToOutline) {
    f.removeBasicBlock(block);
  }

  outlinedCount++;
  return false;
}

bool OutliningPass::runOnRegion(SelectRegions &containsSelect,
                                const Region *region, Function &f,
                                Vec<BasicBlock *> &queuedBlocks,
                                size_t &fallthroughStart,
                                Set<const BasicBlock *> &blockOutlined) {
  auto queueBlock = [&](BasicBlock *block) {
    if (block != f.getEntryBlock() && blockOutlined.count(block) == 0) {
      queuedBlocks.push_back(block);
      blockOutlined.insert(block);
    }
  };

  auto queueBlocksFromRegion = [&](const Region *region) {
    if (const auto *conditionalRegion =
            dynamic_cast<const ConditionalRegion *>(region)) {
      fallthroughStart = queuedBlocks.size();
      queueBlock(conditionalRegion->getHeader());
      for (const Region *region : conditionalRegion->getNestedRegions()) {
        for (auto *block : region->getBasicBlocks()) {
          queueBlock(block);
        }
      }
      if (conditionalRegion->getFalseRegion() != nullptr) {
        fallthroughStart = -1;
      }
      return;
    } else {
      for (auto *block : region->getBasicBlocks()) {
        queueBlock(block);
      }
      fallthroughStart = -1;
    }
  };

  auto outlineQueuedBlocks = [&]() {
    if (fallthroughStart != (size_t)-1) {
      // only outline the blocks before the fallthrough
      queuedBlocks.resize(fallthroughStart);
    }
    outlineBasicBlocks(queuedBlocks, f);
    queuedBlocks.clear();
    fallthroughStart = -1;
  };

  // traverse the regions top down
  if (containsSelect.at(region) == true) {
    if (auto *sequentialRegion =
            dynamic_cast<const SequentialRegion *>(region)) {
      // sequential region is an exception because other part of the region
      // does not affect regions inside it being outlined
      auto *header = sequentialRegion->getHeader();
      if (!header->hasSelect()) {
        queueBlock(header);
        fallthroughStart = -1;
      } else {
        outlineQueuedBlocks();
      }
      for (auto *nestedRegion : sequentialRegion->getNestedRegions()) {
        if (containsSelect.at(nestedRegion) == true) {
          runOnRegion(containsSelect, nestedRegion, f, queuedBlocks,
                      fallthroughStart, blockOutlined);
        } else {
          queueBlocksFromRegion(nestedRegion);
        }
      }
    } else {
      outlineQueuedBlocks();
      if (auto *recursiveRegion =
              dynamic_cast<const RecursiveRegion *>(region)) {
        for (auto *nestedRegion : recursiveRegion->getNestedRegions()) {
          runOnRegion(containsSelect, nestedRegion, f, queuedBlocks,
                      fallthroughStart, blockOutlined);
          outlineQueuedBlocks();
        }
      }
    }
  } else {
    queueBlocksFromRegion(region);
  }
  return true;
}

/**
 * Return true if any successor region including itself changed.
 *
 * Successor regions are nested regions except for conditional regions,
 * which are the regions whose header if the successor of conditional block.
 */
bool computeSelectRegionsHelper(const Region *region,
                                SelectRegions &selectRegions,
                                Set<const Region *> &visitedRegions) {
  bool regionHasSelect = false;
  bool successorChanged = false;
  visitedRegions.insert(region);

  if (selectRegions.find(region) == selectRegions.end()) {
    selectRegions[region] = false;
  }

  bool regionPrevHasSelect = selectRegions.at(region);

  auto *header = region->getHeader();

  if (header->hasSelect()) {
    regionHasSelect = true;
  }

  if (auto *rec = dynamic_cast<const RecursiveRegion *>(region)) {
    for (auto *nested : rec->getNestedRegions()) {
      if (visitedRegions.count(nested) == 0) {
        successorChanged =
            successorChanged ||
            computeSelectRegionsHelper(nested, selectRegions, visitedRegions);
      }
      if (selectRegions.at(nested) == true) {
        regionHasSelect = true;
      }
    }
  }

  selectRegions[region] = regionHasSelect;
  return (regionPrevHasSelect != regionHasSelect) || successorChanged;
}

SelectRegions OutliningPass::computeSelectRegions(const Region *region) const {
  SelectRegions selectRegions;
  auto *header = region->getHeader();
  selectRegions[region] = header->hasSelect();

  if (auto *rec = dynamic_cast<const RecursiveRegion *>(region)) {
    for (auto *nested : rec->getNestedRegions()) {
      for (auto &[selectRegion, flag] : computeSelectRegions(nested)) {
        selectRegions[selectRegion] = flag;
        selectRegions[region] |= flag;
      }
    }
  }
  return selectRegions;
}

bool OutliningPass::runOnFunction(Function &f) {
  drawGraph(f.getCFGString(), "cfg");
  Vec<BasicBlock *> queuedBlocks;
  auto containsSelect = computeSelectRegions(f.getRegion());
  Set<const BasicBlock *> blockOutlined;
  size_t fallthroughStart = -1;
  runOnRegion(containsSelect, f.getRegion(), f, queuedBlocks, fallthroughStart,
              blockOutlined);
  outlineBasicBlocks(queuedBlocks, f);
  return false;
}
