#include "outlining.hpp"
#include "cfg_code_generator.hpp"
#include "compiler.hpp"
#include "file.hpp"
#include "instructions.hpp"
#include "liveness_analysis.hpp"
#include "merge_regions.hpp"
#include "pipeline_pass.hpp"
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
  auto ssaDestructionPipeline = Make<PipelinePass>(Make<SSADestructionPass>());
  ssaDestructionPipeline->runOnFunction(f);

  INFO(fmt::format("Transpiling UDF {}...", f.getFunctionName()));
  compiler.getUdfCount()++;
  CFGCodeGenerator codeGenerator(compiler.getConfig());
  auto res = codeGenerator.run(f);

  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  INFO("Compiling the UDF...");
  compileUDF();
  // load the compiled library
  INFO("Installing and loading the UDF...");
  loadUDF(*compiler.getConnection());
}

static bool allBlocksNaive(const Vec<BasicBlock *> &basicBlocks) {
  // check if all the basic blocks are naive (just jmps)
  for (auto *block : basicBlocks) {
    size_t instCount = 0;
    for (auto &inst : *block) {
      instCount++;
    }
    if (instCount > 1) {
      return false;
    }
    if (block->getSuccessors().size() > 1) {
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
    INFO("Control logic goes to end but has return, this pattern cannot be "
         "outlined for now.");
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
    ASSERT(returnVars.size() == 1,
           fmt::format("Do not support one region to return {} variables",
                       returnVars.size()));
  }

  auto *returnVariable = returnVars.empty() ? nullptr : *returnVars.begin();

  String newFunctionName =
      fmt::format("{}_outlined_{}", f.getFunctionName(), outlinedCount);
  Vec<const Variable *> newFunctionArgs;
  for (auto *var : liveIn) {
    newFunctionArgs.push_back(var);
  }
  Type returnType =
      outliningEndRegion ? f.getReturnType() : returnVariable->getType();
  Map<BasicBlock *, BasicBlock *> tmp;
  auto newFunction = f.partialCloneAndRename(newFunctionName, newFunctionArgs,
                                             returnType, blocksToOutline, tmp);

  if (!outliningEndRegion) {
    // add explicit return of the return variable to the end of the function
    auto *returnBlock = newFunction->makeBasicBlock("returnBlock");
    returnBlock->addInstruction(Make<ReturnInst>(newFunction->bindExpression(
        returnVariable->getName(), returnVariable->getType())));

    newFunction->renameBasicBlocks(nextBasicBlock, returnBlock);
  }

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
      ASSERT(nextPred == nullptr, "Should not have multiple preds in region");
      nextPred = pred;
    }
  }
  pred->renameBasicBlock(regionHeader, nextBasicBlock, nextPred);

  auto *replacement = nextBasicBlock->getRegion();
  auto *currentRegion = regionHeader->getRegion();
  auto *parentRegion = currentRegion->getParentRegion();

  parentRegion->replaceNestedRegion(currentRegion, replacement);

  drawGraph(f.getCFGString(), "cfg");

  for (auto *block : blocksToOutline) {
    f.removeBasicBlock(block);
  }

  outlinedCount++;
  return false;
}

bool OutliningPass::runOnRegion(SelectRegions &containsSelect,
                                const Region *region, Function &f,
                                Vec<BasicBlock *> &queuedBlocks,
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
      queueBlock(conditionalRegion->getHeader());
      for (const Region *region : conditionalRegion->getSuccessorRegions()) {
        for (auto *block : region->getBasicBlocks()) {
          queueBlock(block);
        }
      }
      return;
    }
    for (auto *block : region->getBasicBlocks()) {
      queueBlock(block);
    }
  };

  auto outlineQueuedBlocks = [&]() {
    outlineBasicBlocks(queuedBlocks, f);
    queuedBlocks.clear();
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
      } else {
        outlineQueuedBlocks();
      }
      for (auto *nestedRegion : sequentialRegion->getNestedRegions()) {
        if (containsSelect.at(nestedRegion) == true) {
          runOnRegion(containsSelect, nestedRegion, f, queuedBlocks,
                      blockOutlined);
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
                      blockOutlined);
        }
      }
    }
  } else {
    queueBlocksFromRegion(region);
  }

  outlineQueuedBlocks();
  return true;
}

/**
 * return if any region has successorChanged
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
    for (auto *nested : rec->getSuccessorRegions()) {
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
  Set<const Region *> visitedRegions;
  while (computeSelectRegionsHelper(region, selectRegions, visitedRegions)) {
    visitedRegions.clear();
  }

  return selectRegions;
}

bool OutliningPass::runOnFunction(Function &f) {
  drawGraph(f.getCFGString(), "cfg");
  Vec<BasicBlock *> queuedBlocks;
  auto containsSelect = computeSelectRegions(f.getRegion());
  Set<const BasicBlock *> blockOutlined;
  runOnRegion(containsSelect, f.getRegion(), f, queuedBlocks, blockOutlined);
  return false;
}
