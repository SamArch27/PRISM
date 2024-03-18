#include "outlining.hpp"
#include "break_phi_interference.hpp"
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
Set<BasicBlock *> getNextBasicBlock(const Vec<BasicBlock *> &basicBlocks) {
  Set<BasicBlock *> blockSet;
  Set<BasicBlock *> nextBlocks;
  for (auto *block : basicBlocks) {
    blockSet.insert(block);
  }
  BasicBlock *nextBlock = nullptr;
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
  COUT << "Optimizing function " << f.getFunctionName() << ENDL;
  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<BreakPhiInterferencePass>(), Make<SSADestructionPass>(),
      Make<AggressiveMergeRegionsPass>());
  ssaDestructionPipeline->runOnFunction(f);

  COUT << fmt::format("Transpiling UDF {}...", f.getFunctionName()) << ENDL;
  compiler.getUdfCount()++;
  CFGCodeGenerator codeGenerator(compiler.getConfig());
  auto res = codeGenerator.run(f);

  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  COUT << "Compiling the UDF..." << ENDL;
  compileUDF();
  // load the compiled library
  COUT << "Installing and loading the UDF..." << ENDL;
  loadUDF(*compiler.getConnection());
}

bool allBlocksNaive(const Vec<BasicBlock *> &basicBlocks) {
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
bool OutliningPass::outlineBasicBlocks(Vec<BasicBlock *> basicBlocks,
                                       Function &f) {
  if (basicBlocks.empty()) {
    return false;
  }

  if (allBlocksNaive(basicBlocks)) {
    COUT << "Ignoring naive region" << ENDL;
    return false;
  }

  COUT << "Outlining basic blocks: " << ENDL;
  for (auto *block : basicBlocks) {
    COUT << block->getLabel() << " ";
  }
  COUT << ENDL;

  auto nextBasicBlocks = getNextBasicBlock(basicBlocks);

  for (auto *nextBasicBlock : nextBasicBlocks) {
    std::cout << "Next basic block: " << nextBasicBlock->getLabel()
              << std::endl;
  }

  if (nextBasicBlocks.size() > 1) {
    ERROR("Should not have a case where there are multiple next blocks for an "
          "outlined region");
  }

  BasicBlock *nextBasicBlock =
      nextBasicBlocks.empty() ? nullptr : *nextBasicBlocks.begin();

  bool hasReturn = false;
  for (auto *block : basicBlocks) {
    if (block->getSuccessors().size() == 0) {
      hasReturn = true;
      break;
    }
  }

  std::cout << "hasReturn is set to: " << (hasReturn ? "true" : "false")
            << std::endl;

  if (nextBasicBlock != nullptr && hasReturn) {
    COUT << "Cannot outline region with return" << ENDL;
    return false;
  } else if (nextBasicBlock == nullptr && !hasReturn) {
    EXCEPTION(fmt::format("Control logic goes to end but no return."));
  }
  // rest two cases are both valid
  bool returnRegion = nextBasicBlock == nullptr && hasReturn;

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  const auto useDefs = useDefAnalysis.getUseDefs();

  // get live variable going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();
  std::cout << *liveness << std::endl;
  auto *regionHeader = basicBlocks.front();
  auto liveIn = liveness->getBlockLiveIn(regionHeader);

  // get live variable going out of the region
  Set<const Variable *> liveOut;
  Set<const Variable *> returnVars;
  if (!returnRegion) {
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

  COUT << ENDL;
  COUT << "Return variables: " << ENDL;
  for (auto *var : returnVars) {
    COUT << var->getName() << " ";
  }
  COUT << ENDL;
  COUT << "Input variables: " << ENDL;
  for (auto *var : liveIn) {
    COUT << var->getName() << " ";
  }
  COUT << ENDL;
  COUT << ENDL;

  String newFunctionName =
      fmt::format("{}_outlined_{}", f.getFunctionName(), outlinedCount);
  Vec<const Variable *> newFunctionArgs;
  for (auto *var : liveIn) {
    newFunctionArgs.push_back(var);
  }
  Type returnType =
      returnRegion ? f.getReturnType() : (*returnVars.begin())->getType();
  auto newFunction = f.partialCloneAndRename(newFunctionName, newFunctionArgs,
                                             returnType, basicBlocks);

  COUT << "Outlined Function: " << newFunction->getFunctionName() << ENDL;
  COUT << *newFunction << ENDL;

  if (!returnRegion) {
    // add explicit return of the return variable to the end of the function
    auto *returnBlock = newFunction->makeBasicBlock("return");
    returnBlock->addInstruction(Make<ReturnInst>(newFunction->bindExpression(
        (*returnVars.begin())->getName(), (*returnVars.begin())->getType())));

    newFunction->renameBasicBlocks({{nextBasicBlock, returnBlock}});
  }

  outlineFunction(*newFunction);

  // TODO: rewrite the original function
  String args = "";
  for (auto &arg : newFunctionArgs) {
    if (args != "") {
      args += ", ";
    }
    args += arg->getName();
  }

  auto retInst = Make<ReturnInst>(f.bindExpression(
      newFunctionName + "(" + args + ")", newFunction->getReturnType()));
  std::cout << "Return Instruction to Attach: " << *retInst;

  // remove all instructions from regionHeader
  for (auto it = regionHeader->begin(); it != regionHeader->end();) {
    regionHeader->removeInst(it);
  }

  // add the new return instruction
  regionHeader->addInstruction(std::move(retInst));

  // replace the region
  auto newLeafRegion = Make<LeafRegion>(regionHeader);
  auto *currentRegion =
      dynamic_cast<RecursiveRegion *>(regionHeader->getRegion());
  ASSERT(currentRegion != nullptr,
         "Current region should not be a leaf region!");
  auto *parentRegion = currentRegion->getParentRegion();
  parentRegion->replaceNestedRegion(currentRegion, newLeafRegion.release());

  std::cout << "AFTER OUTLINING THE NEW FUNCTION LOOKS LIKE: " << std::endl;
  std::cout << f << std::endl;

  outlinedCount++;
  return false;
}

bool OutliningPass::runOnRegion(const Region *rootRegion, Function &f) {
  // traverse the region top down
  Queue<const Region *> worklist;
  worklist.push(rootRegion);
  // Vec<const Region *> regionsToOutline;
  Vec<BasicBlock *> basicBlocksToOutline;
  while (!worklist.empty()) {
    auto *region = worklist.front();
    worklist.pop();
    if (!region->hasSelect()) {

      if (region == f.getRegion()) {
        COUT << "The entire function " << f.getFunctionName()
             << " is a compilable!" << ENDL << ENDL;
        return true;
      }

      // outline the region
      // ASSERT(dynamic_cast<const SequentialRegion *>(region),
      //        "Unexpected region type");
      // regionsToOutline.push_back(region);
      for (auto *block : region->getBasicBlocks()) {
        basicBlocksToOutline.push_back(block);
      }

    } else {
      if (auto *loopRegion = dynamic_cast<const LoopRegion *>(region)) {
        ASSERT(loopRegion->getHeader()->getPredecessors().size() >= 2,
               "Loop region should have at least two predecessors");
        outlineBasicBlocks(basicBlocksToOutline, f);
        basicBlocksToOutline.clear();
        // if (!loopRegion->getHeader()->hasSelect()) {
        //   regionsToOutline.push();
        // }
        for (auto *nestedRegion : loopRegion->getNestedRegions()) {
          runOnRegion(nestedRegion, f);
        }
      } else if (auto *conditionalRegion =
                     dynamic_cast<const ConditionalRegion *>(region)) {
        outlineBasicBlocks(basicBlocksToOutline, f);
        basicBlocksToOutline.clear();

        for (auto *nestedRegion : conditionalRegion->getNestedRegions()) {
          runOnRegion(nestedRegion, f);
        }
      } else if (auto *sequentialRegion =
                     dynamic_cast<const SequentialRegion *>(region)) {
        // sequential region is an exception because other part of the region
        // does not affect regions inside it being outlined
        if (!sequentialRegion->getHeader()->hasSelect()) {
          basicBlocksToOutline.push_back(sequentialRegion->getHeader());
        }
        for (auto *nestedRegion : sequentialRegion->getNestedRegions()) {
          if (nestedRegion->hasSelect()) {
            outlineBasicBlocks(basicBlocksToOutline, f);
            basicBlocksToOutline.clear();
            runOnRegion(nestedRegion, f);
          } else {
            for (auto *block : nestedRegion->getBasicBlocks()) {
              basicBlocksToOutline.push_back(block);
            }
          }
        }
      } else if (auto *leafRegion = dynamic_cast<const LeafRegion *>(region)) {
        outlineBasicBlocks(basicBlocksToOutline, f);
      } else {
        ASSERT(false, "Unexpected region type");
      }
    }
  }

  outlineBasicBlocks(basicBlocksToOutline, f);

  return true;
}

bool OutliningPass::runOnFunction(Function &f) {
  // drawGraph(f.getCFGString(), "cfg_" + f.getFunctionName() + ".dot");
  // drawGraph(f.getRegionString(), "region_" + f.getFunctionName() + ".dot");
  std::cout << "OUTLINING ORIGINAL FUNCTION" << std::endl;
  std::cout << f << std::endl;

  // get live variable going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();
  std::cout << *liveness << std::endl;

  return runOnRegion(f.getRegion(), f);
}
