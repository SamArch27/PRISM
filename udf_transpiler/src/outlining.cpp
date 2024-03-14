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
 * Get the next region of this region
 * Should check if there is only one successor
 */
// const Region *getNextRegion(const Region *region) {
//   if (region->getParentRegion() == nullptr) {
//     return nullptr;
//   }
//   auto *parent = region->getParentRegion();
//   // make sure parent is only a sequential region or a loop region
//   ASSERT(dynamic_cast<SequentialRegion *>(parent) ||
//              dynamic_cast<LoopRegion *>(parent),
//          "Unexpected parent region type");
//   const auto &nestedRegions = parent->getNestedRegions();
//   for (size_t i = 0; i < nestedRegions.size(); i++) {
//     if (nestedRegions[i] == region) {
//       if (i == nestedRegions.size() - 1) {
//         return getNextRegion(parent);
//       }
//       return nestedRegions[i + 1];
//     }
//   }
//   return nullptr;
// }

/**
 * A robust way is to find the outgoing branch out of this list of blocks
 */
const BasicBlock *
getNextBasicBlock(const Vec<const BasicBlock *> &basicBlocks) {
  Set<const BasicBlock *> blockSet;
  for (auto *block : basicBlocks) {
    blockSet.insert(block);
  }
  BasicBlock *nextBlock = nullptr;
  for (auto *block : basicBlocks) {
    for (auto *succ : block->getSuccessors()) {
      if (blockSet.count(succ) == 0) {
        if (nextBlock != nullptr && nextBlock != succ) {
          ERROR("There should be only one next block of the whole outlined "
                "region.");
        }
        nextBlock = succ;
      }
    }
  }
  return nextBlock;
}

// void breakBasicBlockWithSelect(Function &f) {
//   Vec<Instruction *> worklist;
//   for (auto &block : f) {
//     for (auto &inst : block) {
//       if (inst.hasSelect()) {
//         worklist.push_back(&inst);
//       }
//     }
//   }

//   // for (auto *inst : worklist) {
//   //   auto *block = inst->getParent();
//   //   auto *nextBlock = block->splitBlock(inst);
//   //   auto *newBlock = f.makeBasicBlock();
//   //   newBlock->addInstruction(Make<BranchInst>(nextBlock));
//   //   for (auto *succ : block->getSuccessors()) {
//   //     succ->replacePredecessor(block, newBlock);
//   //   }
//   // }
// }

void OutliningPass::outlineFunction(Function &f) {
  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<BreakPhiInterferencePass>(), Make<SSADestructionPass>(),
      Make<AggressiveMergeRegionsPass>());
  ssaDestructionPipeline->runOnFunction(f);

  compiler.getUdfCount()++;
  CFGCodeGenerator codeGenerator(compiler.getConfig());
  auto res = codeGenerator.run(f);

  COUT << "Transpiling the UDF..." << ENDL;
  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  COUT << "Compiling the UDF..." << ENDL;
  compileUDF();
  // load the compiled library
  COUT << "Installing and loading the UDF..." << ENDL;
  loadUDF(*compiler.getConnection());
}

bool allBlocksNaive(const Vec<const BasicBlock *> &basicBlocks) {
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
bool OutliningPass::outlineBasicBlocks(Vec<const BasicBlock *> basicBlocks,
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

  auto *nextBasicBlock = getNextBasicBlock(basicBlocks);
  bool hasReturn = false;
  for (auto *block : basicBlocks) {
    if (block->getSuccessors().size() == 0) {
      hasReturn = true;
      break;
    }
  }

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
  auto &useDefs = useDefAnalysis.getUseDefs();

  // get live variable going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();
  auto liveIn = liveness->getBlockLiveIn(basicBlocks.front());

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
      fmt::format("{}_outlined{}", f.getFunctionName(), outlinedCount);
  Vec<const Variable *> newFunctionArgs;
  for (auto *var : liveIn) {
    newFunctionArgs.push_back(var);
  }
  Type returnType =
      returnRegion ? f.getReturnType() : (*returnVars.begin())->getType();
  auto newFunction = f.partialCloneAndRename(newFunctionName, newFunctionArgs,
                                             returnType, basicBlocks);

  // when going out of ssa, do you need region in place?
  COUT << "Outlined Function: " << newFunction->getFunctionName() << ENDL;
  COUT << *newFunction << ENDL;

  if (!returnRegion) {
    // add explicit return of the return variable to the end of the function
    auto *returnBlock = newFunction->makeBasicBlock("return");
    returnBlock->addInstruction(Make<ReturnInst>(
        newFunction->bindExpression((*returnVars.begin())->getName())));

    newFunction->renameBasicBlocks({{nextBasicBlock, returnBlock}});
  }

  outlineFunction(*newFunction);

  // TODO: rewrite the original function

  outlinedCount++;
  return false;
}

bool OutliningPass::runOnRegion(const Region *rootRegion, Function &f) {
  // traverse the region top down
  Queue<const Region *> worklist;
  worklist.push(rootRegion);
  // Vec<const Region *> regionsToOutline;
  Vec<const BasicBlock *> basicBlocksToOutline;
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
        COUT << "1. Pushing block " << block->getLabel() << ENDL;
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
          COUT << "2. Pushing block "
               << sequentialRegion->getHeader()->getLabel() << ENDL;
          basicBlocksToOutline.push_back(sequentialRegion->getHeader());
        }
        for (auto *nestedRegion : sequentialRegion->getNestedRegions()) {
          if (nestedRegion->hasSelect()) {
            outlineBasicBlocks(basicBlocksToOutline, f);
            basicBlocksToOutline.clear();
            runOnRegion(nestedRegion, f);
          } else {
            for (auto *block : nestedRegion->getBasicBlocks()) {
              COUT << "3. Pushing block " << block->getLabel() << ENDL;
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
  drawGraph(f.getCFGString(), "cfg_" + f.getFunctionName() + ".dot");
  drawGraph(f.getRegionString(), "region_" + f.getFunctionName() + ".dot");

  return runOnRegion(f.getRegion(), f);
}
