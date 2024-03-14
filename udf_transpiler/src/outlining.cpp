#include "outlining.hpp"
#include "file.hpp"
#include "instructions.hpp"
#include "liveness_analysis.hpp"
#include "utils.hpp"
#include "udf_transpiler_extension.hpp"
#include "compiler.hpp"


// Own<Function> FunctionCloneAndRenameHelper::cloneAndRename(const Function &f)
// {
//   Map<const Variable *, const Variable *> variableMap;
//   Map<const BasicBlock *, const BasicBlock *> BasicBlockMap;
//   auto newFunction =
//       Make<Function>(f.getConnection(), f.getFunctionName(),
//       f.getReturnType());
//   for (const auto &arg : f.getArguments()) {
//     newFunction->addArgument(arg->getName(), arg->getType());
//   }
//   for (const auto &var : f.getVariables()) {
//     newFunction->addVariable(var->getName(), var->getType(), var->isNull());
//   }
//   // for (const auto &decl : declarations) {
//   //   newFunction->addVarInitialization(decl->getLHS(),
//   //   decl->getRHS()->clone());
//   // }

//   // for (const auto &block : basicBlocks) {
//   //   auto newBlock = newFunction->makeBasicBlock(block->getLabel());
//   //   for (const auto &inst : *block) {
//   //     newBlock->addInstruction(inst.clone());
//   //   }
//   // }
//   // newFunction->setRegion(functionRegion->clone());
//   return newFunction;
// }

/**
 * Get the next region of this region
 * Should check if there is only one successor
 */
const Region *getNextRegion(const Region *region) {
  if (region->getParentRegion() == nullptr) {
    return nullptr;
  }
  auto *parent = region->getParentRegion();
  // make sure parent is only a sequential region or a loop region
  ASSERT(dynamic_cast<SequentialRegion *>(parent) ||
             dynamic_cast<LoopRegion *>(parent),
         "Unexpected parent region type");
  const auto &nestedRegions = parent->getNestedRegions();
  for (size_t i = 0; i < nestedRegions.size(); i++) {
    if (nestedRegions[i] == region) {
      if (i == nestedRegions.size() - 1) {
        return getNextRegion(parent);
      }
      return nestedRegions[i + 1];
    }
  }
  return nullptr;
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

void OutliningPass::outlineFunction(Function &f){
  compiler.getUdfCount()++;
  auto res = compiler.runOnFunction(f);
  COUT << "Transpiling the UDF..." << ENDL;
  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  COUT << "Compiling the UDF..." << ENDL;
  compileUDF();
  // load the compiled library
  COUT << "Installing and loading the UDF..." << ENDL;
  loadUDF(*compiler.getConnection());
}

bool OutliningPass::outlineRegion(Vec<const Region *> regions, Function &f,
                                  bool returnRegion) {
  if (regions.empty()) {
    return false;
  }

  COUT << "Outlining regions: " << ENDL;
  for (auto *region : regions) {
    COUT << region->getRegionLabel() << " ";
  }

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto &useDefs = useDefAnalysis.getUseDefs();

  // get live variable going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();
  auto liveIn = liveness->getBlockLiveIn(regions.front()->getHeader());

  for (auto &block : f) {
    liveIn = liveness->getBlockLiveIn(&block);
    COUT << "Live in: " << block.getLabel() << ENDL;
    for (auto *var : liveIn) {
      COUT << var->getName() << " ";
    }
    COUT << ENDL;
  }

  liveIn = liveness->getBlockLiveIn(regions.front()->getHeader());

  // get live variable going out of the region
  Set<const Variable *> liveOut;
  if (!returnRegion) {
    auto *nextRegion = getNextRegion(regions.back());
    liveOut = liveness->getBlockLiveIn(nextRegion->getHeader());
  }
  // get the return variables:
  // variables that are live out of the region and defined in the region
  Set<const Variable *> returnVars;

  if (!returnRegion) {
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
                                             returnType, regions);

  // when going out of ssa, do you need region in place?
  COUT << "Outlined Function: " << newFunction->getFunctionName() << ENDL;
  COUT << *newFunction << ENDL;

  outlineFunction(*newFunction);

  // TODO: rewrite the original function


  outlinedCount++;
  return false;
}

bool OutliningPass::runOnRegion(const Region *rootRegion, Function &f) {
  // traverse the region top down
  Queue<const Region *> worklist;
  worklist.push(rootRegion);
  Vec<const Region *> regionsToOutline;
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
      regionsToOutline.push_back(region);

      // get the next region
      auto *nextRegion = getNextRegion(region);
      if (nextRegion) {
        worklist.push(nextRegion);
      } else {
        // if there is no next region, this is the last region
        break;
      }
    } else {
      // now keep it simple, do not outline region that has select
      // based on the region type
      if (auto *loopRegion = dynamic_cast<const LoopRegion *>(region)) {
        ASSERT(loopRegion->getHeader()->getPredecessors().size() >= 2,
               "Loop region should have at least two predecessors");
        outlineRegion(regionsToOutline, f, false);
        regionsToOutline = Vec<const Region *>();
        // if (!loopRegion->getHeader()->hasSelect()) {
        //   regionsToOutline.push();
        // }
        for (auto *nestedRegion : loopRegion->getNestedRegions()) {
          runOnRegion(nestedRegion, f);
        }
      } else if (auto *conditionalRegion =
                     dynamic_cast<const ConditionalRegion *>(region)) {
        outlineRegion(regionsToOutline, f, false);
        regionsToOutline = Vec<const Region *>();

        for (auto *nestedRegion : conditionalRegion->getNestedRegions()) {
          runOnRegion(nestedRegion, f);
        }
      } else if (auto *sequentialRegion =
                     dynamic_cast<const SequentialRegion *>(region)) {
        bool outlined = false;
        for (auto *nestedRegion : sequentialRegion->getNestedRegions()) {
          if (nestedRegion->hasSelect()) {
            outlined = true;
            outlineRegion(regionsToOutline, f, false);
            regionsToOutline = Vec<const Region *>();
            runOnRegion(nestedRegion, f);
          } else {
            regionsToOutline.push_back(nestedRegion);
          }
        }
      } else if (auto *leafRegion = dynamic_cast<const LeafRegion *>(region)) {
        outlineRegion(regionsToOutline, f, false);
      } else {
        ASSERT(false, "Unexpected region type");
      }
    }
  }

  outlineRegion(regionsToOutline, f, true);

  return true;
}

bool OutliningPass::runOnFunction(Function &f) {
  drawGraph(f.getCFGString(), "cfg_" + f.getFunctionName() + ".dot");
  drawGraph(f.getRegionString(), "region_" + f.getFunctionName() + ".dot");

  return runOnRegion(f.getRegion(), f);
}
