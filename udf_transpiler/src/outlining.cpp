#include "outlining.hpp"
#include "instructions.hpp"
#include "liveness_dataflow.hpp"
#include "utils.hpp"

template <>
Own<Variable>
FunctionCloneAndRenameHelper::cloneAndRename(const Variable &var) {
  return Make<Variable>(var.getName(), var.getType(), var.isNull());
}

template <>
Own<SelectExpression>
FunctionCloneAndRenameHelper::cloneAndRename(const SelectExpression &expr) {
  Set<const Variable *> newUsedVariables;
  for (auto *var : expr.getUsedVariables()) {
    newUsedVariables.insert(variableMap.at(var));
  }
  return Make<SelectExpression>(expr.getRawSQL(), expr.getLogicalPlanShared(),
                                newUsedVariables);
}

template <>
Own<PhiNode> FunctionCloneAndRenameHelper::cloneAndRename(const PhiNode &phi) {
  VecOwn<SelectExpression> newRHS;
  for (auto &op : phi.getRHS()) {
    newRHS.emplace_back(cloneAndRename(*op));
  }
  return Make<PhiNode>(variableMap.at(phi.getLHS()), std::move(newRHS));
}

template <>
Own<Assignment>
FunctionCloneAndRenameHelper::cloneAndRename(const Assignment &assign) {
  return Make<Assignment>(variableMap.at(assign.getLHS()),
                          cloneAndRename(*assign.getRHS()));
}

template <>
Own<ReturnInst>
FunctionCloneAndRenameHelper::cloneAndRename(const ReturnInst &ret) {
  return Make<ReturnInst>(cloneAndRename(*ret.getExpr()));
}

template <>
Own<BranchInst>
FunctionCloneAndRenameHelper::cloneAndRename(const BranchInst &branch) {
  auto trueBlock = BasicBlockMap.at(branch.getIfTrue());
  auto falseBlock = BasicBlockMap.at(branch.getIfFalse());
  return Make<BranchInst>(trueBlock, falseBlock,
                          cloneAndRename(*branch.getCond()));
}

template <>
Own<BasicBlock>
FunctionCloneAndRenameHelper::cloneAndRename(const BasicBlock &block) {
  auto newBlock = Make<BasicBlock>(block.getLabel());
  for (auto &inst : block) {
    newBlock->addInstruction(cloneAndRename(inst));
  }
  return newBlock;
}

Own<Function> FunctionCloneAndRenameHelper::cloneAndRename(const Function &f) {
  Map<const Variable *, const Variable *> variableMap;
  Map<const BasicBlock *, const BasicBlock *> BasicBlockMap;
  auto newFunction =
      Make<Function>(f.getConnection(), f.getFunctionName(), f.getReturnType());
  for (const auto &arg : f.getArguments()) {
    newFunction->addArgument(arg->getName(), arg->getType());
  }
  for (const auto &var : f.getVariables()) {
    newFunction->addVariable(var->getName(), var->getType(), var->isNull());
  }
  // for (const auto &decl : declarations) {
  //   newFunction->addVarInitialization(decl->getLHS(),
  //   decl->getRHS()->clone());
  // }

  // for (const auto &block : basicBlocks) {
  //   auto newBlock = newFunction->makeBasicBlock(block->getLabel());
  //   for (const auto &inst : *block) {
  //     newBlock->addInstruction(inst.clone());
  //   }
  // }
  // newFunction->setRegion(functionRegion->clone());
  return newFunction;
}

void outlineRegion(Region *region, Function &f, const Region *nextRegion,
                   bool returnRegion) {
  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto &useDefs = useDefAnalysis.getUseDefs();

  // get live variable going into the region
  Set<const Variable *> liveIn;
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();
  liveIn = liveness->getBlockLiveIn(region->getHeader());

  // get live variable going out of the region
  Set<const Variable *> liveOut;
  if (!returnRegion) {
    liveOut = liveness->getBlockLiveIn(nextRegion->getHeader());
  }
  // get the return variables:
  // variables that are live out of the region and defined in the region
  Set<const Variable *> returnVars;
  for (auto *var : liveOut) {
    if (liveIn.count(var) == 0) {
      returnVars.insert(var);
    }
  }
  if (!returnRegion) {
    ASSERT(returnVars.size() <= 1,
           "Do not support one region to return multiple variables");
  }

  // create a new function
  // auto newFunction = f
  COUT << "Outlining region: " << region->getRegionLabel() << std::endl;
  COUT << "Return variables: ";
  for (auto *var : returnVars) {
    COUT << var->getName() << " ";
  }
  COUT << "Input variables: ";
  for (auto *var : liveIn) {
    COUT << var->getName() << " ";
  }
  COUT << ENDL;
}

/**
 * Get the next region of this region
 * Should check if there is only one successor
*/
const Region* getNextRegion(const Region *region){
  if(region->getParentRegion() == nullptr){
    return nullptr;
  }
  auto *parent = region->getParentRegion();
  // make sure parent is only a sequential region or a loop region
  ASSERT(dynamic_cast<SequentialRegion *>(parent) || dynamic_cast<LoopRegion *>(parent),
         "Unexpected parent region type");
  const auto &nestedRegions = parent->getNestedRegions();
  for(size_t i = 0; i < nestedRegions.size(); i++){
    if(nestedRegions[i] == region){
      if(i == nestedRegions.size() - 1){
        return nullptr;
      }
      return nestedRegions[i + 1];
    }
  }
  return nullptr;
}

bool OutliningPass::runOnFunction(Function &f) {
  // traverse the region top down
  Queue<Region *> worklist;
  worklist.push(f.getEntryBlock()->getRegion());
  while (!worklist.empty()) {
    auto *region = worklist.front();
    worklist.pop();
    if (!region->hasSelect()) {
      // outline the region
      ASSERT(dynamic_cast<SequentialRegion *>(region),
             "Unexpected region type");
      auto nextRegion = getNextRegion(region);
      outlineRegion(region, f, nextRegion, nextRegion == nullptr);
      // start from the beginning
      worklist = Queue<Region *>();
      worklist.push(f.getRegion());
    }
  }
}
