#include "function.hpp"
#include "dominator_dataflow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "use_def.hpp"
#include "used_variable_finder.hpp"
#include "utils.hpp"

void Function::makeDuckDBContext() {
  std::stringstream createTableString;
  createTableString << "CREATE TABLE tmp(";
  std::stringstream insertTableString;
  std::stringstream insertTableSecondRow;
  insertTableString << "INSERT INTO tmp VALUES(";
  insertTableSecondRow << "(";

  bool first = true;
  for (const auto &[name, variable] : bindings) {
    auto type = variable->getType();
    createTableString << (first ? "" : ", ");
    insertTableString << (first ? "" : ", ");
    insertTableSecondRow << (first ? "" : ", ");
    if (first) {
      first = false;
    }
    createTableString << name << " " << type;
    insertTableString << "(NULL) ";
    insertTableSecondRow << type.getDefaultValue(true);
  }
  if (bindings.empty()) {
    createTableString << "dummy INT";
    insertTableString << "(NULL)";
    insertTableSecondRow << "0";
  }
  createTableString << ");";
  insertTableString << "),";
  insertTableSecondRow << ");";

  // Create commands
  String createTableCommand = createTableString.str();
  String insertTableCommand =
      insertTableString.str() + insertTableSecondRow.str();

  // CREATE TABLE
  auto res = conn->Query(createTableCommand);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }

  // INSERT (NULL,NULL,...)
  res = conn->Query(insertTableCommand);
  if (res->HasError()) {
    destroyDuckDBContext();
    EXCEPTION(res->GetError());
  }
}

void Function::destroyDuckDBContext() {
  String dropTableCommand = "DROP TABLE tmp;";
  auto res = conn->Query(dropTableCommand);
}

Own<SelectExpression> Function::renameVarInExpression(
    const SelectExpression *original,
    const Map<const Variable *, const Variable *> &oldToNew) {
  auto replacedText = original->getRawSQL();
  for (auto &[oldVar, newVar] : oldToNew) {
    std::regex wordRegex("\\b" + oldVar->getName() + "\\b");
    replacedText =
        std::regex_replace(replacedText, wordRegex, newVar->getName());
  }
  return bindExpression(replacedText)->clone();
}

Own<SelectExpression> Function::replaceVarWithExpression(
    const SelectExpression *original,
    const Map<const Variable *, const SelectExpression *> &oldToNew) {
  auto replacedText = original->getRawSQL();
  for (auto &[oldVar, newExpr] : oldToNew) {
    std::regex wordRegex("\\b" + oldVar->getName() + "\\b");
    replacedText =
        std::regex_replace(replacedText, wordRegex, newExpr->getRawSQL());
  }
  return bindExpression(replacedText)->clone();
}

Own<SelectExpression> Function::bindExpression(const String &expr,
                                               bool needContext) {
  if (needContext) {
    destroyDuckDBContext();
    makeDuckDBContext();
  }

  String selectExpressionCommand;
  if (toUpper(expr).find("SELECT") == String::npos)
    if (needContext)
      selectExpressionCommand = "SELECT " + expr + " FROM tmp;";
    else
      selectExpressionCommand = "SELECT " + expr;
  else if (needContext) {
    // insert tmp to the from clause
    auto fromPos = toUpper(expr).find(" FROM ");
    selectExpressionCommand = expr;
    selectExpressionCommand.insert(fromPos + 6, " tmp, ");
  }

  auto clientContext = conn->context.get();
  clientContext->config.enable_optimizer = true;
  auto &config = duckdb::DBConfig::GetConfig(*clientContext);
  std::set<duckdb::OptimizerType> flagsShouldDelete;

  if (config.options.disabled_optimizers.count(
          duckdb::OptimizerType::STATISTICS_PROPAGATION) == 0)
    flagsShouldDelete.insert(duckdb::OptimizerType::STATISTICS_PROPAGATION);
  config.options.disabled_optimizers.insert(
      duckdb::OptimizerType::STATISTICS_PROPAGATION);

  // SELECT <expr> FROM tmp
  Shared<LogicalPlan> boundExpression;
  Shared<duckdb::Binder> plannerBinder;
  try {
    boundExpression = clientContext->ExtractPlan(selectExpressionCommand, true,
                                                 plannerBinder);

    for (auto &type : flagsShouldDelete) {
      config.options.disabled_optimizers.erase(type);
    }
  } catch (const std::exception &e) {
    for (auto &type : flagsShouldDelete) {
      config.options.disabled_optimizers.erase(type);
    }

    destroyDuckDBContext();
    EXCEPTION(e.what());
  }

  duckdb::UsedVariableFinder usedVariableFinder("tmp", plannerBinder);
  usedVariableFinder.VisitOperator(*boundExpression);

  // for each used variable, bind it to a Variable*
  Set<const Variable *> usedVariables;
  for (const auto &varName : usedVariableFinder.getUsedVariables()) {
    usedVariables.insert(getBinding(varName));
  }

  return Make<SelectExpression>(expr, std::move(boundExpression),
                                usedVariables);
}

Map<Instruction *, Instruction *> Function::replaceUsesWithExpr(
    const Map<const Variable *, const SelectExpression *> &oldToNew,
    const Own<UseDefs> &useDefs) {

  Map<Instruction *, Instruction *> newInstructions;
  for (auto &[oldVar, newVar] : oldToNew) {
    Set<Instruction *> toReplace;
    for (auto *use : useDefs->getUses(oldVar)) {
      for (auto *op : use->getOperands()) {
        useDefs->removeUse(op, use);
      }
      if (auto *def = use->getResultOperand()) {
        useDefs->removeDef(def, use);
      }
      toReplace.insert(use);
    }
    for (auto *inst : toReplace) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst)) {
        auto *rhs = assign->getRHS();
        // replace RHS with new expression
        auto newAssign = Make<Assignment>(
            assign->getLHS(), replaceVarWithExpression(rhs, oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newAssign));
        inst = newInstructions[inst];
      } else if (auto *returnInst = dynamic_cast<const ReturnInst *>(inst)) {
        auto newReturn = Make<ReturnInst>(
            replaceVarWithExpression(returnInst->getExpr(), oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newReturn));
        inst = newInstructions[inst];
      } else if (auto *branchInst = dynamic_cast<const BranchInst *>(inst)) {
        auto newBranch = Make<BranchInst>(
            branchInst->getIfTrue(), branchInst->getIfFalse(),
            replaceVarWithExpression(branchInst->getCond(), oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newBranch));
        inst = newInstructions[inst];
      } else if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
        VecOwn<SelectExpression> newRHS;
        for (auto *op : phi->getRHS()) {
          newRHS.emplace_back(replaceVarWithExpression(op, oldToNew));
        }
        auto newPhi = Make<PhiNode>(phi->getLHS(), std::move(newRHS));
        newInstructions[inst] = inst->replaceWith(std::move(newPhi));
        inst = newInstructions[inst];
      } else {
        std::cout << *inst << std::endl;
        ERROR("Unhandled case in Function::replaceUsesWith!");
      }

      // add uses for the new instruction
      for (auto *op : inst->getOperands()) {
        useDefs->addUse(op, inst);
      }
      if (auto *def = inst->getResultOperand()) {
        useDefs->addDef(def, inst);
      }
    }
  }
  return newInstructions;
}

void Function::mergeBasicBlocks(BasicBlock *top, BasicBlock *bottom) {
  // Replace the top region with the bottom region
  auto *bottomRegion = bottom->getRegion();
  auto *topRegion = bottomRegion->getParentRegion();
  auto *parent = topRegion->getParentRegion();
  parent->replaceNestedRegion(topRegion, bottomRegion);

  // copy instructions from top into bottom (in reverse order)
  Vec<Instruction *> topInstructions;
  for (auto &inst : *top) {
    topInstructions.push_back(&inst);
  }
  std::reverse(topInstructions.begin(), topInstructions.end());

  for (auto *inst : topInstructions) {
    if (!inst->isTerminator()) {
      bottom->insertBefore(bottom->begin(), inst->clone());
    }
  }

  // update predecessors of bottom to jump to top
  for (auto *pred : top->getPredecessors()) {
    if (auto *terminator = dynamic_cast<BranchInst *>(pred->getTerminator())) {
      // replace the branch instruction to target the bottom block
      if (terminator->isUnconditional()) {
        terminator->replaceWith(Make<BranchInst>(bottom));
      } else {
        auto *newTrue = terminator->getIfTrue();
        newTrue = (newTrue == top) ? bottom : newTrue;

        auto *newFalse = terminator->getIfFalse();
        newFalse = (newFalse == top) ? bottom : newFalse;

        terminator->replaceWith(Make<BranchInst>(
            newTrue, newFalse, terminator->getCond()->clone()));
      }
      // update the predecessors "successors"
      pred->removeSuccessor(top);
      pred->addSuccessor(bottom);

      // add this predecessor as a predecessor to the bottom block
      bottom->addPredecessor(pred);
    }
  }

  // finally remove the basic block from the function
  removeBasicBlock(top);
}

void Function::removeBasicBlock(BasicBlock *toRemove) {
  auto it = basicBlocks.begin();
  while (it != basicBlocks.end()) {
    if (it->get() == toRemove) {
      // make all predecessors not reference this
      for (auto &pred : toRemove->getPredecessors()) {
        pred->removeSuccessor(toRemove);
      }
      // make all successors not reference this
      for (auto &succ : toRemove->getSuccessors()) {
        succ->removePredecessor(toRemove);
      }
      // finally erase the block
      it = basicBlocks.erase(it);
    } else {
      ++it;
    }
  }
}

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
    ASSERT(variableMap.find(var) != variableMap.end(),
           fmt::format("Variable {} not found in variableMap", var->getName()));
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
  ASSERT(variableMap.find(phi.getLHS()) != variableMap.end(),
         fmt::format("Variable {} not found in variableMap", phi.getLHS()->getName()));
  return Make<PhiNode>(variableMap.at(phi.getLHS()), std::move(newRHS));
}

template <>
Own<Assignment>
FunctionCloneAndRenameHelper::cloneAndRename(const Assignment &assign) {
  ASSERT(variableMap.find(assign.getLHS()) != variableMap.end(),
         fmt::format("Variable {} not found in variableMap", assign.getLHS()->getName()));
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
  ASSERT(basicBlockMap.find(branch.getIfTrue()) != basicBlockMap.end(),
         fmt::format("BasicBlock {} not found in basicBlockMap", branch.getIfTrue()->getLabel()));
  auto trueBlock = basicBlockMap.at(branch.getIfTrue());
  ASSERT(basicBlockMap.find(branch.getIfFalse()) != basicBlockMap.end(),
         fmt::format("BasicBlock {} not found in basicBlockMap", branch.getIfFalse()->getLabel()));
  auto falseBlock = basicBlockMap.at(branch.getIfFalse());
  return Make<BranchInst>(trueBlock, falseBlock,
                          cloneAndRename(*branch.getCond()));
}

template <>
Own<Instruction>
FunctionCloneAndRenameHelper::cloneAndRename(const Instruction &inst) {
  if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
    return cloneAndRename(*assign);
  } else if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
    return cloneAndRename(*phi);
  } else if (auto *ret = dynamic_cast<const ReturnInst *>(&inst)) {
    return cloneAndRename(*ret);
  } else if (auto *branch = dynamic_cast<const BranchInst *>(&inst)) {
    return cloneAndRename(*branch);
  } else {
    std::cout << "Unhandled case in FunctionCloneAndRenameHelper::cloneAndRename!"
              << std::endl;
    std::cout << inst << std::endl;
    ERROR("Unhandled case in FunctionCloneAndRenameHelper::cloneAndRename!");
  }
}

Own<Function> Function::partialCloneAndRename(
    const String &newName, const Vec<const Variable *> &newArgs,
    const Type &newReturnType, const Vec<const Region *> regions) const {
  Map<const Variable *, const Variable *> variableMap;
  Map<const BasicBlock *, BasicBlock *> basicBlockMap;
  auto newFunction = Make<Function>(conn, newName, newReturnType);
  for (const auto &arg : newArgs) {
    newFunction->addArgument(arg->getName(), arg->getType());
    variableMap[arg] = newFunction->getBinding(arg->getName());
  }

  // create the entry block
  auto entry = newFunction->makeBasicBlock("entry");
  // there is no variable in the new function, every data pass in by argument
  for (const auto &region : regions) {
    for (const auto &basicBlock : region->getBasicBlocks()) {
      auto newBlock = newFunction->makeBasicBlock(basicBlock->getLabel());
      basicBlockMap[basicBlock] = newBlock;
    }
  }

  FunctionCloneAndRenameHelper cloneHelper{variableMap, basicBlockMap};
  for (auto &[oldBasicBlock, newBasicBlock] : basicBlockMap) {
    for (const auto &inst : *oldBasicBlock) {
      auto newInst = cloneHelper.cloneAndRename(inst);
      newBasicBlock->addInstruction(std::move(newInst));
    }
  }

  // let the entry jump to the first block of the first region
  ASSERT(basicBlockMap.find(regions.front()->getHeader()) != basicBlockMap.end(),
         fmt::format("BasicBlock {} not found in basicBlockMap", regions.front()->getHeader()->getLabel()));
  newFunction->getEntryBlock()->addInstruction(Make<BranchInst>(basicBlockMap.at(regions.front()->getHeader())));
  
  return std::move(newFunction);
}