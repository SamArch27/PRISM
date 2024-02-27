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
  if(bindings.empty()){
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
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
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

Own<SelectExpression> Function::bindExpression(const String &expr, bool needContext) {
  if(needContext){
    destroyDuckDBContext();
    makeDuckDBContext();
  }
  
  String selectExpressionCommand;
  if (toUpper(expr).find("SELECT") == String::npos)
    if(needContext)
      selectExpressionCommand = "SELECT " + expr + " FROM tmp;";
    else
      selectExpressionCommand = "SELECT " + expr;
  else if (needContext){
    // insert tmp to the from clause
    auto fromPos = expr.find(" FROM ");
    selectExpressionCommand = expr;
    selectExpressionCommand.insert(fromPos + 6, " tmp, ");
  }
  auto clientContext = conn->context.get();
  clientContext->config.enable_optimizer = true;
  auto &config = duckdb::DBConfig::GetConfig(*clientContext);
  std::set<duckdb::OptimizerType> flagsShouldDelete;

  if (config.options.disabled_optimizers.count(
          duckdb::OptimizerType::STATISTICS_PROPAGATION) == 0)
    flagsShouldDelete.insert(
        duckdb::OptimizerType::STATISTICS_PROPAGATION);
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
  Vec<const Variable *> usedVariables;
  for (const auto &varName : usedVariableFinder.getUsedVariables()) {
    usedVariables.push_back(getBinding(varName));
  }

  return Make<SelectExpression>(expr, std::move(boundExpression),
                                usedVariables);
}

Map<Instruction *, Instruction *> Function::replaceUsesWithVar(
    const Map<const Variable *, const Variable *> &oldToNew,
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
        auto newAssign = Make<Assignment>(assign->getLHS(),
                                          renameVarInExpression(rhs, oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newAssign));
        inst = newInstructions[inst];
      } else if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
        auto newArguments = phi->getRHS();
        for (auto &arg : newArguments) {
          if (oldToNew.find(arg) == oldToNew.end()) {
            continue;
          }
          arg = oldToNew.at(arg);
        }
        auto newPhi = Make<PhiNode>(phi->getLHS(), newArguments);
        newInstructions[inst] = inst->replaceWith(std::move(newPhi));
        inst = newInstructions[inst];
      } else if (auto *returnInst = dynamic_cast<const ReturnInst *>(inst)) {
        auto newReturn = Make<ReturnInst>(
            renameVarInExpression(returnInst->getExpr(), oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newReturn));
        inst = newInstructions[inst];
      } else if (auto *branchInst = dynamic_cast<const BranchInst *>(inst)) {
        auto newBranch = Make<BranchInst>(
            branchInst->getIfTrue(), branchInst->getIfFalse(),
            renameVarInExpression(branchInst->getCond(), oldToNew));
        newInstructions[inst] = inst->replaceWith(std::move(newBranch));
        inst = newInstructions[inst];
      } else {
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
      } else if (dynamic_cast<const PhiNode *>(inst)) {
        // do nothing for phi functions
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