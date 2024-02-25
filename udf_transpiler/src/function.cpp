#include "function.hpp"
#include "dominator_dataflow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
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

Own<SelectExpression> Function::buildReplacedExpression(
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

Own<SelectExpression> Function::bindExpression(const String &expr) {
  destroyDuckDBContext();
  makeDuckDBContext();

  String selectExpressionCommand;
  if (toUpper(expr).find("SELECT") == String::npos)
    selectExpressionCommand = "SELECT " + expr + " FROM tmp;";
  else {
    // insert tmp to the from clause
    auto fromPos = expr.find(" FROM ");
    selectExpressionCommand = expr;
    selectExpressionCommand.insert(fromPos + 6, " tmp, ");
  }
  auto clientContext = conn->context.get();
  clientContext->config.enable_optimizer = true;
  auto &config = duckdb::DBConfig::GetConfig(*clientContext);
  std::set<duckdb::OptimizerType> disable_optimizers_should_delete;

  if (config.options.disabled_optimizers.count(
          duckdb::OptimizerType::STATISTICS_PROPAGATION) == 0)
    disable_optimizers_should_delete.insert(
        duckdb::OptimizerType::STATISTICS_PROPAGATION);
  config.options.disabled_optimizers.insert(
      duckdb::OptimizerType::STATISTICS_PROPAGATION);

  // SELECT <expr> FROM tmp
  Shared<LogicalPlan> boundExpression;
  Shared<duckdb::Binder> plannerBinder;
  try {
    boundExpression = clientContext->ExtractPlan(selectExpressionCommand, true,
                                                 plannerBinder);

    for (auto &type : disable_optimizers_should_delete) {
      config.options.disabled_optimizers.erase(type);
    }
  } catch (const std::exception &e) {
    for (auto &type : disable_optimizers_should_delete) {
      config.options.disabled_optimizers.erase(type);
    }

    destroyDuckDBContext();
    EXCEPTION(e.what());
  }

  duckdb::UsedVariableFinder usedVariableFinder("tmp", plannerBinder);
  usedVariableFinder.VisitOperator(*boundExpression);

  // for each used variable, bind it to a Variable*
  Vec<const Variable *> usedVariables;
  for (const auto &varName : usedVariableFinder.usedVariables) {
    usedVariables.push_back(getBinding(varName));
  }

  return Make<SelectExpression>(expr, std::move(boundExpression),
                                usedVariables);
}

bool Function::replaceDefsWith(
    const Map<const Variable *, const Variable *> &oldToNew) {
  bool changed = false;
  for (auto &block : basicBlocks) {
    for (auto it = block->begin(); it != block->end();) {
      auto &inst = *it;
      auto *var = inst.getResultOperand();
      if (var == nullptr) {
        ++it;
        continue;
      }
      if (oldToNew.find(var) == oldToNew.end()) {
        ++it;
        continue;
      }
      changed = true;
      // replace the defs
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        it = block->replaceInst(
            it, Make<Assignment>(oldToNew.at(var), assign->getRHS()->clone()));
      } else if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        it = block->replaceInst(it,
                                Make<PhiNode>(oldToNew.at(var), phi->getRHS()));
      } else {
        ERROR("Unhandled instruction type during SSA destruction!");
      }
    }
  }
  return changed;
}

bool Function::replaceUsesWith(
    const Map<const Variable *, const Variable *> &oldToNew) {
  bool changed = false;
  for (auto &block : basicBlocks) {
    for (auto it = block->begin(); it != block->end();) {
      auto &inst = *it;

      // skip the instruction if it doesn't use the variable we are
      // replacing
      const auto &ops = inst.getOperands();
      if (std::find_if(ops.begin(), ops.end(), [&](const Variable *op) {
            return oldToNew.find(op) != oldToNew.end();
          }) == ops.end()) {
        ++it;
        continue;
      }

      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        auto *rhs = assign->getRHS();
        // replace RHS with new expression
        auto newAssign = Make<Assignment>(
            assign->getLHS(), buildReplacedExpression(rhs, oldToNew));
        changed = true;
        it = block->replaceInst(it, std::move(newAssign));
      } else if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        auto newArguments = phi->getRHS();
        for (auto &arg : newArguments) {
          if (oldToNew.find(arg) == oldToNew.end()) {
            continue;
          }
          arg = oldToNew.at(arg);
        }

        auto newPhi = Make<PhiNode>(phi->getLHS(), newArguments);
        changed = true;
        it = block->replaceInst(it, std::move(newPhi));

      } else if (auto *returnInst = dynamic_cast<const ReturnInst *>(&inst)) {
        auto newReturn = Make<ReturnInst>(
            buildReplacedExpression(returnInst->getExpr(), oldToNew));
        changed = true;
        it = block->replaceInst(it, std::move(newReturn));
      } else if (auto *branchInst = dynamic_cast<const BranchInst *>(&inst)) {
        if (branchInst->isUnconditional()) {
          ++it;
          continue;
        }
        auto newBranch = Make<BranchInst>(
            branchInst->getIfTrue(), branchInst->getIfFalse(),
            buildReplacedExpression(branchInst->getCond(), oldToNew));
        changed = true;
        it = block->replaceInst(it, std::move(newBranch));
      } else {
        ++it;
      }
    }
  }
  return changed;
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