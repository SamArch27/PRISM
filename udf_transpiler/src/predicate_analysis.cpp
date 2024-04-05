#include "predicate_analysis.hpp"
#include "function.hpp"
#include "use_def_analysis.hpp"

Vec<Vec<BasicBlock *>>
PredicateAnalysis::getAllPathsToBlock(BasicBlock *startBlock) const {

  // Visit all of the predecessor blocks until reaching the entry block
  Vec<Vec<BasicBlock *>> allPaths;
  allPaths.push_back({startBlock});

  auto *curr = startBlock;
  while (curr != f.getEntryBlock()) {
    auto preds = curr->getPredecessors();

    if (preds.size() == 1) {
      auto *uniquePred = preds.front();
      // extend every path with the new block
      for (auto &path : allPaths) {
        path.push_back(uniquePred);
      }
      curr = uniquePred;
    } else {

      Vec<Vec<BasicBlock *>> updatedPaths;
      // recursively compute all paths
      for (auto *pred : preds) {
        auto predPaths = getAllPathsToBlock(pred);
        // for every recursive path
        for (auto &predPath : predPaths) {
          // for every existing path
          for (auto &path : allPaths) {
            // extend it with the recursive path
            Vec<BasicBlock *> newPath;
            newPath.insert(newPath.end(), path.begin(), path.end());
            newPath.insert(newPath.end(), predPath.begin(), predPath.end());
            // add to the list of updated paths
            updatedPaths.push_back(newPath);
          }
        }
      }
      allPaths = updatedPaths;
      break;
    }
  }

  return allPaths;
}

String PredicateAnalysis::getCondFromPath(const Vec<BasicBlock *> &path) const {
  String cond = "";
  BasicBlock *prevBlock = nullptr;
  for (auto *block : path) {
    if (auto *branch =
            dynamic_cast<const BranchInst *>(block->getTerminator())) {
      if (branch->isConditional()) {
        if (cond != "") {
          cond += " AND ";
        }

        cond += "(";

        if (prevBlock != branch->getIfFalse()) {
          cond += "NOT ";
        }
        cond += "NOT (" + branch->getCond()->getRawSQL() + ")";
        cond += ")";
      }
    }
    prevBlock = block;
  }
  return cond;
}

String
PredicateAnalysis::getExprOnPath(const Vec<BasicBlock *> &path,
                                 const SelectExpression *returnValue) const {

  auto resolvedValue = returnValue->clone();

  while (true) {
    // if the value only depends on the function arguments
    auto usedVariables = resolvedValue->getUsedVariables();
    bool allArguments =
        std::all_of(usedVariables.begin(), usedVariables.end(),
                    [&](const Variable *use) { return f.isArgument(use); });
    if (allArguments) {
      return resolvedValue->getRawSQL();
    }

    // not all arguments so we need to follow use-def chains
    for (auto *use : usedVariables) {
      if (!f.isArgument(use)) {
        auto *def = useDefs->getDef(use);
        if (auto *assign = dynamic_cast<const Assignment *>(def)) {
          Map<const Variable *, const SelectExpression *> oldToNew;
          oldToNew.insert({assign->getLHS(), assign->getRHS()});
          resolvedValue =
              f.replaceVarWithExpression(resolvedValue.get(), oldToNew);
        } else if (auto *phi = dynamic_cast<const PhiNode *>(def)) {
          // find the corresponding predecessor
          auto *block = phi->getParent();
          for (auto *pred : block->getPredecessors()) {
            if (std::find(path.begin(), path.end(), pred) != path.end()) {
              // for the matching predecessor, get the corresponding phi op
              auto phiOp = phi->getRHS()[block->getPredNumber(pred)];
              // then do the replacement
              Map<const Variable *, const SelectExpression *> oldToNew;
              oldToNew.insert({phi->getLHS(), phiOp});
              resolvedValue =
                  f.replaceVarWithExpression(resolvedValue.get(), oldToNew);
            }
          }
        } else {
          ERROR("Can't have definition which isn't a phi or assignment!");
        }
      }
    }
  }
  return resolvedValue->getRawSQL();
}

void PredicateAnalysis::runAnalysis() {
  auto root = f.getRegion();

  Set<const Region *> worklist;
  worklist.insert(root);

  while (!worklist.empty()) {
    auto *curr = *worklist.begin();
    worklist.erase(curr);

    // no predicates if we have a loop region
    if (dynamic_cast<const LoopRegion *>(curr)) {
      return;
    }

    if (auto *rec = dynamic_cast<const RecursiveRegion *>(curr)) {
      for (auto *nested : rec->getNestedRegions()) {
        worklist.insert(nested);
      }
    }
  }

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  useDefs = useDefAnalysis.getUseDefs();

  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *assign = dynamic_cast<Assignment *>(&inst)) {
        auto *lhs = assign->getLHS();
        auto *rhs = assign->getRHS();
        // No predicate if we have multiple uses of a SELECT statement
        if (rhs->isSQLExpression() && useDefs->getUses(lhs).size() > 1) {
          return;
        }
      }
    }
  }

  // =, <=, >=, <, >
  Vec<String> ops = {"=", "<=", ">=", "<", ">"};
  Vec<String> suffix = {"eq", "leq", "geq", "lt", "gt"};
  predicates.resize(5);

  // add a variable t to compare against
  f.addVariable("t", f.getReturnType(), false);

  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *ret = dynamic_cast<const ReturnInst *>(&inst)) {
        auto pathsToReturn = getAllPathsToBlock(&block);

        for (auto &path : pathsToReturn) {
          String condValue = "";
          auto cond = getCondFromPath(path);
          if (cond != "") {
            auto boundCondition = f.bindExpression(cond, Type::BOOLEAN);
            condValue = getExprOnPath(path, boundCondition.get());
          }

          auto returnValue = getExprOnPath(path, ret->getExpr());

          for (std::size_t i = 0; i < predicates.size(); ++i) {
            auto &pred = predicates[i];
            if (pred != "") {
              pred += " OR ";
            }
            pred += "((";
            if (condValue != "") {
              pred += condValue + " AND ";
            }
            pred += ("(" + returnValue + " " + ops[i] + " t)");
            pred += "))";
          }
        }
      }
    }
  }

  for (std::size_t i = 0; i < predicates.size(); ++i) {
    auto &pred = predicates[i];
    String args = "(t";
    for (auto &arg : f.getArguments()) {
      args += (", " + arg->getName());
    }
    args += ")";
    pred = "CREATE MACRO " + f.getFunctionName() + "_" + suffix[i] + args +
           " AS (" + pred + ");";
  }
}