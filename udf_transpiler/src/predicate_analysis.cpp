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

void PredicateAnalysis::runAnalysis() {

  std::cout << "\nPREDICATE ANALYSIS\n" << std::endl;
  std::cout << f << std::endl;

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
  auto &useDefs = useDefAnalysis.getUseDefs();

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

  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *ret = dynamic_cast<const ReturnInst *>(&inst)) {
        std::cout << *ret << std::endl;

        auto pathsToReturn = getAllPathsToBlock(&block);

        for (auto &path : pathsToReturn) {
          String cond = "";

          std::cout << "Path: ";
          BasicBlock *prevBlock = nullptr;
          for (auto *block : path) {
            std::cout << block->getLabel() << " ";
            if (auto *branch =
                    dynamic_cast<const BranchInst *>(block->getTerminator())) {
              if (branch->isConditional()) {
                if (cond != "") {
                  cond += " AND ";
                }

                cond += "(";

                if (prevBlock != branch->getIfFalse()) {
                  cond += "NOT";
                }
                cond += "(" + branch->getCond()->getRawSQL() +
                        ") IS DISTINCT FROM TRUE";
                cond += ")";
              }
              prevBlock = block;
            }
          }

          auto boundCondition = f.bindExpression(cond, f.getReturnType());
          std::cout << std::endl;
          std::cout << "Predicate: " << *boundCondition << std::endl;
        }

        // Collect all conjunctions along the path
      }
    }
  }
}