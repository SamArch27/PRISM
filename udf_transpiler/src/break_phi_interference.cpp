#include "break_phi_interference.hpp"
#include "instructions.hpp"
#include "liveness_dataflow.hpp"
#include "utils.hpp"

bool BreakPhiInterferencePass::runOnFunction(Function &f) {
  auto livenessAnalysis = Make<LivenessAnalysis>(f);
  livenessAnalysis->runAnalysis();
  auto &liveness = livenessAnalysis->getLiveness();
  auto &interferenceGraph = livenessAnalysis->getInterferenceGraph();
  auto phiCongruent = createPhiCongruenceClasses(f);

  // for every phi, resolve the phi interference
  for (auto &block : f) {
    for (auto it = block.begin(); it != block.end(); ++it) {
      it = resolvePhiInterference(f, it, phiCongruent, *interferenceGraph,
                                  *liveness);
    }
  }

  invalidateSingletons(phiCongruent);
  removeCopies(f, phiCongruent, *interferenceGraph);
  return true;
}

InstIterator BreakPhiInterferencePass::resolvePhiInterference(
    Function &f, InstIterator it, CongruenceClasses &phiCongruent,
    InterferenceGraph &interferenceGraph, Liveness &liveness) {

  MarkedSet marked;
  DeferredSet deferred;

  auto &inst = *it;
  if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
    computeSourceConflicts(phi, phiCongruent, interferenceGraph, liveness,
                           marked, deferred);
    computeResultConflicts(phi, phiCongruent, interferenceGraph, liveness,
                           marked, deferred);
    removeMarkedFromDeferred(marked, deferred);
    moveDeferredToMarked(marked, deferred);
    it = processMarkedSet(f, it, phiCongruent, interferenceGraph, liveness,
                          marked);
    mergePhiCongruenceClasses(it, phiCongruent);
  }
  return it;
}

void BreakPhiInterferencePass::removeCopies(
    Function &f, const CongruenceClasses &phiCongruent,
    const InterferenceGraph &interferenceGraph) {

  // Remove superfluous copies using phiCongruence
  for (auto &block : f) {
    for (auto it = block.begin(); it != block.end();) {
      auto &inst = *it;
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        // Try converting the RHS to a variable
        auto rhsText = assign->getRHS()->getRawSQL();
        if (!f.hasBinding(rhsText)) {
          ++it;
          continue;
        }
        auto *lhs = assign->getLHS();
        auto *rhs = f.getBinding(rhsText);

        bool lhsEmpty = phiCongruent.at(lhs).empty();
        bool rhsEmpty = phiCongruent.at(rhs).empty();

        bool lhsConflict = false;
        bool rhsConflict = false;

        auto rhsConflicts = phiCongruent.at(rhs);
        rhsConflicts.erase(rhs);
        for (auto *x : rhsConflicts) {
          rhsConflict |= interferenceGraph.interferes(lhs, x);
        }

        auto lhsConflicts = phiCongruent.at(lhs);
        lhsConflicts.erase(lhs);
        for (auto *x : lhsConflicts) {
          lhsConflict |= interferenceGraph.interferes(rhs, x);
        }

        if (lhsEmpty && rhsEmpty) {
          it = block.removeInst(it);
        } else if (lhsEmpty && !rhsEmpty) {
          if (rhsConflict) {
            ++it;
            continue;
          }
          it = block.removeInst(it);
        } else if (!lhsEmpty && rhsEmpty) {
          if (lhsConflict) {
            ++it;
            continue;
          }
          it = block.removeInst(it);
        } else {
          if (lhsConflict || rhsConflict) {
            ++it;
            continue;
          }
          it = block.removeInst(it);
        }
      } else {
        ++it;
      }
    }
  }
}

Set<const Variable *>
BreakPhiInterferencePass::intersect(const Set<const Variable *> &lhs,
                                    const Set<const Variable *> &rhs) {
  Set<const Variable *> result;
  for (auto *var : lhs) {
    if (rhs.find(var) != rhs.end()) {
      result.insert(var);
    }
  }
  return result;
}

CongruenceClasses
BreakPhiInterferencePass::createPhiCongruenceClasses(Function &f) {
  CongruenceClasses phiCongruent;
  for (auto *var : f.getAllVariables()) {
    phiCongruent[var].insert(var);
  }
  return phiCongruent;
}

void BreakPhiInterferencePass::computeSourceConflicts(
    const PhiNode *phi, const CongruenceClasses &phiCongruent,
    InterferenceGraph &interferenceGraph, const Liveness &liveness,
    MarkedSet &marked, DeferredSet &deferred) {
  // for each pair of arguments
  auto *block = phi->getParent();
  auto &args = phi->getRHS();
  for (std::size_t i = 0; i < args.size(); ++i) {
    auto *x_i = args[i];
    for (std::size_t j = i + 1; j < args.size(); ++j) {
      auto *x_j = args[j];
      if (interferenceGraph.interferes(x_i, x_j)) {

        // Resolve according to the four cases
        auto *n_i = block->getPredecessors()[i];
        auto *n_j = block->getPredecessors()[j];

        bool lhsConflict =
            !intersect(phiCongruent.at(x_i), liveness.getBlockLiveOut(n_j))
                 .empty();
        bool rhsConflict =
            !intersect(phiCongruent.at(x_j), liveness.getBlockLiveOut(n_i))
                 .empty();
        if (lhsConflict && !rhsConflict) {
          // mark x_i
          marked.insert(x_i);
        } else if (!lhsConflict && rhsConflict) {
          // mark x_j
          marked.insert(x_j);
        } else if (!lhsConflict && !rhsConflict) {
          // unsure so we defer (x_i,x_j)
          deferred.insert({x_i, x_j});
        } else {
          // mark both
          marked.insert(x_i);
          marked.insert(x_j);
        }
      }
    }
  }
}

void BreakPhiInterferencePass::computeResultConflicts(
    const PhiNode *phi, const CongruenceClasses &phiCongruent,
    InterferenceGraph &interferenceGraph, const Liveness &liveness,
    MarkedSet &marked, DeferredSet &deferred) {
  auto *block = phi->getParent();

  // for the result variable and each argument variable
  auto *x_i = phi->getLHS();
  auto &rhs = phi->getRHS();
  for (std::size_t j = 0; j < rhs.size(); ++j) {
    auto *x_j = rhs[j];
    if (interferenceGraph.interferes(x_i, x_j)) {
      // Resolve according to the four cases
      auto *n = block; // defining block for x_i is trivially n
      auto *n_j = block->getPredecessors()[j];

      bool selfConflict =
          !intersect(phiCongruent.at(x_i), liveness.getBlockLiveOut(n)).empty();
      bool lhsConflict =
          !intersect(phiCongruent.at(x_i), liveness.getBlockLiveOut(n_j))
               .empty();
      bool rhsInConflict =
          !intersect(phiCongruent.at(x_j), liveness.getBlockLiveIn(n)).empty();
      bool rhsOutConflict =
          !intersect(phiCongruent.at(x_j), liveness.getBlockLiveOut(n)).empty();

      if (selfConflict && !rhsInConflict) {
        // mark x_i
        marked.insert(x_i);
      } else if (!selfConflict && rhsInConflict) {
        // mark x_j
        marked.insert(x_j);
      } else if (!lhsConflict && !rhsOutConflict) {
        // unsure so we defer (x_i, x_j)
        deferred.insert({x_i, x_j});
      } else if (lhsConflict && rhsOutConflict) {
        // mark both
        marked.insert(x_i);
        marked.insert(x_j);
      }
    }
  }
}

void BreakPhiInterferencePass::moveDeferredToMarked(MarkedSet &marked,
                                                    DeferredSet &deferred) {
  while (!deferred.empty()) {
    // find the most frequently occurring variable
    Counter maxCount = 0;
    Map<const Variable *, Counter> freqCount;
    for (auto &[first, second] : deferred) {
      if (freqCount.find(first) == freqCount.end()) {
        freqCount[first] = 0;
      }
      if (freqCount.find(second) == freqCount.end()) {
        freqCount[second] = 0;
      }
      freqCount[first]++;
      freqCount[second]++;
      maxCount = std::max(maxCount, freqCount[first]);
      maxCount = std::max(maxCount, freqCount[second]);
    }

    const Variable *mostFreqVar = nullptr;
    for (auto &[var, count] : freqCount) {
      if (count == maxCount) {
        mostFreqVar = var;
        break;
      }
    }
    ASSERT(mostFreqVar != nullptr,
           "Error, incorrectly retrieved most frequent element!");

    // add it to the marked set
    marked.insert(mostFreqVar);

    // remove all pairs containing it from the deferred set
    for (auto it = deferred.begin(); it != deferred.end();) {
      if (it->first == mostFreqVar || it->second == mostFreqVar) {
        it = deferred.erase(it);
      } else {
        ++it;
      }
    }
  }
}

void BreakPhiInterferencePass::removeMarkedFromDeferred(MarkedSet &marked,
                                                        DeferredSet &deferred) {
  for (auto *var : marked) {
    for (auto it = deferred.begin(); it != deferred.end();) {
      if (it->first == var || it->second == var) {
        it = deferred.erase(it);
      } else {
        ++it;
      }
    }
  }
}

InstIterator BreakPhiInterferencePass::processMarkedSet(
    Function &f, InstIterator it, CongruenceClasses &phiCongruent,
    InterferenceGraph &interferenceGraph, Liveness &liveness,
    const MarkedSet &marked) {
  for (auto *x : marked) {
    auto &inst = *it;
    auto *oldPhi = dynamic_cast<const PhiNode *>(&inst);
    auto *block = oldPhi->getParent();
    auto *xPrime = createUpdatedVariable(f, x);
    auto newPhi = createUpdatedPhi(x, xPrime, oldPhi, phiCongruent);
    auto newName = xPrime->getName();
    bool resultConflict = (newPhi->getLHS() != oldPhi->getLHS());

    if (resultConflict) {
      processResultConflict(f, x, xPrime, it, interferenceGraph, liveness);
    } else {
      processSourceConflict(f, x, xPrime, it, interferenceGraph, liveness);
    }

    it = block->replaceInst(it, std::move(newPhi));
  }
  return it;
}

void BreakPhiInterferencePass::processResultConflict(
    Function &f, const Variable *x, const Variable *xPrime, InstIterator it,
    InterferenceGraph &interferenceGraph, Liveness &liveness) {
  // Insert x = x' after the phi instruction
  auto newAssignment = Make<Assignment>(x, f.bindExpression(xPrime->getName()));
  auto *block = it->getParent();
  block->insertAfter(it, std::move(newAssignment));

  // Update the live in/out and interference graph
  liveness.removeBlockLiveIn(block, x);
  for (auto *var : liveness.getBlockLiveIn(block)) {
    interferenceGraph.addInterferenceEdge(xPrime, var);
  }
  liveness.addBlockLiveIn(block, xPrime);
}

void BreakPhiInterferencePass::processSourceConflict(
    Function &f, const Variable *x, const Variable *xPrime, InstIterator it,
    InterferenceGraph &interferenceGraph, Liveness &liveness) {

  // For each argument, insert x' = x at the end of the corresponding pred block
  auto *block = it->getParent();
  auto oldArgs = it->getOperands();
  auto argIt = oldArgs.begin();
  while ((argIt = std::find(argIt, oldArgs.end(), x)) != oldArgs.end()) {
    auto newAssignment =
        Make<Assignment>(xPrime, f.bindExpression(x->getName()));

    auto predIndex = std::distance(oldArgs.begin(), argIt);
    auto *toModify = block->getPredecessors()[predIndex];
    if (toModify->isConditional()) {
      ASSERT(toModify->getPredecessors().size() == 1,
             "Must have unique predecessor for conditional block!!");
      toModify = toModify->getPredecessors().front();
    }
    toModify->insertBefore(--toModify->end(), std::move(newAssignment));
    // update the live in/out and interference graph
    auto &successors = block->getSuccessors();
    if (std::all_of(
            successors.begin(), successors.end(), [&](BasicBlock *succ) {
              // not in livein of succ
              auto &succLiveIn = liveness.getBlockLiveIn(succ);
              if (succLiveIn.find(x) != succLiveIn.end()) {
                return false;
              }
              // not referenced in any phi
              for (auto *phi : f.getPhisFromBlock(succ)) {
                auto &uses = phi->getRHS();
                if (std::find(uses.begin(), uses.end(), x) != uses.end()) {
                  return false;
                }
              }
              return true;
            })) {
      liveness.removeBlockLiveOut(toModify, x);
    }
    for (auto *var : liveness.getBlockLiveOut(toModify)) {
      interferenceGraph.addInterferenceEdge(xPrime, var);
    }
    liveness.addBlockLiveOut(toModify, xPrime);
    ++argIt;
  }
}

const Variable *
BreakPhiInterferencePass::createUpdatedVariable(Function &f,
                                                const Variable *x) {
  // Make x' a new variable
  auto newName = String("p") + x->getName();
  if (!f.hasBinding(newName)) {
    f.addVariable(newName, x->getType(), x->isNull());
  }
  return f.getBinding(newName);
}

Own<PhiNode> BreakPhiInterferencePass::createUpdatedPhi(
    const Variable *x, const Variable *xPrime, const PhiNode *oldPhi,
    CongruenceClasses &phiCongruent) {

  phiCongruent[xPrime].insert(xPrime);

  // Replace x with x'
  auto oldResult = oldPhi->getLHS();
  auto oldArgs = oldPhi->getRHS();
  auto newResult = oldResult;
  auto newArgs = oldArgs;
  bool modifyingResult = (newResult == x);

  if (modifyingResult) {
    newResult = xPrime;
  } else {
    for (auto &newArg : newArgs) {
      if (newArg == x) {
        newArg = xPrime;
      }
    }
  }
  return Make<PhiNode>(newResult, newArgs);
}

void BreakPhiInterferencePass::mergePhiCongruenceClasses(
    InstIterator it, CongruenceClasses &phiCongruent) {
  auto &inst = *it;
  Vec<const Variable *> variables = inst.getOperands();
  variables.push_back(inst.getResultOperand());
  Set<const Variable *> currentClass;
  for (auto *x : variables) {
    for (auto *congruent : phiCongruent[x]) {
      currentClass.insert(congruent);
    }
  }
  for (auto *x : currentClass) {
    phiCongruent[x] = currentClass;
  }
}

void BreakPhiInterferencePass::invalidateSingletons(
    CongruenceClasses &phiCongruent) {
  for (auto &[var, congruent] : phiCongruent) {
    if (congruent.size() == 1) {
      congruent.clear();
    }
  }
}