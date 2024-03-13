#pragma once

#include "function_pass.hpp"
#include "liveness_analysis.hpp"
#include "utils.hpp"

using CongruenceClasses = Map<const Variable *, Set<const Variable *>>;
using MarkedSet = Set<const Variable *>;
using DeferredSet = OrderedSet<Pair<const Variable *, const Variable *>>;

class BreakPhiInterferencePass : public FunctionPass {
public:
  BreakPhiInterferencePass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "BreakPhiInterference"; }

private:
  InstIterator resolvePhiInterference(Function &f, InstIterator it,
                                      CongruenceClasses &phiCongruent,
                                      InterferenceGraph &interferenceGraph,
                                      Liveness &liveness);

  void removeCopies(Function &f, const CongruenceClasses &phiCongruent,
                    const InterferenceGraph &interferenceGraph);
  Set<const Variable *> intersect(const Set<const Variable *> &lhs,
                                  const Set<const Variable *> &rhs);
  CongruenceClasses createPhiCongruenceClasses(Function &f);
  void computeSourceConflicts(const PhiNode *phi,
                              const CongruenceClasses &phiCongruent,
                              InterferenceGraph &interferenceGraph,
                              const Liveness &liveness, MarkedSet &marked,
                              DeferredSet &deferred);
  void computeResultConflicts(const PhiNode *phi,
                              const CongruenceClasses &phiCongruent,
                              InterferenceGraph &interferenceGraph,
                              const Liveness &liveness, MarkedSet &marked,
                              DeferredSet &deferred);
  void moveDeferredToMarked(MarkedSet &marked, DeferredSet &deferred);
  void removeMarkedFromDeferred(MarkedSet &marked, DeferredSet &deferred);
  InstIterator processMarkedSet(Function &f, InstIterator it,
                                CongruenceClasses &phiCongruent,
                                InterferenceGraph &interferenceGraph,
                                Liveness &liveness, const MarkedSet &marked);
  void mergePhiCongruenceClasses(InstIterator inst,
                                 CongruenceClasses &phiCongruent);
  void invalidateSingletons(CongruenceClasses &phiCongruent);

  const Variable *createUpdatedVariable(Function &f, const Variable *x);
  Own<PhiNode> createUpdatedPhi(Function &f, const Variable *x,
                                const Variable *xPrime, const PhiNode *oldPhi,
                                CongruenceClasses &phiCongruent);

  void processResultConflict(Function &f, const Variable *x,
                             const Variable *xPrime, InstIterator it,
                             InterferenceGraph &interferenceGraph,
                             Liveness &liveness);

  void processSourceConflict(Function &f, const Variable *x,
                             const Variable *xPrime, InstIterator it,
                             InterferenceGraph &interferenceGraph,
                             Liveness &liveness);
};