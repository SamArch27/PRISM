#include "copy_propagation.hpp"
#include "instructions.hpp"
#include "utils.hpp"
#include <iostream>

String CopyPropagationPass::getPassName() const { return "CopyPropagation"; }

bool CopyPropagationPass::runOnFunction(Function &f) {
  bool changed = false;

  for (auto &basicBlock : f) {
    // ignore the entry block because of how arguments are handled
    if (&basicBlock == f.getEntryBlock()) {
      continue;
    }
    for (auto it = basicBlock.begin(); it != basicBlock.end();) {
      auto &inst = *it;
      // check for x = y assignment
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        auto *lhs = assign->getLHS();
        auto *rhs = assign->getRHS();

        // skip SQL expressions
        if (rhs->isSQLExpression()) {
          ++it;
          continue;
        }

        // Try converting the RHS to a variable
        if (!f.hasBinding(rhs->getRawSQL())) {
          ++it;
          continue;
        }
        auto *var = f.getBinding(rhs->getRawSQL());

        Map<const Variable *, const Variable *> oldToNew{{lhs, var}};
        changed |= f.replaceUsesWith(oldToNew);
        it = basicBlock.removeInst(it);

      } else if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        if (!phi->hasIdenticalArguments()) {
          ++it;
          continue;
        }
        Map<const Variable *, const Variable *> oldToNew{
            {phi->getLHS(), phi->getRHS().front()}};
        changed |= f.replaceUsesWith(oldToNew);
        it = basicBlock.removeInst(it);
      } else {
        ++it;
      }
    }
  }
  return changed;
}
