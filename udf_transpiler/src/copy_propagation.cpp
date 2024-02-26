#include "copy_propagation.hpp"
#include "instructions.hpp"
#include "utils.hpp"
#include <iostream>

bool CopyPropagationPass::runOnFunction(Function &f) {
  bool changed = false;

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto &useDefs = useDefAnalysis.getUseDefs();

  for (auto &basicBlock : f) {
    // ignore the entry block because of how arguments are handled
    if (&basicBlock == f.getEntryBlock()) {
      continue;
    }
    for (auto it = basicBlock.begin(); it != basicBlock.end(); ++it) {
      auto &inst = *it;

      // Replace phi functions with identical arguments
      if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        if (phi->hasIdenticalArguments()) {
          changed = true;
          it = basicBlock.replaceInst(
              it, Make<Assignment>(
                      phi->getLHS(),
                      f.bindExpression(phi->getRHS().front()->getName())));
        }
      }

      // check for x = y assignment
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        if (useDefs->getUses(assign->getLHS()).empty()) {
          continue;
        }

        if (!f.hasBinding(assign->getRHS()->getRawSQL())) {
          continue;
        }

        // get RHS as a variable
        changed = true;
        auto *rhsVar = f.getBinding(assign->getRHS()->getRawSQL());
        Map<const Variable *, const Variable *> oldToNew{
            {assign->getLHS(), rhsVar}};

        // replace uses of RHS with LHS
        f.replaceUsesWith(oldToNew, useDefs);
      }
    }
  }
  return changed;
}
