#include "expression_propagation.hpp"
#include "instructions.hpp"
#include "utils.hpp"
#include <iostream>

bool ExpressionPropagationPass::runOnFunction(Function &f) {
  bool changed = false;

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto &useDefs = useDefAnalysis.getUseDefs();

  auto worklist = useDefs->getAllDefs();
  while (!worklist.empty()) {
    auto *inst = *worklist.begin();
    worklist.erase(inst);

    if (inst->getParent() == f.getEntryBlock()) {
      continue;
    }

    // Replace phi functions with identical arguments
    if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
      if (phi->hasIdenticalArguments()) {
        inst = inst->replaceWith(
            Make<Assignment>(phi->getLHS(), phi->getRHS().front()->clone()));
      }
    }

    // check for x = <expr> assignment
    if (auto *assign = dynamic_cast<const Assignment *>(inst)) {

      // skip if there are no uses
      auto uses = useDefs->getUses(assign->getLHS());
      if (uses.empty()) {
        continue;
      }

      // don't do duplicate SQL statements
      if (assign->getRHS()->isSQLExpression() && uses.size() > 1) {
        continue;
      }

      // replace all occurrences of LHS with RHS
      Map<const Variable *, const SelectExpression *> oldToNew{
          {assign->getLHS(), assign->getRHS()}};

      // replace uses of RHS with LHS and add to the worklist
      for (auto &[oldInst, newInst] :
           f.replaceUsesWithExpr(oldToNew, useDefs)) {
        changed = true;
        worklist.erase(oldInst);
        worklist.insert(newInst);
      }
    }
  }

  return changed;
}
