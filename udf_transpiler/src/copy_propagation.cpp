#include "copy_propagation.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool CopyPropagationPass::runOnFunction(Function &f) {
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
        changed = true;
        inst = inst->replaceWith(Make<Assignment>(
            phi->getLHS(), f.bindExpression(phi->getRHS().front()->getName())));
      }
    }

    // check for x = y assignment
    if (auto *assign = dynamic_cast<const Assignment *>(inst)) {
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

      // replace uses of RHS with LHS and add to the worklist
      for (auto *newInst : f.replaceUsesWithVar(oldToNew, useDefs)) {
        worklist.insert(newInst);
      }
    }
  }
  return changed;
}
