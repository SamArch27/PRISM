#include "instruction_elimination.hpp"
#include "instructions.hpp"
#include "udf_transpiler_extension.hpp"
#include "utils.hpp"
#include <iostream>

bool InstructionEliminationPass::runOnFunction(Function &f) {
  bool changed = false;

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto useDefs = useDefAnalysis.getUseDefs();

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

      if (duckdb::dbPlatform == "duckdb" && uses.size() > 1 &&
          assign->getRHS()->isSQLExpression()) {
        continue;
      }

      // don't do propagation of SQL statements
      if (!aggressive && assign->getRHS()->isSQLExpression()) {
        continue;
      }

      // replace all occurrences of LHS with RHS
      auto bracketedRHS =
          f.bindExpression("(" + assign->getRHS()->getRawSQL() + ")",
                           assign->getRHS()->getReturnType());
      Map<const Variable *, const SelectExpression *> oldToNew{
          {assign->getLHS(), bracketedRHS.get()}};

      // replace uses of RHS with LHS and add to the worklist
      for (auto &[oldInst, newInst] :
           f.replaceUsesWithExpr(oldToNew, *useDefs)) {
        changed = true;
        worklist.erase(oldInst);
        worklist.insert(newInst);
      }
    }
  }

  return changed;
}
