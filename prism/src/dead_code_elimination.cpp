#include "dead_code_elimination.hpp"
#include "use_def_analysis.hpp"

bool DeadCodeEliminationPass::runOnFunction(Function &f) {
  bool changed = false;
  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto useDefs = useDefAnalysis.getUseDefs();

  auto worklist = useDefs->getAllDefs();
  while (!worklist.empty()) {
    auto *inst = *worklist.begin();
    worklist.erase(inst);

    // don't remove any instructions from the entry block
    if (inst->getParent() == f.getEntryBlock()) {
      continue;
    }

    // this def has uses
    if (!useDefs->getUses(inst->getResultOperand()).empty()) {
      continue;
    }

    // we remove this instruction
    changed = true;

    // for each operand on the RHS
    // get its definition and remove this instruction as a "user"
    Set<const Variable *> toRemove;
    for (auto *operand : inst->getOperands()) {
      auto *def = useDefs->getDef(operand);
      toRemove.insert(def->getResultOperand());
    }
    for (auto *def : toRemove) {
      useDefs->removeUse(def, inst);
    }

    // if after removing the uses from this instruction
    // there are no uses, add the inst for the worklist
    for (auto *def : toRemove) {
      if (useDefs->getUses(def).empty()) {
        worklist.insert(useDefs->getDef(def));
      }
    }
    inst->eraseFromParent();
  }

  return changed;
}
