#include "use_def.hpp"

void UseDefAnalysis::runAnalysis() {
  useDefs = Make<UseDefs>();
  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *result = inst.getResultOperand()) {
        useDefs->addDef(result, &inst);
      }
      for (auto *use : inst.getOperands()) {
        useDefs->addUse(use, &inst);
      }
    }
  }
}