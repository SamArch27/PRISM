#include "dead_code_elimination.hpp"
#include "liveness_dataflow.hpp"
#include <iostream>

bool DeadCodeEliminationPass::runOnFunction(Function &f) {
  bool changed = false;
  bool removedDeadCode = false;

  do {
    removedDeadCode = false;
    LivenessDataflow dataflow(f);
    dataflow.runAnalysis();
    auto liveness = dataflow.computeLiveness();

    for (auto &block : f) {
      if (&block == f.getEntryBlock()) {
        continue;
      }
      auto &liveOut = liveness->getLiveOut(&block);
      for (auto it = block.begin(); it != block.end(); ++it) {
        auto &inst = *it;

        if (auto *result = inst.getResultOperand()) {
          bool out = liveOut.find(result) != liveOut.end();

          // dead if its defined in this block but not live out
          if (!out) {
            changed = true;
            removedDeadCode = true;
            it = block.removeInst(it);
          }
        }
      }
    }
  } while (removedDeadCode);
  return changed;
}
