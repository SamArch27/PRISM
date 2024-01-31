#include "aggify_dfa.hpp"

bool AggifyDFA::analyzeBasicBlock(BasicBlock *bb, const Function &func) {
  for (auto &inst : bb->getInstructions()) {
    if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
      returnVarName = assign->getVar()->getName();
      return true;
    }
  }
  return false;
}

/**
 * for now, find the first mentioned variable
 * udf_todo: follow the actual aggify paper
 */
void AggifyDFA::analyzeFunction(const Function &function) {
  for (auto &bbUniq : function.getBasicBlocks()) {
    // fast exit
    if (analyzeBasicBlock(bbUniq.get(), function))
      return;
  }
  ERROR("No assignment instruction found in the cursor loop.");
}