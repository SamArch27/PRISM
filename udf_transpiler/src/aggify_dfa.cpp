#include "aggify_dfa.hpp"

bool AggifyDFA::analyzeBasicBlock(BasicBlock *bb, const Function &f) {
  for (auto &inst : *bb) {
    if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
      returnVarName = assign->getLHS()->getName();
      return true;
    }
  }
  return false;
}

/**
 * for now, find the first mentioned variable
 * udf_todo: follow the actual aggify paper
 */
void AggifyDFA::analyzeFunction(const Function &f) {
  for (auto &bbUniq : f) {
    // fast exit
    if (analyzeBasicBlock(&bbUniq, f))
      return;
  }
  ERROR("No assignment instruction found in the cursor loop.");
}