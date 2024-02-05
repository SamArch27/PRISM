#include "cfg.hpp"
#include "dominator_dataflow.hpp"
#include "utils.hpp"

void Function::removeBasicBlock(BasicBlock *toRemove) {
  auto it = basicBlocks.begin();
  while (it != basicBlocks.end()) {
    if (it->get() == toRemove) {
      // make all predecessors not reference this
      for (auto &pred : toRemove->getPredecessors()) {
        pred->removeSuccessor(toRemove);
      }
      // make all successors not reference this
      for (auto &succ : toRemove->getSuccessors()) {
        succ->removePredecessor(toRemove);
      }
      // finally erase the block
      it = basicBlocks.erase(it);
    } else {
      ++it;
    }
  }
}

Function newFunction(const String &functionName) {
  return Function(functionName, Make<Type>(false, std::nullopt, std::nullopt,
                                           PostgresTypeTag::INTEGER));
}