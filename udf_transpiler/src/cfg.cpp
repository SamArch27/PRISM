#include "cfg.hpp"
#include "dominator_dataflow.hpp"
#include "utils.hpp"

void Function::mergeBasicBlocks() {
  visitBFS([this](BasicBlock *block) {
    while (true) {

      // skip if we are entry or exit
      if (block == getEntryBlock() || block == getExitBlock()) {
        return;
      }
      // if we have an unique successor
      auto successors = block->getSuccessors();
      if (successors.size() != 1) {
        return;
      }
      auto *uniqueSucc = *successors.begin();
      // that isn't entry/exit
      if (uniqueSucc == getEntryBlock() || uniqueSucc == getExitBlock()) {
        return;
      }
      // and we are the unique predecessor
      if (uniqueSucc->getPredecessors().size() != 1) {
        return;
      }

      // merge the two blocks
      block->appendBasicBlock(uniqueSucc);

      // finally remove the basic block from the function
      removeBasicBlock(uniqueSucc);
    }
  });
}

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