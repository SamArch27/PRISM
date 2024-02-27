#include "merge_basic_blocks.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool MergeBasicBlocksPass::runOnFunction(Function &f) {

  bool changed = false;

  // visit the CFG in a BFS fashion, merging blocks as we go
  Queue<BasicBlock *> q;
  Set<BasicBlock *> visited;
  q.push(f.getEntryBlock());
  while (!q.empty()) {

    auto *block = q.front();
    q.pop();

    if (visited.find(block) != visited.end()) {
      continue;
    }

    visited.insert(block);

    while (true) {

      // skip if we are entry or exit
      if (block == f.getEntryBlock()) {
        break;
      }

      // if we have an unique successor
      auto successors = block->getSuccessors();
      if (successors.size() != 1) {
        break;
      }
      auto *uniqueSucc = *successors.begin();

      // that isn't entry/exit
      if (uniqueSucc == f.getEntryBlock()) {
        break;
      }
      // and we are the unique predecessor
      if (uniqueSucc->getPredecessors().size() != 1) {
        break;
      }

      // special: don't merge conditionals
      if (uniqueSucc->getSuccessors().size() != 1) {
        break;
      }

      // special: don't merge blocks containing SELECT
      if (auto *assign =
              dynamic_cast<const Assignment *>(block->getInitiator())) {
        if (assign->getRHS()->isSQLExpression()) {
          break;
        }
      }
      if (auto *assign =
              dynamic_cast<const Assignment *>(uniqueSucc->getInitiator())) {
        if (assign->getRHS()->isSQLExpression()) {
          break;
        }
      }

      // merge the two blocks
      block->appendBasicBlock(uniqueSucc);

      // finally remove the basic block from the function
      f.removeBasicBlock(uniqueSucc);

      // mark change
      changed = true;
    }

    // try merging successor blocks
    for (auto &succ : block->getSuccessors()) {
      q.push(succ);
    }
  }
  return changed;
}
