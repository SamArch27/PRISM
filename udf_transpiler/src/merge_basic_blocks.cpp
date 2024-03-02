#include "merge_basic_blocks.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool MergeBasicBlocksPass::runOnFunction(Function &f) {
  bool changed = false;

  // visit the CFG in a BFS fashion, merging blocks as we go
  Set<BasicBlock *> worklist;
  for (auto &block : f) {
    if (f.getEntryBlock() != &block) {
      worklist.insert(&block);
    }
  }

  while (!worklist.empty()) {
    auto *top = *worklist.begin();
    worklist.erase(top);

    // must be a sequential region with no fallthrough
    if (auto *sequentialRegion =
            dynamic_cast<SequentialRegion *>(top->getParentRegion())) {
      if (sequentialRegion->getFallthroughRegion()) {
        continue;
      }

      // with a unique successor
      auto successors = top->getSuccessors();
      if (successors.size() != 1) {
        continue;
      }

      // and top must be a unique predecessor
      auto *bottom = successors.front();
      if (bottom->getPredecessors().size() != 1) {
        continue;
      }

      // special: don't merge conditionals
      if (bottom->getSuccessors().size() != 1) {
        continue;
      }

      // special: don't merge blocks containing SELECT
      if (auto *assign =
              dynamic_cast<const Assignment *>(top->getInitiator())) {
        if (assign->getRHS()->isSQLExpression()) {
          continue;
        }
      }
      if (auto *assign =
              dynamic_cast<const Assignment *>(bottom->getInitiator())) {
        if (assign->getRHS()->isSQLExpression()) {
          continue;
        }
      }

      // mark change, merge the basic blocks, and update the worklist
      changed = true;
      f.mergeBasicBlocks(top, bottom);
      worklist.insert(bottom);
    }
  }
  return changed;
}
