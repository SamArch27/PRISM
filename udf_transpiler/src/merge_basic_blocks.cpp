#include "merge_basic_blocks.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool MergeBasicBlocksPass::runOnFunction(Function &f) {
  bool changed = false;

  // visit the CFG in a BFS fashion, merging blocks as we go
  Set<BasicBlock *> worklist;
  for (auto &block : f) {
    worklist.insert(&block);
  }

  while (!worklist.empty()) {
    auto *top = *worklist.begin();
    worklist.erase(top);

    // skip if we are entry
    if (top == f.getEntryBlock()) {
      continue;
    }

    // must be a sequential region without a fallthrough
    auto *sequentialRegion =
        dynamic_cast<SequentialRegion *>(top->getParentRegion());
    if (!sequentialRegion) {
      continue;
    }
    if (sequentialRegion->getFallthroughRegion()) {
      continue;
    }

    // must have an unique successor
    auto successors = top->getSuccessors();
    if (successors.size() != 1) {
      continue;
    }
    auto *bottom = successors.front();

    // that isn't entry
    if (bottom == f.getEntryBlock()) {
      continue;
    }
    // and we are the unique predecessor
    if (bottom->getPredecessors().size() != 1) {
      continue;
    }

    // special: don't merge conditionals
    if (bottom->getSuccessors().size() != 1) {
      continue;
    }

    // special: don't merge blocks containing SELECT
    if (auto *assign = dynamic_cast<const Assignment *>(top->getInitiator())) {
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

    // mark change
    changed = true;
    f.mergeBasicBlocks(top, bottom);
    worklist.insert(bottom);
  }
  return changed;
}
