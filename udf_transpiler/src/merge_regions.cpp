#include "merge_regions.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool MergeRegionsPass::runOnFunction(Function &f) {

  bool changed = false;
  // visit the CFG in a BFS fashion, merging blocks as we go
  Set<BasicBlock *> worklist;
  for (auto &block : f) {
    // skip entry block
    if (f.getEntryBlock() == &block) {
      continue;
    }
    worklist.insert(&block);
  }

  while (!worklist.empty()) {
    auto *top = *worklist.begin();
    worklist.erase(top);

    if (top == f.getEntryBlock()) {
      continue;
    }

    // don't merge with join points
    if (top->getPredecessors().size() > 1) {
      continue;
    }

    // must be a sequential region with no fallthrough
    if (auto *sequentialRegion =
            dynamic_cast<SequentialRegion *>(top->getRegion())) {
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

      if (!aggressive) {
        // don't merge conditionals
        if (dynamic_cast<ConditionalRegion *>(bottom->getRegion())) {
          continue;
        }

        // don't merge loops
        if (dynamic_cast<LoopRegion *>(bottom->getRegion())) {
          continue;
        }
      }

      // special: don't merge a SELECT region with other regions (unless the
      // other one is empty)
      auto *topRegion = top->getRegion();
      auto *bottomRegion = bottom->getRegion();
      if (topRegion->containsSELECT() && !bottomRegion->containsSELECT()) {
        if (bottom->getInitiator() != bottom->getTerminator()) {
          continue;
        }
      }
      if (!topRegion->containsSELECT() && bottomRegion->containsSELECT()) {
        if (top->getInitiator() != top->getTerminator()) {
          continue;
        }
      }

      // don't merge with phi nodes
      if (dynamic_cast<PhiNode *>(top->getInitiator())) {
        continue;
      }

      // mark change, merge the basic blocks, and update the worklist
      changed = true;
      f.mergeBasicBlocks(top, bottom);
      worklist.insert(bottom);
    }
  }
  return changed;
}
