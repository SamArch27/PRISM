#include "merge_regions.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool MergeRegionsPass::runOnFunction(Function &f) {
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

      // don't merge conditionals
      if (dynamic_cast<ConditionalRegion *>(bottom->getRegion())) {
        continue;
      }

      // don't merge loops
      if (dynamic_cast<LoopRegion *>(bottom->getRegion())) {
        continue;
      }

      // special: don't merge a SELECT region with other regions
      auto *topRegion = top->getRegion();
      auto *bottomRegion = bottom->getRegion();
      if (topRegion->containsSELECT() && !bottomRegion->containsSELECT()) {
        continue;
      }
      if (!topRegion->containsSELECT() && bottomRegion->containsSELECT()) {
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