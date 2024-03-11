#include "jump_threading.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool JumpThreadingPass::runOnFunction(Function &f) {

  bool changed = false;

  // visit the CFG in a BFS fashion, merging blocks as we go
  Set<BasicBlock *> worklist;
  for (auto &block : f) {
    if (f.getEntryBlock() != &block) {
      worklist.insert(&block);
    }
  }

  while (!worklist.empty()) {
    auto *block = *worklist.begin();
    worklist.erase(block);

    // look for blocks that are a single jump
    if (block->getInitiator() != block->getTerminator()) {
      continue;
    }

    // don't merge if it's a loop region
    if (dynamic_cast<LoopRegion *>(block->getRegion())) {
      continue;
    }

    if (auto *branch =
            dynamic_cast<const BranchInst *>(block->getTerminator())) {

      // ignore conditional branches
      if (branch->isConditional()) {
        continue;
      }

      // update predecessor of target
      auto *newTarget = block->getSuccessors().front();
      newTarget->removePredecessor(block);

      for (auto *pred : block->getPredecessors()) {
        // replace branch target of all predecessors
        if (auto *predBranch =
                dynamic_cast<BranchInst *>(pred->getTerminator())) {
          auto *ifBranch = predBranch->getIfTrue();
          auto *elseBranch = predBranch->getIfFalse();
          auto *cond = predBranch->getCond();

          if (ifBranch == block) {
            ifBranch = newTarget;
          }
          if (elseBranch == block) {
            elseBranch = newTarget;
          }

          auto newBranch =
              predBranch->isConditional()
                  ? Make<BranchInst>(ifBranch, elseBranch, cond->clone())
                  : Make<BranchInst>(ifBranch);
          predBranch->replaceWith(std::move(newBranch));
        }
        // update successor
        pred->removeSuccessor(block);
        pred->addSuccessor(newTarget);

        // update predecessor of target
        newTarget->addPredecessor(pred);

        // replace the block region with the target region
        auto *targetRegion = newTarget->getRegion();
        auto *blockRegion = targetRegion->getParentRegion();
        auto *predRegion = blockRegion->getParentRegion();
        predRegion->replaceNestedRegion(blockRegion, targetRegion);
      }

      // finally remove the block
      f.removeBasicBlock(block);
      changed = true;
    }
  }

  return changed;
}
