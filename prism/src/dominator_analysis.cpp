#include "dominator_analysis.hpp"
#include "utils.hpp"

void DominatorAnalysis::runAnalysis() {
  Vec<BasicBlock *> blocks;
  Map<BasicBlock *, BasicBlock *> idoms;

  for (auto &block : f) {
    idoms[&block] = nullptr;
    blocks.push_back(&block);
  }

  idoms[f.getEntryBlock()] = f.getEntryBlock();

  Map<BasicBlock *, std::size_t> rpoNumber;
  Set<BasicBlock *> visited;
  Vec<BasicBlock *> reversePostOrder;

  // compute post order
  std::function<void(BasicBlock *)> postorder = [&](BasicBlock *root) {
    visited.insert(root);
    for (auto *succ : root->getSuccessors()) {
      if (visited.find(succ) == visited.end()) {
        postorder(succ);
      }
    }
    reversePostOrder.push_back(root);
  };
  postorder(f.getEntryBlock());
  visited.clear();

  // reverse it
  std::reverse(reversePostOrder.begin(), reversePostOrder.end());
  for (std::size_t i = 0; i < reversePostOrder.size(); ++i) {
    rpoNumber[reversePostOrder[i]] = i + 1;
  }

  auto intersect = [&](BasicBlock *b1, BasicBlock *b2) {
    auto *finger1 = b1;
    auto *finger2 = b2;
    while (rpoNumber[finger1] != rpoNumber[finger2]) {
      while (rpoNumber[finger1] > rpoNumber[finger2]) {
        finger1 = idoms[finger1];
      }
      while (rpoNumber[finger2] > rpoNumber[finger1]) {
        finger2 = idoms[finger2];
      }
    }
    return finger1;
  };

  bool changed = true;
  while (changed) {
    changed = false;

    for (auto *curr : reversePostOrder) {
      if (curr == f.getEntryBlock()) {
        continue;
      }

      // find the predecessor with the lowest RPO number
      BasicBlock *lowest = curr->getPredecessors().front();
      auto lowestNumber = rpoNumber[lowest];
      for (auto *pred : curr->getPredecessors()) {
        auto num = rpoNumber[pred];
        if (num < lowestNumber) {
          lowestNumber = num;
          lowest = pred;
        }
      }

      // process the remaining predecessors
      BasicBlock *newIdom = lowest;
      for (auto *pred : curr->getPredecessors()) {
        if (lowest == pred) {
          continue;
        }
        if (idoms[pred] != nullptr) {
          newIdom = intersect(pred, newIdom);
        }
      }
      if (idoms[curr] != newIdom) {
        idoms[curr] = newIdom;
        changed = true;
      }
    }
  }

  // build the dominator tree
  Vec<String> labels;
  for (auto &block : f) {
    labels.push_back(block.getLabel());
  }
  dominatorTree = Make<DominatorTree>(labels);

  for (auto &[b1, b2] : idoms) {
    if (b1 == f.getEntryBlock()) {
      continue;
    }
    dominatorTree->addChild(b2->getLabel(), b1->getLabel());
  }

  // build the dominance frontier
  dominanceFrontier = Make<DominanceFrontier>(blocks);

  for (auto *block : blocks) {
    if (block->getPredecessors().size() >= 2) {
      for (auto *pred : block->getPredecessors()) {
        auto *runner = pred;
        while (runner != idoms[block]) {
          dominanceFrontier->addToFrontier(runner, block);
          runner = idoms[runner];
        }
      }
    }
  }
}