#include "dominator_dataflow.hpp"
#include "utils.hpp"

BitVector DominatorAnalysis::transfer(BitVector in, Instruction *inst) {

  BitVector gen(blockToIndex.size(), false);
  BasicBlock *block = inst->getParent();
  gen[blockToIndex[block]] = true;

  // OUT = (IN - KILL) U GEN
  BitVector out(blockToIndex.size(), false);
  out |= in;
  out |= gen;
  return out;
}

BitVector DominatorAnalysis::meet(BitVector result, BitVector in,
                                  BasicBlock *) {
  BitVector out(blockToIndex.size(), false);
  out |= result;
  out &= in;
  return out;
}

void DominatorAnalysis::preprocessInst(Instruction *inst) {
  auto *block = inst->getParent();
  if (blockToIndex.find(block) == blockToIndex.end()) {
    std::size_t newSize = blockToIndex.size();
    blockToIndex[block] = newSize;
    basicBlocks.push_back(block);
  }
}

void DominatorAnalysis::genBoundaryInner() {
  // TOP is all 1 because meet is union
  innerStart = BitVector(blockToIndex.size(), true);
  // Boundary condition is nothing available
  boundaryStart = BitVector(blockToIndex.size(), false);
}

void DominatorAnalysis::finalize() {

  Map<BasicBlock *, BasicBlock *> idoms;
  for (auto &block : f) {
    idoms[&block] = nullptr;
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

      // Find the predecessor with the lowest RPO number
      BasicBlock *lowest = curr->getPredecessors().front();
      auto lowestNumber = rpoNumber[lowest];
      for (auto *pred : curr->getPredecessors()) {
        auto num = rpoNumber[pred];
        if (num < lowestNumber) {
          lowestNumber = num;
          lowest = pred;
        }
      }

      // Process the remaining predecessors
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

  Vec<String> labels;
  for (auto *block : basicBlocks) {
    labels.push_back(block->getLabel());
  }
  auto domTree = Make<DominatorTree>(labels);

  for (auto &[b1, b2] : idoms) {
    if (b1 == f.getEntryBlock()) {
      continue;
    }
    domTree->addChild(b2->getLabel(), b1->getLabel());
  }

  computeDominators();
  computeDominanceFrontier();
  computeDominatorTree();

  std::cout << *domTree << std::endl;
  std::cout << *dominatorTree << std::endl;
}

void DominatorAnalysis::computeDominators() {
  dominators = Make<Dominators>(basicBlocks);

  // for each block, add the dominance info
  for (auto *block : basicBlocks) {
    for (auto *other : basicBlocks) {
      auto bitVector = results.at(other->getInitiator()).out;
      if (bitVector[blockToIndex.at(block)]) {
        dominators->addDominanceEdge(block, other);
      }
    }
  }
}

void DominatorAnalysis::computeDominanceFrontier() {
  frontier = Make<DominanceFrontier>(basicBlocks);
  for (auto *n : basicBlocks) {
    for (auto *m : basicBlocks) {
      bool dominatesPredecessor = false;
      for (auto *p : m->getPredecessors()) {
        dominatesPredecessor |= dominators->dominates(n, p);
      }

      if (!dominatesPredecessor) {
        continue;
      }

      if (dominators->strictlyDominates(n, m)) {
        continue;
      }

      frontier->addToFrontier(n, m);
    }
  }
}

void DominatorAnalysis::computeDominatorTree() {
  Vec<String> labels;
  for (auto *block : basicBlocks) {
    labels.push_back(block->getLabel());
  }
  dominatorTree = Make<DominatorTree>(labels);

  for (auto *block : basicBlocks) {
    // for all strict dominators of this block
    auto strictDominators = dominators->getDominatingNodes(block);
    strictDominators.erase(block);

    // for a dominating block
    for (auto *dominatingBlock : strictDominators) {

      // this dominating block does not strictly dominate any other (strictly)
      // dominating block
      bool immediateDominator =
          std::none_of(strictDominators.begin(), strictDominators.end(),
                       [&](BasicBlock *otherDominatingBlock) {
                         return dominators->strictlyDominates(
                             dominatingBlock, otherDominatingBlock);
                       });
      if (immediateDominator) {
        dominatorTree->addChild(dominatingBlock->getLabel(), block->getLabel());
      }
    }
  }
}
