#include "dominator_dataflow.hpp"
#include "utils.hpp"

BitVector DominatorDataflow::transfer(BitVector in, Instruction *inst) {

  BitVector gen(blockToIndex.size(), false);
  BasicBlock *block = inst->getParent();
  gen[blockToIndex[block]] = true;

  // OUT = (IN - KILL) U GEN
  BitVector out(blockToIndex.size(), false);
  out |= in;
  out |= gen;
  return out;
}

BitVector DominatorDataflow::meet(BitVector in1, BitVector in2) {
  BitVector out(blockToIndex.size(), false);
  out |= in1;
  out &= in2;
  return out;
}

void DominatorDataflow::preprocessInst(Instruction *inst) {
  auto *block = inst->getParent();
  if (blockToIndex.find(block) == blockToIndex.end()) {
    std::size_t newSize = blockToIndex.size();
    blockToIndex[block] = newSize;
    basicBlocks.push_back(block);
  }
}

void DominatorDataflow::genBoundaryInner() {
  // TOP is all 1 because meet is union
  innerStart = BitVector(blockToIndex.size(), true);
  // Boundary condition is nothing available
  boundaryStart = BitVector(blockToIndex.size(), false);
}

Own<Dominators> DominatorDataflow::computeDominators() const {
  auto dominators = Make<Dominators>(basicBlocks);

  // for each block, add the dominance info
  for (auto *block : basicBlocks) {
    for (auto *other : basicBlocks) {
      auto bitVector = results.at(other->getInitiator()).out;
      if (bitVector[blockToIndex.at(block)]) {
        dominators->addDominanceEdge(block, other);
      }
    }
  }
  return dominators;
}

Own<DominanceFrontier> DominatorDataflow::computeDominanceFrontier(
    const Own<Dominators> &dominators) const {
  auto frontier = Make<DominanceFrontier>(basicBlocks);
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

  return frontier;
}

Own<DominatorTree> DominatorDataflow::computeDominatorTree(
    const Own<Dominators> &dominators) const {
  Vec<String> labels;
  for (auto *block : basicBlocks) {
    labels.push_back(block->getLabel());
  }
  auto dominatorTree = Make<DominatorTree>(labels);

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
  return dominatorTree;
}