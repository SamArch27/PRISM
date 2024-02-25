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
  computeDominators();
  computeDominanceFrontier();
  computeDominatorTree();
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
