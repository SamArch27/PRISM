#include "dominator_dataflow.hpp"

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
  // map each basic block to an index in the BitVector
  BasicBlock *block = inst->getParent();
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

void DominatorDataflow::printDominators() {
  for (auto *block : basicBlocks) {
    Instruction *firstInst = block->getInitiator();
    BitVector bitVector = results[firstInst].out;
    for (std::size_t i = 0; i < bitVector.size(); i++) {
      if (bitVector[i]) {
        std::cout << basicBlocks[i]->getLabel();
        std::cout << " dom ";
        std::cout << block->getLabel();
        std::cout << std::endl;
      }
    }
  }
}

void DominatorDataflow::printDominanceFrontiers() {

  Map<BasicBlock *, Set<BasicBlock *>> frontiers;

  for (auto *n : basicBlocks) {
    for (auto *m : basicBlocks) {
      bool dominatesPredecessor = false;
      for (auto *p : m->getPredecessors()) {
        dominatesPredecessor |= dominates(n, p);
      }

      if (!dominatesPredecessor) {
        continue;
      }

      bool strictlyDominates = (m != n && dominates(n, m));
      if (strictlyDominates) {
        continue;
      }

      frontiers[n].insert(m);
    }
  }

  for (auto &[block, frontier] : frontiers) {
    std::cout << "DF(" << block->getLabel() << ") = {";
    bool first = true;
    for (auto *other : frontier) {
      if (first) {
        first = false;
      } else {
        std::cout << ",";
      }
      std::cout << other->getLabel();
    }
    std::cout << "}" << std::endl;
  }
}

bool DominatorDataflow::dominates(BasicBlock *b1, BasicBlock *b2) {
  BitVector bitVector = results[b2->getInitiator()].out;
  return bitVector[blockToIndex[b1]];
}