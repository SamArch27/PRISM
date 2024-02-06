#include "liveness_dataflow.hpp"
#include "utils.hpp"

Own<Liveness> LivenessDataflow::computeLiveness() const {
  Vec<BasicBlock *> blocks;
  for (auto &block : f.getBasicBlocks()) {
    blocks.push_back(block.get());
  }
  auto liveness = Make<Liveness>(blocks);
  for (auto &block : f.getBasicBlocks()) {
    auto liveIn = results.at(block->getInitiator()).in;
    auto liveOut = results.at(block->getInitiator()).out;
    for (std::size_t i = 0; i < varToIndex.size(); ++i) {
      if (liveIn[i]) {
        liveness->addLiveIn(block.get(), variables[i]);
      }
      if (liveOut[i]) {
        liveness->addLiveOut(block.get(), variables[i]);
      }
    }
  }
  return liveness;
}

BitVector LivenessDataflow::transfer(BitVector out, Instruction *inst) {
  BitVector gen(varToIndex.size(), false);
  BitVector kill(varToIndex.size(), false);

  // Compute gen set (RHS)
  for (auto *var : inst->getOperands()) {
    gen[varToIndex[var]] = true;
  }

  // The kill set is just the used variable on the LHS
  auto *resultOperand = inst->getResultOperand();
  if (resultOperand != nullptr) {
    auto it = varToIndex.find(resultOperand);
    if (it != varToIndex.end()) {
      kill[varToIndex.at(resultOperand)] = true;
    }
  }

  BitVector in(varToIndex.size(), false);
  in |= out;
  in &= kill.flip();
  in |= gen;
  return in;
}

BitVector LivenessDataflow::meet(BitVector in1, BitVector in2) {
  BitVector in(varToIndex.size(), false);
  in |= in1;
  in |= in2;
  return in;
}

void LivenessDataflow::preprocessInst(Instruction *inst) {
  for (const auto *var : inst->getOperands()) {
    if (varToIndex.find(var) == varToIndex.end()) {
      std::size_t newSize = varToIndex.size();
      varToIndex[var] = newSize;
      variables.push_back(var);
    }
  }
  const auto *resultOperand = inst->getResultOperand();
  if (resultOperand != nullptr) {
    def.insert({resultOperand, inst});
  }
}

void LivenessDataflow::genBoundaryInner() {
  innerStart = BitVector(varToIndex.size(), false);
  boundaryStart = BitVector(varToIndex.size(), false);
}