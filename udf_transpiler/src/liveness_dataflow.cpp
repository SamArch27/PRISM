#include "liveness_dataflow.hpp"
#include "utils.hpp"

Own<Liveness> LivenessDataflow::computeLiveness() const {
  Vec<BasicBlock *> blocks;
  for (auto &block : f.getBasicBlocks()) {
    blocks.push_back(block.get());
  }
  auto liveness = Make<Liveness>(blocks);

  for (auto &block : f.getBasicBlocks()) {
    auto *firstInst = block->getInitiator();
    auto &in = results.at(firstInst).in;
    for (std::size_t i = 0; i < in.size(); ++i) {
      if (in[i]) {
        liveness->addLiveIn(block.get(),
                            definingInstructions[i]->getResultOperand());
      }
    }
    for (auto &inst : block->getInstructions()) {
      auto &out = results.at(inst.get()).out;
      for (std::size_t i = 0; i < out.size(); ++i) {
        if (out[i]) {
          liveness->addLiveOut(block.get(),
                               definingInstructions[i]->getResultOperand());
        }
      }
    }
  }
  return liveness;
}

Own<InterferenceGraph> LivenessDataflow::computeInterfenceGraph() const {
  auto interferenceGraph = Make<InterferenceGraph>();

  for (auto &block : f.getBasicBlocks()) {
    for (auto &inst : block->getInstructions()) {
      auto &out = results.at(inst.get()).out;

      // all pairs
      for (std::size_t i = 0; i < out.size(); ++i) {
        for (std::size_t j = i + 1; j < out.size(); ++j) {
          if (out[i] && out[j]) {
            auto *left = definingInstructions[i]->getResultOperand();
            auto *right = definingInstructions[j]->getResultOperand();
            interferenceGraph->addInterferenceEdge(left, right);
          }
        }
      }
    }
  }

  return interferenceGraph;
}

BitVector LivenessDataflow::transfer(BitVector out, Instruction *inst) {
  BitVector gen(instToIndex.size(), false);
  BitVector kill(instToIndex.size(), false);

  // Compute gen set (RHS)
  for (auto *var : inst->getOperands()) {
    if (var == nullptr) {
      std::cout << "Found null variable in: " << *inst << std::endl;
    }
    gen[instToIndex.at(def.at(var))] = true;
  }

  // The kill set is just the used variable on the LHS
  auto *resultOperand = inst->getResultOperand();
  if (resultOperand != nullptr) {
    auto it = instToIndex.find(def.at(resultOperand));
    if (it != instToIndex.end()) {
      kill[instToIndex.at(def.at(resultOperand))] = true;
    }
  }

  BitVector in(instToIndex.size(), false);
  in |= out;
  in &= kill.flip();
  in |= gen;
  return in;
}

BitVector LivenessDataflow::meet(BitVector in1, BitVector in2) {
  BitVector in(instToIndex.size(), false);
  in |= in1;
  in |= in2;
  return in;
}

void LivenessDataflow::preprocessInst(Instruction *inst) {
  // map each variable to the instruction that defines it
  // similarly map each defining instruction to the position in the vector
  const auto *resultOperand = inst->getResultOperand();
  if (resultOperand != nullptr) {
    def.insert({resultOperand, inst});
    if (instToIndex.find(def.at(resultOperand)) == instToIndex.end()) {
      std::size_t newSize = instToIndex.size();
      instToIndex[def.at(resultOperand)] = newSize;
      definingInstructions.push_back(inst);
    }
  }
}

void LivenessDataflow::genBoundaryInner() {
  innerStart = BitVector(instToIndex.size(), false);
  boundaryStart = BitVector(instToIndex.size(), false);
}