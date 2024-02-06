#include "liveness_dataflow.hpp"
#include "utils.hpp"

Own<Liveness> LivenessDataflow::computeLiveness() const {}

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