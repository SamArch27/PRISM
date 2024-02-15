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

  auto *block = inst->getParent();

  // Create bitvector for defs
  BitVector def(instToIndex.size(), false);
  for (auto *definition : allDefs[block]) {
    def[instToIndex.at(definition)] = true;
  }

  // Create bitvector for phiDefs
  BitVector phiDef(instToIndex.size(), false);
  for (auto *definition : phiDefs[block]) {
    phiDef[instToIndex.at(definition)] = true;
  }

  // Create bitvector for upwardsExposed
  BitVector ue(instToIndex.size(), false);
  for (auto *use : upwardsExposed[block]) {
    ue[instToIndex.at(use)] = true;
  }

  BitVector result(instToIndex.size(), false);
  // LiveOut(B) \ Def(B)
  result |= out;
  result &= def.flip();
  // Union with PhiDefs(B) and UpwardsExposed(B)
  result |= phiDef;
  result |= ue;
  return result;
}

BitVector LivenessDataflow::meet(BitVector result, BitVector in,
                                 BasicBlock *block) {
  // Create bitvector for phiDefs
  BitVector phiDef(instToIndex.size(), false);
  for (auto *definition : phiDefs[block]) {
    phiDef[instToIndex.at(definition)] = true;
  }

  // Create bitvector for phiUses
  BitVector phiUse(instToIndex.size(), false);
  for (auto *definition : phiUses[block]) {
    phiUse[instToIndex.at(definition)] = true;
  }

  // LiveIn(S) \ PhiDef(S)
  BitVector newInfo(instToIndex.size(), false);
  newInfo |= in;
  newInfo &= phiDef.flip();
  // Union with PhiUses(B)
  newInfo |= phiUse;
  // Union with result and return
  result |= in;
  return result;
}

void LivenessDataflow::preprocessInst(Instruction *inst) {
  // Collect definitions for each block, mapping them to bitvector positions
  const auto *resultOperand = inst->getResultOperand();
  auto *block = inst->getParent();
  if (resultOperand != nullptr) {
    auto *def = f.getDefiningInstruction(resultOperand);
    if (instToIndex.find(def) == instToIndex.end()) {
      std::size_t newSize = instToIndex.size();
      instToIndex[def] = newSize;
      definingInstructions.push_back(inst);
      allDefs[block].insert(inst);
    }
  }

  // If the current instruction is a phi node
  if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
    // Add the def to the phiDefs for the block
    auto *result = phi->getResultOperand();
    phiDefs[block].insert(f.getDefiningInstruction(result));

    // Add the uses to every predecessor block
    for (auto *operand : phi->getOperands()) {
      auto *definingInst = f.getDefiningInstruction(operand);
      phiUses[block].insert(definingInst);
    }
  }
  // Otherwise update the upwardsExposed
  else {
    // For each operatnd, check if it is "upwards exposed"
    for (auto *operand : inst->getOperands()) {
      // Get the block that it was defined in
      auto *definingInst = f.getDefiningInstruction(operand);
      auto *definingBlock = definingInst->getParent();
      // If it was defined outside of this block then it is "upwards exposed"
      if (definingBlock != inst->getParent()) {
        upwardsExposed[block].insert(definingInst);
      }
    }
  }
}

void LivenessDataflow::genBoundaryInner() {
  innerStart = BitVector(instToIndex.size(), false);
  boundaryStart = BitVector(instToIndex.size(), false);
}