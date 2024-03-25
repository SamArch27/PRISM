#include "liveness_analysis.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

void LivenessAnalysis::runAnalysis() {
  preprocess();
  runBackwards();
  finalize();
}

void LivenessAnalysis::preprocess() {

  // run useDefAnalysis to do preprocessing
  useDefAnalysis->runAnalysis();

  // save exit blocks
  for (auto &basicBlock : f) {
    if (basicBlock.getSuccessors().empty()) {
      exitBlocks.push_back(&basicBlock);
    }
  }

  // call pre-process for each inst
  for (auto &basicBlock : f) {
    for (auto &inst : basicBlock) {
      auto *currentInst = &inst;
      preprocessInst(currentInst);
    }
  }
  genBoundaryInner();

  // initialize the IN/OUT sets
  for (auto &basicBlock : f) {
    results[&basicBlock].in = innerStart;
    results[&basicBlock].out = innerStart;
  }

  results[f.getEntryBlock()].in = boundaryStart;

  for (auto *exitBlock : exitBlocks) {
    results[exitBlock].out = boundaryStart;
  }
}

void LivenessAnalysis::runBackwards() {

  bool changed = true;
  while (changed) {
    changed = false;

    // insert every block
    Set<BasicBlock *> worklist;
    for (auto &block : f) {
      worklist.insert(&block);
    }

    while (!worklist.empty()) {
      BasicBlock *basicBlock = *worklist.begin();
      worklist.erase(basicBlock);

      // iterate over successors, calling meet over their in sets
      auto newOut = BitVector(varToIndex.size(), false);
      for (auto *succ : basicBlock->getSuccessors()) {
        newOut = meet(newOut, results[succ].in, succ);
      }
      // Create bitvector for phiUses
      BitVector phiUse(varToIndex.size(), false);
      for (auto *definition : phiUses[basicBlock]) {
        phiUse[varToIndex.at(definition)] = true;
      }
      newOut |= phiUse;

      // apply transfer function to compute in
      auto newIn = transfer(newOut, basicBlock);
      auto oldIn = results[basicBlock].in;

      // change with in means we keep iterating
      if (oldIn != newIn) {
        changed = true;
      }

      results[basicBlock].in = newIn;
      results[basicBlock].out = newOut;
    }
  }
}

void LivenessAnalysis::genBoundaryInner() {
  innerStart = BitVector(varToIndex.size(), false);
  boundaryStart = BitVector(varToIndex.size(), false);
}

void LivenessAnalysis::finalize() {
  computeLiveness();
  computeInterferenceGraph();
}

BitVector LivenessAnalysis::transfer(BitVector out, BasicBlock *block) {

  // Create bitvector for defs
  BitVector def(varToIndex.size(), false);
  for (auto *definition : allDefs[block]) {
    def[varToIndex.at(definition)] = true;
  }

  // Create bitvector for phiDefs
  BitVector phiDef(varToIndex.size(), false);
  for (auto *definition : phiDefs[block]) {
    phiDef[varToIndex.at(definition)] = true;
  }

  // Create bitvector for upwardsExposed
  BitVector ue(varToIndex.size(), false);
  for (auto *use : upwardsExposed[block]) {
    ue[varToIndex.at(use)] = true;
  }

  BitVector result(varToIndex.size(), false);
  // LiveOut(B) \ Def(B)
  result |= out;
  result &= def.flip();
  // Union with PhiDefs(B) and UpwardsExposed(B)
  result |= phiDef;
  result |= ue;
  return result;
}

BitVector LivenessAnalysis::meet(BitVector result, BitVector in,
                                 BasicBlock *block) {

  // Create bitvector for phiDefs
  BitVector phiDef(varToIndex.size(), false);
  for (auto *definition : phiDefs[block]) {
    phiDef[varToIndex.at(definition)] = true;
  }

  // LiveIn(S) \ PhiDef(S)
  BitVector newInfo(varToIndex.size(), false);
  newInfo |= in;
  newInfo &= phiDef.flip();
  newInfo |= result;
  return newInfo;
}

void LivenessAnalysis::preprocessInst(Instruction *inst) {

  auto useDefs = useDefAnalysis->getUseDefs();

  // Collect definitions for each block, mapping them to bitvector positions
  const auto *resultOperand = inst->getResultOperand();
  auto *block = inst->getParent();

  if (resultOperand != nullptr) {
    if (varToIndex.find(resultOperand) == varToIndex.end()) {
      std::size_t newSize = varToIndex.size();
      varToIndex[resultOperand] = newSize;
      variables.push_back(resultOperand);
      allDefs[block].insert(resultOperand);
    }
  }

  // If the current instruction is a phi node
  if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {

    // Add the def to the phiDefs for the block
    auto *result = phi->getResultOperand();
    phiDefs[block].insert(result);

    // For each predecessor, consider a use in the phi
    for (auto *predBlock : block->getPredecessors()) {
      auto index = block->getPredNumber(predBlock);
      auto *phiOp = phi->getRHS()[index];
      for (auto *operand : phiOp->getUsedVariables()) {
        auto *definingInst = useDefs->getDef(operand);
        phiUses[predBlock].insert(operand);
        if (definingInst->getParent() != predBlock) {
          upwardsExposed[predBlock].insert(operand);
        }
      }
    }
  }
  // Otherwise update the upwardsExposed
  else {
    // For each operand, check if it is "upwards exposed"
    for (auto *operand : inst->getOperands()) {
      // Everything is trivially upwards exposed in entry
      if (block == f.getEntryBlock()) {
        upwardsExposed[block].insert(operand);
        // Also add it as a def
        if (varToIndex.find(operand) == varToIndex.end()) {
          std::size_t newSize = varToIndex.size();
          varToIndex[operand] = newSize;
          variables.push_back(operand);
          allDefs[block].insert(operand);
        }
      } else {
        // Get the block that it was defined in
        auto *definingInst = useDefs->getDef(operand);
        auto *definingBlock = definingInst->getParent();
        // If it was defined outside of this block then it is "upwards exposed"
        if (definingBlock != inst->getParent()) {
          upwardsExposed[block].insert(operand);
        }
      }
    }
  }
}

void LivenessAnalysis::computeLiveness() {
  Vec<BasicBlock *> blocks;
  for (auto &block : f) {
    blocks.push_back(&block);
  }
  liveness = Make<Liveness>(blocks);

  for (auto &block : f) {
    auto &in = results.at(&block).in;
    for (std::size_t i = 0; i < in.size(); ++i) {
      if (in[i]) {
        liveness->addBlockLiveIn(&block, variables[i]);
      }
    }
    auto &out = results.at(&block).out;
    for (std::size_t i = 0; i < out.size(); ++i) {
      if (out[i]) {
        liveness->addBlockLiveOut(&block, variables[i]);
      }
    }
  }
}

void LivenessAnalysis::computeInterferenceGraph() {
  interferenceGraph = Make<InterferenceGraph>();

  for (auto &block : f) {
    for (std::size_t i = 0; i < variables.size(); ++i) {
      for (std::size_t j = i + 1; j < variables.size(); ++j) {
        auto liveOut = liveness->getBlockLiveOut(&block);
        auto *left = variables[i];
        auto *right = variables[j];
        if (liveOut.find(left) == liveOut.end()) {
          continue;
        }
        if (liveOut.find(right) == liveOut.end()) {
          continue;
        }
        interferenceGraph->addInterferenceEdge(left, right);
      }
    }
  }
}