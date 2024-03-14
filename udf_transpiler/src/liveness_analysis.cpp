#include "liveness_analysis.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

void LivenessAnalysis::runAnalysis() {
  preprocess();
  runBackwards();
  finalize();
}

void LivenessAnalysis::preprocess() {

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
    for (auto &inst : basicBlock) {
      auto *currentInst = &inst;
      results[currentInst].in = innerStart;
      results[currentInst].out = innerStart;
    }
  }

  results[f.getEntryBlock()->getInitiator()].in = boundaryStart;

  for (auto *exitBlock : exitBlocks) {
    results[exitBlock->getTerminator()].out = boundaryStart;
  }
}

void LivenessAnalysis::runBackwards() {
  Set<BasicBlock *> worklist;
  for (auto *exitBlock : exitBlocks) {
    worklist.insert(exitBlock);
  }

  while (!worklist.empty()) {
    BasicBlock *basicBlock = *worklist.begin();
    worklist.erase(basicBlock);

    auto *lastInst = basicBlock->getTerminator();

    auto newOut = results[lastInst].out;
    bool changed = false;

    // iterate over successors, calling meet over their out sets
    for (auto *succ : basicBlock->getSuccessors()) {
      auto &succResult = results[succ->getInitiator()];
      if (!changed) {
        newOut = succResult.in;
        changed = true;
      } else {
        newOut = meet(succResult.in, newOut, succ);
      }
    }

    results[lastInst].out = newOut;
    auto oldIn = results[basicBlock->getInitiator()].in;
    results[lastInst].in = transfer(results[lastInst].out, lastInst);
    auto prevInst = lastInst;

    // iterate the remaining instructions
    auto iter = basicBlock->end();

    do {
      --iter;
      auto *currentInst = &*iter;
      if (currentInst == lastInst) {
        break;
      }
      results[currentInst].in = transfer(results[currentInst].out, currentInst);
      results[currentInst].out = results[prevInst].in;

      prevInst = currentInst;
    } while (iter != basicBlock->begin());

    if (results[basicBlock->getInitiator()].in != oldIn) {
      for (auto *pred : basicBlock->getPredecessors()) {
        worklist.insert(pred);
      }
    }
  }
}

void LivenessAnalysis::genBoundaryInner() {
  innerStart = BitVector(instToIndex.size(), false);
  boundaryStart = BitVector(instToIndex.size(), false);
}

void LivenessAnalysis::finalize() {
  computeLiveness();
  computeInterferenceGraph();
}

BitVector LivenessAnalysis::transfer(BitVector out, Instruction *inst) {

  auto *block = inst->getParent();

  if (block == f.getEntryBlock()) {
    return out;
  }

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

BitVector LivenessAnalysis::meet(BitVector result, BitVector in,
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

void LivenessAnalysis::preprocessInst(Instruction *inst) {

  UseDefAnalysis useDefAnalysis(f);
  useDefAnalysis.runAnalysis();
  auto &useDefs = useDefAnalysis.getUseDefs();

  // Collect definitions for each block, mapping them to bitvector positions
  const auto *resultOperand = inst->getResultOperand();
  auto *block = inst->getParent();

  if (resultOperand != nullptr) {
    auto *def = useDefs->getDef(resultOperand);
    if (instToIndex.find(def) == instToIndex.end()) {
      std::size_t newSize = instToIndex.size();
      instToIndex[def] = newSize;
      definingInstructions.push_back(inst);
      allDefs[block].insert(inst);
    }
  }

  if (block == f.getEntryBlock()) {
    return;
  }

  // If the current instruction is a phi node
  if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
    // Add the def to the phiDefs for the block
    auto *result = phi->getResultOperand();
    phiDefs[block].insert(useDefs->getDef(result));

    // Add the uses to every predecessor block
    for (auto *operand : phi->getOperands()) {
      auto *definingInst = useDefs->getDef(operand);
      phiUses[block].insert(definingInst);
    }
  }
  // Otherwise update the upwardsExposed
  else {
    // For each operatnd, check if it is "upwards exposed"
    for (auto *operand : inst->getOperands()) {
      // Get the block that it was defined in
      auto *definingInst = useDefs->getDef(operand);
      auto *definingBlock = definingInst->getParent();
      // If it was defined outside of this block then it is "upwards exposed"
      if (definingBlock != inst->getParent()) {
        upwardsExposed[block].insert(definingInst);
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
    auto *firstInst = block.getInitiator();
    auto &in = results.at(firstInst).in;
    for (std::size_t i = 0; i < in.size(); ++i) {
      if (in[i]) {
        liveness->addBlockLiveIn(&block,
                                 definingInstructions[i]->getResultOperand());
      }
    }
    for (auto &inst : block) {
      auto &out = results.at(&inst).out;
      for (std::size_t i = 0; i < out.size(); ++i) {
        if (out[i]) {
          liveness->addBlockLiveOut(
              &block, definingInstructions[i]->getResultOperand());
        }
      }
    }
  }
}

void LivenessAnalysis::computeInterferenceGraph() {
  interferenceGraph = Make<InterferenceGraph>();

  for (auto &block : f) {
    for (auto &inst : block) {
      auto &out = results.at(&inst).out;

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
}