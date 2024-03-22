#include "liveness_analysis.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

void LivenessAnalysis::runAnalysis() {
  std::cout << "Before preprocess" << std::endl;
  preprocess();
  std::cout << "Before runBackwards" << std::endl;
  runBackwards();
  std::cout << "Before finalize" << std::endl;
  finalize();
  std::cout << "After finalize" << std::endl;
}

void LivenessAnalysis::preprocess() {

  std::cout << "A" << std::endl;
  // run useDefAnalysis to do preprocessing
  useDefAnalysis->runAnalysis();
  std::cout << "B" << std::endl;

  // save exit blocks
  for (auto &basicBlock : f) {
    if (basicBlock.getSuccessors().empty()) {
      exitBlocks.push_back(&basicBlock);
    }
  }
  std::cout << "C" << std::endl;

  // call pre-process for each inst
  for (auto &basicBlock : f) {
    for (auto &inst : basicBlock) {
      auto *currentInst = &inst;
      std::cout << "Preprocess called on: " << *currentInst << std::endl;
      preprocessInst(currentInst);
    }
  }
  std::cout << "D" << std::endl;

  genBoundaryInner();
  std::cout << "E" << std::endl;

  // initialize the IN/OUT sets
  for (auto &basicBlock : f) {
    results[&basicBlock].in = innerStart;
    results[&basicBlock].out = innerStart;
  }

  results[f.getEntryBlock()].in = boundaryStart;

  for (auto *exitBlock : exitBlocks) {
    results[exitBlock].out = boundaryStart;
  }
  std::cout << "F" << std::endl;
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
      auto newOut = BitVector(instToIndex.size(), false);
      for (auto *succ : basicBlock->getSuccessors()) {
        newOut = meet(newOut, results[succ].in, succ);
      }
      // Create bitvector for phiUses
      BitVector phiUse(instToIndex.size(), false);
      for (auto *definition : phiUses[basicBlock]) {
        phiUse[instToIndex.at(definition)] = true;
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
  innerStart = BitVector(instToIndex.size(), false);
  boundaryStart = BitVector(instToIndex.size(), false);
}

void LivenessAnalysis::finalize() {
  computeLiveness();
  computeInterferenceGraph();
}

BitVector LivenessAnalysis::transfer(BitVector out, BasicBlock *block) {

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

  // LiveIn(S) \ PhiDef(S)
  BitVector newInfo(instToIndex.size(), false);
  newInfo |= in;
  newInfo &= phiDef.flip();
  newInfo |= result;
  return newInfo;
}

void LivenessAnalysis::preprocessInst(Instruction *inst) {

  auto useDefs = useDefAnalysis->getUseDefs();

  for (auto *def : useDefs->getAllDefs()) {
    std::cout << def->getResultOperand()->getName()
              << " has pointer value: " << def->getResultOperand() << std::endl;
  }

  // Collect definitions for each block, mapping them to bitvector positions
  const auto *resultOperand = inst->getResultOperand();
  auto *block = inst->getParent();

  std::cout << "X1" << std::endl;

  if (resultOperand != nullptr) {
    auto *def = useDefs->getDef(resultOperand);
    if (instToIndex.find(def) == instToIndex.end()) {
      std::size_t newSize = instToIndex.size();
      instToIndex[def] = newSize;
      definingInstructions.push_back(inst);
      allDefs[block].insert(inst);
    }
  }

  std::cout << "X2" << std::endl;

  if (block == f.getEntryBlock()) {
    return;
  }

  // If the current instruction is a phi node
  if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
    std::cout << "X3 start" << std::endl;

    // Add the def to the phiDefs for the block
    auto *result = phi->getResultOperand();
    phiDefs[block].insert(useDefs->getDef(result));

    // For each predecessor, consider a use in the phi
    for (auto *predBlock : block->getPredecessors()) {
      auto index = block->getPredNumber(predBlock);
      auto *phiOp = phi->getRHS()[index];
      for (auto *operand : phiOp->getUsedVariables()) {
        auto *definingInst = useDefs->getDef(operand);
        phiUses[predBlock].insert(definingInst);
        if (definingInst->getParent() != predBlock) {
          upwardsExposed[predBlock].insert(definingInst);
        }
      }
    }
    std::cout << "X3 end" << std::endl;

  }
  // Otherwise update the upwardsExposed
  else {
    std::cout << "X4 start" << std::endl;

    // For each operatnd, check if it is "upwards exposed"
    for (auto *operand : inst->getOperands()) {
      // Get the block that it was defined in
      std::cout << "Before def on x4" << std::endl;
      std::cout << "Pointer value is: " << operand << std::endl;
      auto *definingInst = useDefs->getDef(operand);
      std::cout << "After def on x4" << std::endl;

      auto *definingBlock = definingInst->getParent();
      // If it was defined outside of this block then it is "upwards exposed"
      if (definingBlock != inst->getParent()) {
        upwardsExposed[block].insert(definingInst);
      }
    }
    std::cout << "X4 end" << std::endl;
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
        liveness->addBlockLiveIn(&block,
                                 definingInstructions[i]->getResultOperand());
      }
    }
    auto &out = results.at(&block).out;
    for (std::size_t i = 0; i < out.size(); ++i) {
      if (out[i]) {
        liveness->addBlockLiveOut(&block,
                                  definingInstructions[i]->getResultOperand());
      }
    }
  }
}

void LivenessAnalysis::computeInterferenceGraph() {
  interferenceGraph = Make<InterferenceGraph>();

  for (auto &block : f) {
    auto &out = results.at(&block).out;

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