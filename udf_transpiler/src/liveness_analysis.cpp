#include "liveness_analysis.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

void LivenessAnalysis::runAnalysis() {
  preprocess();

  for (auto &[block, defs] : defs) {
    std::cout << "Block: " << block->getLabel() << " has defs: " << std::endl;
    for (auto *def : defs) {
      std::cout << *def << " " << std::endl;
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;

  for (auto &[block, defs] : phiDefs) {
    std::cout << "Block: " << block->getLabel()
              << " has phiDefs: " << std::endl;
    for (auto *def : defs) {
      std::cout << *def << " " << std::endl;
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;

  for (auto &[block, uses] : phiUses) {
    std::cout << "Block: " << block->getLabel()
              << " has phiUses: " << std::endl;
    for (auto *use : uses) {
      std::cout << *use << " " << std::endl;
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;

  for (auto &[block, ue] : upwardsExposed) {
    std::cout << "Block: " << block->getLabel()
              << " has upwardsExposed: " << std::endl;
    for (auto *u : ue) {
      std::cout << *u << " " << std::endl;
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;

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

    // insert every non-exit block
    Set<BasicBlock *> worklist;
    for (auto &block : f) {
      if (std::find(exitBlocks.begin(), exitBlocks.end(), &block) ==
          exitBlocks.end()) {
        worklist.insert(&block);
      }
    }

    while (!worklist.empty()) {
      BasicBlock *basicBlock = *worklist.begin();
      worklist.erase(basicBlock);

      // iterate over successors, calling meet over their out sets
      auto newOut = results[basicBlock].out;
      for (auto *succ : basicBlock->getSuccessors()) {
        newOut = meet(newOut, results[succ].in, succ, basicBlock);
      }
      results[basicBlock].out = newOut;

      // apply transfer function to compute in
      auto oldIn = results[basicBlock].in;
      results[basicBlock].in = transfer(results[basicBlock].out, basicBlock);

      if (oldIn != results[basicBlock].in) {
        changed = true;
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

BitVector LivenessAnalysis::transfer(BitVector out, BasicBlock *block) {
  if (block == f.getEntryBlock()) {
    return out;
  }

  // Create bitvector for phiDefs
  BitVector phiDef(instToIndex.size(), false);
  for (auto *definition : phiDefs[block]) {
    phiDef[instToIndex.at(definition)] = true;
  }

  // Create bitvector for defs
  BitVector def(instToIndex.size(), false);
  for (auto *definition : defs[block]) {
    def[instToIndex.at(definition)] = true;
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

BitVector LivenessAnalysis::meet(BitVector out, BitVector in, BasicBlock *succ,
                                 BasicBlock *block) {

  // Create bitvector for phiDefs
  BitVector phiDef(instToIndex.size(), false);
  for (auto *definition : phiDefs[succ]) {
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
  // Union with out and return
  out |= newInfo;
  return out;
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

      // add the def if it's not a phi node
      // if (!dynamic_cast<PhiNode *>(def)) {
      defs[block].insert(inst);
      // }
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
    auto phiOps = phi->getRHS();
    for (auto *pred : block->getPredecessors()) {
      auto predNumber = f.getPredNumber(block, pred);
      auto *phiOp = phi->getRHS()[predNumber];
      for (auto *use : phiOp->getUsedVariables()) {
        auto *definingInst = useDefs->getDef(use);
        phiUses[pred].insert(definingInst);
      }
    }
  }
  // Otherwise update the upwardsExposed
  else {
    // For each operand, check if it is "upwards exposed"
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