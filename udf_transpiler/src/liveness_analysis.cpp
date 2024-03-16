#include "liveness_analysis.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

void LivenessAnalysis::runAnalysis() {
  preprocess();

  for (auto &[block, use] : uses) {
    std::cout << "Block: " << block->getLabel() << " has uses: " << std::endl;
    for (auto *u : use) {
      std::cout << u->getResultOperand()->getName() << std::endl;
    }
    std::cout << std::endl;
  }

  for (auto &[block, def] : defs) {
    std::cout << "Block: " << block->getLabel() << " has defs: " << std::endl;
    for (auto *d : def) {
      std::cout << d->getResultOperand()->getName() << std::endl;
    }
    std::cout << std::endl;
  }

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

    // insert every block
    Set<BasicBlock *> worklist;
    for (auto &block : f) {
      worklist.insert(&block);
    }

    while (!worklist.empty()) {
      BasicBlock *basicBlock = *worklist.begin();
      worklist.erase(basicBlock);

      // iterate over successors, calling meet over their out sets
      auto newOut = BitVector(instToIndex.size(), false);
      for (auto *succ : basicBlock->getSuccessors()) {
        newOut |= results[succ].in;
      }
      // apply transfer function to compute in
      auto newIn = transfer(results[basicBlock].out, basicBlock);
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

  // Create bitvector for uses
  BitVector use(instToIndex.size(), false);
  for (auto *definition : uses[block]) {
    use[instToIndex.at(definition)] = true;
  }

  // Create bitvector for defs
  BitVector def(instToIndex.size(), false);
  for (auto *definition : defs[block]) {
    def[instToIndex.at(definition)] = true;
  }

  BitVector result(instToIndex.size(), false);
  result |= out;
  result &= def.flip();
  result |= use;
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
      defs[block].insert(inst);
    }
  }

  if (block == f.getEntryBlock()) {
    return;
  }

  // If the current instruction is a phi node
  if (auto *phi = dynamic_cast<const PhiNode *>(inst)) {
    // Add the uses to every predecessor block
    auto phiOps = phi->getRHS();
    for (auto *pred : block->getPredecessors()) {
      auto predNumber = f.getPredNumber(block, pred);
      auto *phiOp = phi->getRHS()[predNumber];
      for (auto *use : phiOp->getUsedVariables()) {
        auto *definingInst = useDefs->getDef(use);
        uses[pred].insert(definingInst);
      }
    }
  } else {
    for (auto *use : inst->getOperands()) {
      uses[block].insert(useDefs->getDef(use));
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