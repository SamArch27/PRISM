#include "control_dependence_analysis.hpp"
#include "dominator_analysis.hpp"
#include "utils.hpp"

void ControlDependenceAnalysis::runAnalysis() {
  // Copy the function to hold the reverse CFG
  auto reversed =
      Make<Function>(f.getConnection(), f.getFunctionName(), f.getReturnType());

  // Copy the basic blocks
  for (auto &block : f) {
    reversed->makeBasicBlock(block.getLabel());
  }

  // Copy the edges (in reverse)
  for (auto &block : f) {
    auto *reversedBlock = reversed->getBlockFromLabel(block.getLabel());
    for (auto *pred : block.getPredecessors()) {
      auto *reversedPred = reversed->getBlockFromLabel(pred->getLabel());
      reversedBlock->addSuccessor(reversedPred);
    }
    for (auto *succ : block.getSuccessors()) {
      auto *reversedSucc = reversed->getBlockFromLabel(succ->getLabel());
      reversedBlock->addPredecessor(reversedSucc);
    }
  }

  // Create a dummy exit
  auto *entryBlock = reversed->getEntryBlock();
  auto *exitBlock = reversed->makeBasicBlock("exit");

  for (auto &block : f) {
    // If we have a return
    if (block.getSuccessors().empty()) {
      // Add (exit -> block) edges
      auto *reversedBlock = reversed->getBlockFromLabel(block.getLabel());
      exitBlock->addSuccessor(reversedBlock);
      reversedBlock->addPredecessor(exitBlock);
    }
  }

  std::cout << "Original function: " << std::endl;
  std::cout << f << std::endl;

  std::cout << "Reversed function: " << std::endl;
  std::cout << *reversed << std::endl;

  // Change entry and exit block name
  entryBlock->setLabel("exit");
  exitBlock->setLabel("entry");

  DominatorAnalysis dominatorAnalysis(*reversed);
  dominatorAnalysis.runAnalysis();
  auto &postDominanceFrontier = dominatorAnalysis.getDominanceFrontier();

  // Change them back
  entryBlock->setLabel("entry");
  exitBlock->setLabel("exit");

  std::cout << "Post Dominance Frontier: " << std::endl;
  std::cout << *postDominanceFrontier << std::endl;
}