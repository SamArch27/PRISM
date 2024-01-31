#include "cfg.hpp"
#include "dominator_dataflow.hpp"
#include "utils.hpp"

void Function::convertToSSAForm() {
  insertPhiFunctions();
  renameVariablesToSSA();
}

void Function::insertPhiFunctions() {
  DominatorDataflow dataflow(*this);
  dataflow.runAnalysis();
  auto dominanceFrontier = dataflow.getDominanceFrontier();

  // For each variable, when it is assigned in a block, map to the block
  Map<const Variable *, Set<BasicBlock *>> varToBlocksAssigned;

  for (auto &block : basicBlocks) {
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        auto *var = assign->getVar();
        varToBlocksAssigned[var].insert(block.get());
      }
    }
  }

  // Initialize worklists
  Set<BasicBlock *> worklist;
  Map<BasicBlock *, const Variable *> inWorklist;
  Map<BasicBlock *, const Variable *> inserted;
  for (auto &block : basicBlocks) {
    inserted[block.get()] = nullptr;
    inWorklist[block.get()] = nullptr;
  }

  // Collect all variables and arguments into a single set
  Set<Variable *> allVariables;
  for (auto &var : variables) {
    allVariables.insert(var.get());
  }
  for (auto &arg : arguments) {
    allVariables.insert(arg.get());
  }

  // for each variable
  for (auto *var : allVariables) {
    // add to the worklist each block where it has been assigned
    for (auto *block : varToBlocksAssigned[var]) {
      inWorklist[block] = var;
      worklist.insert(block);
    }
    while (!worklist.empty()) {
      auto *block = *worklist.begin();
      worklist.erase(block);

      for (auto *m : dominanceFrontier[block]) {
        if (inserted[m] != var) {
          // place a phi instruction for var at m
          auto numPreds = m->getPredecessors().size();
          auto args = Vec<const Variable *>(numPreds, var);
          auto phiInst = Make<PhiNode>(var, args);
          m->insertBefore(m->getInitiator(), std::move(phiInst));
          // update worklists
          inserted[m] = var;
          if (inWorklist[m] != var) {
            inWorklist[m] = var;
            worklist.insert(m);
          }
        }
      }
    }
  }
}

void Function::mergeBasicBlocks() {
  visitBFS([this](BasicBlock *block) {
    while (true) {

      // skip if we are entry or exit
      if (block == getEntryBlock() || block == getExitBlock()) {
        return;
      }
      // if we have an unique successor
      auto successors = block->getSuccessors();
      if (successors.size() != 1) {
        return;
      }
      auto *uniqueSucc = *successors.begin();
      // that isn't entry/exit
      if (uniqueSucc == getEntryBlock() || uniqueSucc == getExitBlock()) {
        return;
      }
      // and we are the unique predecessor
      if (uniqueSucc->getPredecessors().size() != 1) {
        return;
      }

      // merge the two blocks
      block->appendBasicBlock(uniqueSucc);

      // finally remove the basic block from the function
      removeBasicBlock(uniqueSucc);
    }
  });
}

void Function::removeBasicBlock(BasicBlock *toRemove) {
  auto it = basicBlocks.begin();
  while (it != basicBlocks.end()) {
    if (it->get() == toRemove) {
      // make all predecessors not reference this
      for (auto &pred : toRemove->getPredecessors()) {
        pred->removeSuccessor(toRemove);
      }
      // make all successors not reference this
      for (auto &succ : toRemove->getSuccessors()) {
        succ->removePredecessor(toRemove);
      }
      // finally erase the block
      it = basicBlocks.erase(it);
    } else {
      ++it;
    }
  }
}

void Function::renameVariablesToSSA() {}

Function newFunction(const String &functionName) {
  return Function(functionName, Make<NonDecimalType>(DuckdbTypeTag::INTEGER));
}