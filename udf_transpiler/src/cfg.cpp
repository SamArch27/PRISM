#include "cfg.hpp"
#include "dominator_dataflow.hpp"
#include "utils.hpp"

void Function::convertToSSAForm() { insertPhiFunctions(); }

void Function::insertPhiFunctions() {
  DominatorDataflow dataflow(*this);
  dataflow.runAnalysis();
  auto dominators = dataflow.computeDominators();

  // print dominator info
  std::cout << "\nDominators:" << std::endl;
  std::cout << *dominators << std::endl;

  auto dominanceFrontier = dataflow.computeDominanceFrontier(dominators);

  // print dominance frontier
  std::cout << "\nDominance Frontier:" << std::endl;
  std::cout << *dominanceFrontier << std::endl;

  auto dominatorTree = dataflow.computeDominatorTree(dominators);

  // print dominator tree
  std::cout << *dominatorTree << std::endl;

  // For each variable, when it is assigned in a block, map to the block
  Map<const Variable *, Set<BasicBlock *>> varToBlocksAssigned;

  for (auto &block : basicBlocks) {
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        auto *var = assign->getLHS();
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

      for (auto *m : dominanceFrontier->getFrontier(block)) {
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

  renameVariablesToSSA(dominatorTree);
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

void Function::renameVariablesToSSA(const Own<DominatorTree> &dominatorTree) {

  Map<const Variable *, Counter> counter;
  Map<const Variable *, Stack<Counter>> stacks;

  // initialize data structures
  for (auto *var : getAllVariables()) {
    counter.insert({var, 0});
    stacks.insert({var, Stack<Counter>({0})});
  }

  auto predNumber = [&](BasicBlock *child, BasicBlock *parent) {
    const auto &preds = child->getPredecessors();
    auto it = preds.find(parent);
    ASSERT(it != preds.end(),
           "Error! No predecessor found with predNumber function!");
    return std::distance(preds.begin(), it);
  };

  std::function<void(BasicBlock *)> rename = [&](BasicBlock *block) -> void {
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        // rename RHS
        for (auto *var : assign->getRHS()->getUsedVariables()) {
          auto i = stacks.at(var).top();

          // create x_i as a new variable
          addVariable(var->getName() + "_" + std::to_string(i),
                      var->getType()->clone(), var->isNull());

          // TODO: Replace x by x_i
        }
        // rename LHS
        const auto *lhs = assign->getLHS();
        auto i = counter.at(lhs);
        // Replace y by new variable y_i in (lhs = rhs)
        stacks.at(lhs).push(i);
        counter.at(lhs) = i + 1;
      }
    }

    // for each successor
    for (auto *succ : block->getSuccessors()) {
      auto j = predNumber(succ, block);
      // for each phi instruction in succ
      for (auto &inst : succ->getInstructions()) {
        if (auto *phi = dynamic_cast<const PhiNode *>(inst.get())) {
          auto i = stacks.at(phi->getRHS()[j]).top();
          // TODO: Replace jth operand x in Phi(...) by x_i
        }
      }
    }

    for (const auto &childLabel :
         dominatorTree->getChildren(block->getLabel())) {
      rename(labelToBasicBlock.at(childLabel));
    }

    // for each assignment, pop the stack
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        // Important: TODO: check the name must be the old name!!
        stacks.at(assign->getLHS()).pop();
      }
    }
  };
  rename(getEntryBlock());
}

Function newFunction(const String &functionName) {
  return Function(functionName, Make<NonDecimalType>(DuckdbTypeTag::INTEGER));
}