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
  auto dominators = dataflow.computeDominators();

  // print dominator info
  std::cout << "PRINTING DOMINATORS " << std::endl;
  for (auto &block : basicBlocks) {
    for (auto &other : dominators->getDominatingNodes(block.get())) {
      std::cout << other->getLabel() << " dom " << block->getLabel()
                << std::endl;
    }
  }

  auto dominanceFrontier = dataflow.computeDominanceFrontier(dominators);

  // print dominance frontier
  std::cout << "PRINTING DOMINANCE FRONTIER" << std::endl;
  for (auto &block : basicBlocks) {
    std::cout << "DF(" << block->getLabel() << ") = {";

    bool first = true;
    for (auto other : dominanceFrontier->at(block.get())) {
      if (first) {
        first = false;
      } else {
        std::cout << ",";
      }
      std::cout << other->getLabel();
    }
    std::cout << "}" << std::endl;
  }

  auto dominatorTree = dataflow.computeDominatorTree(dominators);

  // print dominator tree
  std::cout << "PRINTING DOMINATOR TREE" << std::endl;
  for (auto &block : basicBlocks) {
    for (const auto &childLabel :
         dominatorTree->getChildren(block->getLabel())) {
      std::cout << block->getLabel() << " -> " << childLabel << std::endl;
    }
  }

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

      for (auto *m : dominanceFrontier->at(block)) {
        if (m == getExitBlock()) {
          continue;
        }
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

void Function::renameVariablesToSSA() {
  /*
  // Collect all variables and arguments into a single set
  Set<Variable *> allVariables;
  for (auto &var : variables) {
    allVariables.insert(var.get());
  }
  for (auto &arg : arguments) {
    allVariables.insert(arg.get());
  }

  Map<const Variable *, Counter> counter;
  Map<const Variable *, Stack<Counter>> stacks;

  // initialize data structures
  for (auto *var : allVariables) {
    counter[var] = 0;
    stacks[var];
  }

  auto predNumber = [&](BasicBlock *child, BasicBlock *parent) {
    const auto &preds = child->getPredecessors();
    auto it = preds.find(parent);
    ASSERT(it != preds.end(),
           "Error! No predecessor found with predNumber function!");
    return std::distance(preds.begin(), it);
  };

  auto topOfStack = [&](const Variable *var) {
    return stacks[var].empty() ? 0 : stacks[var].top();
  };

  auto rename = [&](BasicBlock *block) -> void {
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        // rename RHS
        for (auto *var : assign->getRHS()->getUsedVariables()) {
          auto i = topOfStack(var);
          // Replace x by x_i
        }
        // rename LHS
        const auto *lhs = assign->getLHS();
        auto i = counter[lhs];
        // Replace y by new variable y_i in (lhs = rhs)
        stacks[lhs].push(i);
        counter[lhs] = i + 1;
      }
    }

    // for each successor
    for (auto *succ : block->getSuccessors()) {
      auto j = predNumber(succ, block);
      // for each phi instruction in succ
      for (auto &inst : succ->getInstructions()) {
        if (auto *phi = dynamic_cast<const PhiNode *>(inst.get())) {
          auto i = topOfStack(phi->getRHS()[j]);
          // Replace jth operand x in Phi(...) by x_i
        }
      }
    }

    // for each child IN THE DOMINATOR TREE!!
    // 1. rename(child)

    // for each assignment, pop the stack
    for (auto &inst : block->getInstructions()) {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        // Important: check the name!!
        stacks[assign->getLHS()].pop();
      }
    }
  };
  rename(getEntryBlock());
  */
}

Function newFunction(const String &functionName) {
  return Function(functionName, Make<NonDecimalType>(DuckdbTypeTag::INTEGER));
}