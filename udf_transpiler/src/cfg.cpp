#include "cfg.hpp"
#include "dominator_dataflow.hpp"
#include "utils.hpp"

void Compiler::convertToSSAForm(Function &f) { insertPhiFunctions(f); }

void Compiler::insertPhiFunctions(Function &f) {
  DominatorDataflow dataflow(f);
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

  for (auto &block : f.getBasicBlocks()) {
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
  for (auto &block : f.getBasicBlocks()) {
    inserted[block.get()] = nullptr;
    inWorklist[block.get()] = nullptr;
  }

  // for each variable
  for (auto *var : f.getAllVariables()) {
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

  std::cout << "Before renaming!" << std::endl;
  std::cout << f << std::endl;
  renameVariablesToSSA(dominatorTree, f);
  std::cout << "After renaming!" << std::endl;
  std::cout << f << std::endl;
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

void Compiler::renameVariablesToSSA(Function &f,
                                    const Own<DominatorTree> &dominatorTree) {

  Map<const Variable *, Counter> counter;
  Map<const Variable *, Stack<Counter>> stacks;

  // initialize data structures
  for (auto *var : f.getAllVariables()) {
    counter.insert({var, 0});
    stacks.insert({var, Stack<Counter>({0})});
  }

  auto getOriginalName = [](const String &ssaName) {
    return ssaName.substr(0, ssaName.find_first_of("_"));
  };

  auto accessCounter = [&](const Variable *var) -> Counter & {
    return counter.at(f.getBinding(getOriginalName(var->getName())));
  };

  auto accessStack = [&](const Variable *var) -> Stack<Counter> & {
    return stacks.at(f.getBinding(getOriginalName(var->getName())));
  };

  auto predNumber = [&](BasicBlock *child, BasicBlock *parent) {
    const auto &preds = child->getPredecessors();
    auto it = preds.find(parent);
    ASSERT(it != preds.end(),
           "Error! No predecessor found with predNumber function!");
    return std::distance(preds.begin(), it);
  };

  std::function<void(BasicBlock *)> rename = [&](BasicBlock *block) -> void {
    const auto &instructions = block->getInstructions();
    for (auto it = instructions.begin(); it != instructions.end(); ++it) {
      if (auto *phi = dynamic_cast<const PhiNode *>(it->get())) {
        // rename the LHS of the phi node
        auto *var = phi->getLHS();
        auto i = accessStack(var).top();
        auto newName =
            getOriginalName(var->getName()) + "_" + std::to_string(i);
        f.addVariable(newName, var->getType()->clone(), var->isNull());
        auto newPhi = Make<PhiNode>(f.getBinding(newName), phi->getRHS());
        newPhi->setParent(block);
        it = block->replaceInst(it->get(), std::move(newPhi));
      } else if (auto *assign = dynamic_cast<const Assignment *>(it->get())) {
        // rename RHS

        // For each variable, map it to its SSA variable
        Map<String, String> oldToNew;

        // Map RHS variables to new SSA variable names
        for (auto *var : assign->getRHS()->getUsedVariables()) {
          auto i = accessStack(var).top();
          auto newName =
              getOriginalName(var->getName()) + "_" + std::to_string(i);
          oldToNew.insert({var->getName(), newName});
        }

        // Map LHS variable to new SSA name
        const auto *lhs = assign->getLHS();
        auto i = accessCounter(lhs);
        auto newLHSName =
            getOriginalName(lhs->getName()) + "_" + std::to_string(i);
        oldToNew.insert({lhs->getName(), newLHSName});

        // Add the new variables to the bindings map
        Vec<const Variable *> usedVariables;
        for (auto &[oldName, newName] : oldToNew) {
          auto *oldVar = f.getBinding(oldName);
          f.addVariable(newName, oldVar->getType()->clone(), oldVar->isNull());
          usedVariables.push_back(f.getBinding(newName));
        }

        // Find replace the text of the assignment and rebind
        auto replacedText = assign->getRHS()->getRawSQL();
        for (auto &[oldName, newName] : oldToNew) {
          std::regex wordRegex(String("\s+") + oldName);
          replacedText = std::regex_replace(replacedText, wordRegex, newName);
        }

        auto newAssignment = Make<Assignment>(
            f.getBinding(newLHSName), bindExpression(f, replacedText)->clone());
        newAssignment->setParent(block);
        it = block->replaceInst(it->get(), std::move(newAssignment));

        accessStack(lhs).push(i);
        accessCounter(lhs) = i + 1;
      }
    }

    // for each successor
    for (auto *succ : block->getSuccessors()) {
      auto j = predNumber(succ, block);
      // for each phi instruction in succ
      auto &instructions = succ->getInstructions();
      for (auto it = instructions.begin(); it != instructions.end(); ++it) {
        auto &inst = *it;
        if (auto *phi = dynamic_cast<const PhiNode *>(inst.get())) {
          auto *var = phi->getRHS()[j];
          auto i = accessStack(var).top();
          auto newName =
              getOriginalName(var->getName()) + "_" + std::to_string(i);

          auto newArguments = phi->getRHS();
          newArguments[j] = getBinding(newName);

          auto newPhi = Make<PhiNode>(phi->getLHS(), newArguments);
          newPhi->setParent(succ);
          it = succ->replaceInst(inst.get(), std::move(newPhi));
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
        accessStack(assign->getLHS()).pop();
      }
    }
  };
  rename(getEntryBlock());
}

Function newFunction(const String &functionName) {
  return Function(functionName, Make<Type>(false, std::nullopt, std::nullopt,
                                           PostgresTypeTag::INTEGER));
}