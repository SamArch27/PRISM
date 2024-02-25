#include "ssa_construction.hpp"
#include "dominator_dataflow.hpp"
#include "instructions.hpp"
#include "utils.hpp"

bool SSAConstructionPass::runOnFunction(Function &f) {
  DominatorAnalysis dominatorAnalysis(f);
  dominatorAnalysis.runAnalysis();
  auto &dominanceFrontier = dominatorAnalysis.getDominanceFrontier();
  auto &dominatorTree = dominatorAnalysis.getDominatorTree();

  insertPhiFunctions(f, dominanceFrontier);
  renameVariablesToSSA(f, dominatorTree);
  return true;
}

void SSAConstructionPass::insertPhiFunctions(
    Function &f, const Own<DominanceFrontier> &dominanceFrontier) {

  // For each variable, when it is assigned in a block, map to the block
  Map<const Variable *, Set<BasicBlock *>> varToBlocksAssigned;

  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        auto *var = assign->getLHS();
        varToBlocksAssigned[var].insert(&block);
      }
    }
  }

  // Initialize worklists
  Set<BasicBlock *> worklist;
  Map<BasicBlock *, const Variable *> inWorklist;
  Map<BasicBlock *, const Variable *> inserted;
  for (auto &block : f) {
    inserted[&block] = nullptr;
    inWorklist[&block] = nullptr;
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
          m->insertBefore(m->begin(), std::move(phiInst));
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

void SSAConstructionPass::renameVariablesToSSA(
    Function &f, const Own<DominatorTree> &dominatorTree) {

  Map<const Variable *, Counter> counter;
  Map<const Variable *, Stack<Counter>> stacks;

  // initialize data structures
  for (auto *var : f.getAllVariables()) {
    counter.insert({var, 0});
    stacks.insert({var, Stack<Counter>({0})});
  }

  auto accessCounter = [&](const Variable *var) -> Counter & {
    return counter.at(f.getBinding(f.getOriginalName(var->getName())));
  };

  auto accessStack = [&](const Variable *var) -> Stack<Counter> & {
    return stacks.at(f.getBinding(f.getOriginalName(var->getName())));
  };

  auto getNewArgumentName = [&](const Variable *var) {
    auto i = accessStack(var).top();
    auto oldName = f.getOriginalName(var->getName());
    auto newName = oldName + "_" + std::to_string(i) + "_";
    return newName;
  };

  auto renameVariable = [&](const Variable *var, bool updateVariable) {
    auto i = updateVariable ? accessCounter(var) : accessStack(var).top();
    auto oldName = f.getOriginalName(var->getName());
    auto newName = oldName + "_" + std::to_string(i) + "_";
    auto *oldVar = f.getBinding(oldName);
    if (!f.hasBinding(newName)) {
      f.addVariable(newName, oldVar->getType(), oldVar->isNull());
    }
    // Update stack and counters
    if (updateVariable) {
      accessStack(var).push(i);
      accessCounter(var) = i + 1;
    }
    return f.getBinding(newName);
  };

  auto renameSelectExpression = [&](const SelectExpression *expr) {
    // For each variable, map it to its SSA variable
    Map<String, String> oldToNew;

    // Map RHS variables to new SSA variable names
    for (auto *var : expr->getUsedVariables()) {
      auto i = accessStack(var).top();
      auto newName =
          f.getOriginalName(var->getName()) + "_" + std::to_string(i) + "_";
      oldToNew.insert({var->getName(), newName});
    }

    // Map old variables to new ones
    Map<const Variable *, const Variable *> varMapping;
    for (auto &[oldName, newName] : oldToNew) {
      auto *oldVar = f.getBinding(oldName);
      if (!f.hasBinding(newName)) {
        f.addVariable(newName, oldVar->getType(), oldVar->isNull());
      }
      varMapping.insert({oldVar, f.getBinding(newName)});
    }

    // For every variable, rebind the select expression with its new equivalent
    Own<SelectExpression> replacedExpression = expr->clone();
    replacedExpression =
        f.buildReplacedExpression(replacedExpression.get(), varMapping);
    return replacedExpression;
  };

  std::function<void(BasicBlock *)> rename = [&](BasicBlock *block) -> void {
    // we don't perform renaming for the entry block (due to arguments)
    if (block != f.getEntryBlock()) {
      for (auto it = block->begin(); it != block->end(); ++it) {
        auto &inst = *it;
        if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
          auto newPhi =
              Make<PhiNode>(renameVariable(phi->getLHS(), true), phi->getRHS());
          it = block->replaceInst(it, std::move(newPhi));
        } else if (auto *returnInst = dynamic_cast<const ReturnInst *>(&inst)) {
          auto newReturn =
              Make<ReturnInst>(renameSelectExpression(returnInst->getExpr()));
          it = block->replaceInst(it, std::move(newReturn));
        } else if (auto *branchInst = dynamic_cast<const BranchInst *>(&inst)) {
          if (branchInst->isUnconditional()) {
            continue;
          }
          auto newBranch = Make<BranchInst>(
              branchInst->getIfTrue(), branchInst->getIfFalse(),
              renameSelectExpression(branchInst->getCond()));
          it = block->replaceInst(it, std::move(newBranch));
        } else if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
          auto newAssignment =
              Make<Assignment>(renameVariable(assign->getLHS(), true),
                               renameSelectExpression(assign->getRHS()));
          it = block->replaceInst(it, std::move(newAssignment));
        } else {
          ERROR("Unhandled instruction type during SSA renaming!");
        }
      }
    }

    // for each successor
    for (auto *succ : block->getSuccessors()) {
      auto j = f.getPredNumber(succ, block);
      for (auto it = succ->begin(); it != succ->end(); ++it) {
        auto &inst = *it;
        if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
          auto newArguments = phi->getRHS();
          newArguments[j] = renameVariable(phi->getRHS()[j], false);
          auto newPhi = Make<PhiNode>(phi->getLHS(), newArguments);
          it = succ->replaceInst(it, std::move(newPhi));
        }
      }
    }

    for (const auto &childLabel :
         dominatorTree->getChildren(block->getLabel())) {
      rename(f.getBlockFromLabel(childLabel));
    }

    // for each assignment, pop the stack
    for (auto &inst : *block) {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        accessStack(assign->getLHS()).pop();
      }
    }
  };

  // For each arg, create x' and assignment x' = x in entry
  for (const auto &arg : f.getArguments()) {
    auto oldName = arg->getName();
    auto newName = getNewArgumentName(arg.get());
    f.addVariable(newName, arg->getType(), false);
    auto assign =
        Make<Assignment>(f.getBinding(newName), f.bindExpression(oldName));
    auto *entryBlock = f.getEntryBlock();
    entryBlock->insertBefore(--entryBlock->end(), std::move(assign));
  }

  // rename all of the variables
  rename(f.getEntryBlock());

  // collect the old variable
  Vec<const Variable *> oldVariables;
  for (auto &var : f.getVariables()) {
    if (var->getName() == f.getOriginalName(var->getName())) {
      oldVariables.push_back(var.get());
    }
  }

  // delete the old variables
  for (auto *oldVar : oldVariables) {
    f.removeVariable(oldVar);
  }
}

String SSAConstructionPass::getPassName() const { return "SSAConstruction"; }