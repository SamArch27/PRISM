#include "ssa_destruction.hpp"

bool SSADestructionPass::runOnFunction(Function &f) {
  removePhis(f);
  removeSSANames(f);
  return true;
}

void SSADestructionPass::removePhis(Function &f) {
  // Remove all phis, inserting the appropriate assignments in predecessors
  for (auto &block : f) {
    for (auto it = block.begin(); it != block.end();) {
      auto &inst = *it;
      if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        // Add assignment instructions for each arg to the appropriate block
        for (auto *pred : block.getPredecessors()) {
          auto predNumber = f.getPredNumber(&block, pred);
          auto *arg = phi->getRHS()[predNumber];
          // Elide the assignment if the source and destination are the same
          if (f.getOriginalName(arg->getName()) ==
              f.getOriginalName(phi->getLHS()->getName())) {
            continue;
          }
          auto newAssignment =
              Make<Assignment>(phi->getLHS(), f.bindExpression(arg->getName()));
          if (pred->isConditional()) {
            ASSERT(pred->getPredecessors().size() == 1,
                   "Must have unique predecessor for conditional block!!");
            pred = pred->getPredecessors().front();
          }
          pred->insertBefore(--pred->end(), std::move(newAssignment));
        }
        // Remove the phi
        it = block.removeInst(it);
      } else {
        ++it;
      }
    }
  }
}

void SSADestructionPass::removeSSANames(Function &f) {

  // collect the old variables
  Vec<const Variable *> oldVariables;
  for (auto &var : f.getVariables()) {
    oldVariables.push_back(var.get());
  }

  // map each old (SSA) variable to its new (non-SSA) variable
  Map<const Variable *, const Variable *> oldToNew;
  for (auto *var : oldVariables) {
    auto oldName = f.getOriginalName(var->getName());
    if (!f.hasBinding(oldName)) {
      f.addVariable(oldName, var->getType(), var->isNull());
    }
    auto *originalVar = f.getBinding(oldName);
    if (originalVar != var) {
      oldToNew.insert({var, originalVar});
    }
  }

  for (auto &block : f) {
    for (auto it = block.begin(); it != block.end(); ++it) {
      auto &inst = *it;

      // rewrite all instructions
      if (auto *phi = dynamic_cast<const PhiNode *>(&inst)) {
        Vec<const Variable *> newRHS = phi->getRHS();
        for (auto &var : newRHS) {
          var = oldToNew.at(var);
        }
        it = block.replaceInst(
            it, Make<PhiNode>(oldToNew.at(phi->getLHS()), newRHS));
      } else if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        it = block.replaceInst(
            it, Make<Assignment>(
                    oldToNew.at(assign->getLHS()),
                    f.renameVarInExpression(assign->getRHS(), oldToNew)));
      } else if (auto *returnInst = dynamic_cast<const ReturnInst *>(&inst)) {
        it = block.replaceInst(it, Make<ReturnInst>(f.renameVarInExpression(
                                       returnInst->getExpr(), oldToNew)));
      } else if (auto *branchInst = dynamic_cast<const BranchInst *>(&inst)) {
        if (branchInst->isConditional()) {
          it = block.replaceInst(
              it,
              Make<BranchInst>(
                  branchInst->getIfTrue(), branchInst->getIfFalse(),
                  f.renameVarInExpression(branchInst->getCond(), oldToNew)));
        }
      } else {
        ERROR("Unhandled instruction when renaming variables out of SSA during "
              "SSA destruction!");
      }
    }
  }

  // delete the old variables
  for (auto *oldVar : oldVariables) {
    f.removeVariable(oldVar);
  }
}