#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class RemoveUnusedVariablePass : public FunctionPass {
public:
  RemoveUnusedVariablePass() : FunctionPass() {}

  bool runOnFunction(Function &f) {
    // Remove any unused variables
    Set<const Variable *> usedVars;
    for (auto &block : f) {
      for (auto &inst : block) {
        if (inst.getResultOperand()) {
          usedVars.insert(inst.getResultOperand());
        }
        for (auto *var : inst.getOperands()) {
          usedVars.insert(var);
        }
      }
    }

    Set<const Variable *> toRemove;
    for (auto &var : f.getVariables()) {
      if (usedVars.find(var.get()) == usedVars.end()) {
        toRemove.insert(var.get());
      }
    }
    for (auto *var : toRemove) {
      f.removeVariable(var);
    }
    return false;
  }

  String getPassName() const override { return "RemoveUnusedVariable"; }
};