#pragma once

#include "function_pass.hpp"

class AssignmentEliminationPass : public FunctionPass {
public:
  AssignmentEliminationPass(bool aggressive = false)
      : FunctionPass(), aggressive(aggressive) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "AssignmentElimination"; };

private:
  bool aggressive;
};

class AggressiveAssignmentEliminationPass : public AssignmentEliminationPass {
public:
  AggressiveAssignmentEliminationPass() : AssignmentEliminationPass(true) {}
  String getPassName() const override {
    return "AggressiveAssignmentElimination";
  };
};