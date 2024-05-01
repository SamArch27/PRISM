#pragma once

#include "function_pass.hpp"

class InstructionEliminationPass : public FunctionPass {
public:
  InstructionEliminationPass(bool aggressive = false)
      : FunctionPass(), aggressive(aggressive) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "InstructionElimination"; };

private:
  bool aggressive;
};

class AggressiveInstructionEliminationPass : public InstructionEliminationPass {
public:
  AggressiveInstructionEliminationPass() : InstructionEliminationPass(true) {}
  String getPassName() const override {
    return "AggressiveInstructionElimination";
  };
};