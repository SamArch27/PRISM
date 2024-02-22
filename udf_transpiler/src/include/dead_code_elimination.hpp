#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class DeadCodeEliminationPass : public FunctionPass {
public:
  DeadCodeEliminationPass() : FunctionPass() {}

  bool runOnFunction(Function &f) override;

  String getPassName() const override { return "DeadCodeElimination"; }
};