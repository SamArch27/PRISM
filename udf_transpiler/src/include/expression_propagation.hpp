#pragma once

#include "function_pass.hpp"

class ExpressionPropagationPass : public FunctionPass {
public:
  ExpressionPropagationPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "CopyPropagation"; };
};