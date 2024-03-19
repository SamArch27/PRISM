#pragma once

#include "function_pass.hpp"

class ExpressionPropagationPass : public FunctionPass {
public:
  ExpressionPropagationPass(bool aggressive = false)
      : FunctionPass(), aggressive(aggressive) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "ExpressionPropagation"; };

private:
  bool aggressive;
};

class AggressiveExpressionPropagationPass : public ExpressionPropagationPass {
public:
  AggressiveExpressionPropagationPass() : ExpressionPropagationPass(true) {}
  String getPassName() const override {
    return "AggressiveExpressionPropagation";
  };
};