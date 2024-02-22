#pragma once

#include "function_pass.hpp"

class CopyPropagationPass : public FunctionPass {
public:
  CopyPropagationPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "CopyPropagation"; };
};