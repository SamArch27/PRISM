#pragma once

#include "function_pass.hpp"

class JumpThreadingPass : public FunctionPass {
public:
  JumpThreadingPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "JumpThreading"; }
};
