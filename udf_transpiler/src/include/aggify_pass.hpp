#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class AggifyPass : public FunctionPass {
public:
  AggifyPass() : FunctionPass() {}

  bool runOnFunction(Function &f) override;

  String getPassName() const override { return "Aggify"; }
};