#pragma once

#include "function_pass.hpp"

class QueryMotionPass : public FunctionPass {
public:
  QueryMotionPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "QueryMotion"; }
};