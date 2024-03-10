#pragma once

#include "function_pass.hpp"

class MergeRegionsPass : public FunctionPass {
public:
  MergeRegionsPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "MergeRegions"; }
};