#pragma once

#include "function_pass.hpp"

class MergeRegionsPass : public FunctionPass {
public:
  MergeRegionsPass(bool aggressive = false)
      : FunctionPass(), aggressive(aggressive) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "MergeRegions"; }

private:
  bool aggressive;
};

class AggressiveMergeRegionsPass : public MergeRegionsPass {
public:
  AggressiveMergeRegionsPass() : MergeRegionsPass(true) {}
  String getPassName() const override { return "AggressiveMergeRegions"; }
};