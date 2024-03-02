#pragma once

#include "function_pass.hpp"

class MergeRegionsPass : public FunctionPass {
public:
  MergeRegionsPass(bool mergePreheaders = false)
      : FunctionPass(), mergePreheaders(mergePreheaders) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "MergeRegions"; }

private:
  bool mergePreheaders;
};

class AggressiveMergeRegionsPass : public MergeRegionsPass {
public:
  AggressiveMergeRegionsPass() : MergeRegionsPass(true) {}
  String getPassName() const override { return "AggressiveMergeRegions"; }
};
