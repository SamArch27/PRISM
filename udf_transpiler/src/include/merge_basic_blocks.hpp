#pragma once

#include "function_pass.hpp"

class MergeBasicBlocksPass : public FunctionPass {
public:
  MergeBasicBlocksPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override;
};