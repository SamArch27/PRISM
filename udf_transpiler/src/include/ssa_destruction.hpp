#pragma once

#include "function_pass.hpp"

class SSADestructionPass : public FunctionPass {
public:
  SSADestructionPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "SSADestruction"; }

private:
  void removePhis(Function &f);
  void removeSSANames(Function &f);
};