#pragma once

#include "function_pass.hpp"

class Compiler;

class OutliningPass : public FunctionPass {
public:
  OutliningPass(Compiler &compiler) : FunctionPass(), compiler(compiler) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "OutliningPass"; }

private:
  void outlineFunction(Function &f);
  bool outlineBasicBlocks(Vec<BasicBlock *> basicBlocks, Function &f);
  bool runOnRegion(const Region *rootRegion, Function &f,
                   Vec<BasicBlock *> &queuedBlocks);

  Compiler &compiler;
  int outlinedCount = 0;
};