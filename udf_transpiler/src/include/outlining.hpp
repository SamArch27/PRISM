#pragma once

#include "function_pass.hpp"

class Compiler;

using SelectRegions = Map<const Region *, bool>;

class OutliningPass : public FunctionPass {
public:
  OutliningPass(Compiler &compiler) : FunctionPass(), compiler(compiler) {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "OutliningPass"; }

private:
  SelectRegions computeSelectRegions(const Region *root) const;
  void outlineFunction(Function &f);
  bool outlineBasicBlocks(Vec<BasicBlock *> basicBlocks, Function &f);
  bool runOnRegion(SelectRegions &selectRegions, const Region *rootRegion,
                   Function &f, Vec<BasicBlock *> &queuedBlocks,
                   size_t &fallthoughStart,
                   Set<const BasicBlock *> &blockOutlined);

  Compiler &compiler;
  int outlinedCount = 0;
};