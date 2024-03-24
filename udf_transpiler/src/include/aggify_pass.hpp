#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class Compiler;

using SelectRegions = Map<const Region *, bool>;

class AggifyPass : public FunctionPass {
public:
  AggifyPass(Compiler &compiler) : FunctionPass(), compiler(compiler) {}

  bool runOnFunction(Function &f) override;

  String getPassName() const override { return "AggifyPass"; }

private:
  // void outlineFunction(Function &f);
  bool outlineRegion(const Region *region, Function &f);
  bool runOnRegion(const Region *rootRegion, Function &f);

  Compiler &compiler;
  int outlinedCount = 0;
};