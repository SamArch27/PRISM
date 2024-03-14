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
  bool outlineRegion(Vec<const Region *> regions, Function &f,
                     bool returnRegion);
  bool runOnRegion(const Region *rootRegion, Function &f);

  Compiler &compiler;
  int outlinedCount = 0;
};