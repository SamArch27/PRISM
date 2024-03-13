#pragma once

#include "function_pass.hpp"

class OutliningPass : public FunctionPass {
public:
  OutliningPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  void outlineRegion(Region *region, Function &f);
  String getPassName() const override { return "OutliningPass"; }

private:
  bool outlineRegion(Vec<const Region *> regions, Function &f,
                     bool returnRegion);
  bool runOnRegion(const Region *rootRegion, Function &f);

  int outlinedCount = 0;
};