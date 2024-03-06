#pragma once

#include "function_pass.hpp"

using RegionDefs = Map<const Region *, Set<const Variable *>>;

class QueryMotionPass : public FunctionPass {
public:
  QueryMotionPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override { return "QueryMotion"; }

private:
  RegionDefs computeDefs(const Region *root) const;
};