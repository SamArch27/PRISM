#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class FixpointPass : public FunctionPass {
public:
  FixpointPass(Own<FunctionPass> pass)
      : FunctionPass(), pass(std::move(pass)) {}

  bool runOnFunction(Function &f) override {
    bool changed = false;
    do {
      changed = false;
      changed |= pass->runOnFunction(f);
    } while (changed);
    return changed;
  }

  String getPassName() const override { return "Fixpoint"; }

  FunctionPass &getPass() { return *pass; }

private:
  Own<FunctionPass> pass;
};