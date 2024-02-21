#pragma once

#include "function.hpp"
#include "utils.hpp"

class FunctionPass {
public:
  virtual bool runOnFunction(Function &f) = 0;
  virtual ~FunctionPass() {}
  virtual String getPassName() const = 0;
};