#pragma once

#include "function.hpp"

class Analysis {
public:
  Analysis(Function &f) : f(f) {}

  virtual void runAnalysis() = 0;

protected:
  Function &f;
};