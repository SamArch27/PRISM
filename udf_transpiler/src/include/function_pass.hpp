#pragma once

#include "function.hpp"
#include "udf_transpiler_extension.hpp"
#include "utils.hpp"

class FunctionPass {
public:
  virtual bool runOnFunction(Function &f) = 0;
  virtual ~FunctionPass() {}
  virtual String getPassName() const = 0;

protected:
  bool passOn(const String &passName) const {
    if (passName == "Pipeline" || passName == "Fixpoint") {
      return true;
    }
    return duckdb::optimizerPassOnMap.count(passName) > 0 &&
           duckdb::optimizerPassOnMap.at(passName);
  }
};