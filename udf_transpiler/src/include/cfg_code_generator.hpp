/**
 * @file cfg_code_generator.hpp
 * @brief Generate C++ code from CFG of PL/pgSQL
 *
 */

#pragma once
#include "compiler_fmt/core.h"
#include "logical_operator_code_generator.hpp"
#include "utils.hpp"
#include "yaml-cpp/yaml.h"
#include <iostream>
#include <string>
#include <vector>

class CodeContainer {
public:
  // the code generation result of all the basic blocks
  Vec<String> basicBlockCodes;
  // the subfunction used as the loop body of the vectorized function
  String body;
  // the main vectorized function to be registered to DuckDB
  String main;
  // the registration codes called by DuckDB
  String registration;
};

struct CFGCodeGeneratorResult {
  // the main definition of the custom aggregate
  String code;
  // to register the custom aggregate to DuckDB
  String registration;
};

class CFGCodeGenerator {
protected:
  CodeContainer container;
  const YAMLConfig &config;

public:
  CFGCodeGenerator(const YAMLConfig &_config) : config(_config){};
  void basicBlockCodeGenerator(BasicBlock *bb, const Function &func,
                               CodeGenInfo &function_info);
  String extractVarFromChunk(const Function &func);
  CFGCodeGeneratorResult run(const Function &func);

private:
  String createReturnValue(const String &retName, const Type &retType,
                           const String &retValue);
};