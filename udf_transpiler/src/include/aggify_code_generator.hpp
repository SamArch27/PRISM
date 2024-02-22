/**
 * @file aggify_code_generator.hpp
 * Implements the Aggify algorithm to convert cursor loops to custom aggregates
 */

#pragma once
#include "aggify_dfa.hpp"
#include "cfg_code_generator.hpp"
#include "function.hpp"
#include "instructions.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <iostream>
#include <json.hpp>

using json = nlohmann::json;

struct AggifyCodeGeneratorResult : CFGCodeGeneratorResult {
  // name of the custom aggregate
  String name;
  // the caller to the custom aggregate
  String caller;
};

/**
 * It should be able to register the custom aggregate to DuckDB
 */
class AggifyCodeGenerator : public CFGCodeGenerator {
private:
  Vec<String> getOrginalCursorLoopCol(const json &ast);

public:
  AggifyCodeGenerator(const YAMLConfig &_config) : CFGCodeGenerator(_config) {}

  AggifyCodeGeneratorResult run(Function &func, const json &ast,
                                const AggifyDFA &dfaResult, size_t id);
};