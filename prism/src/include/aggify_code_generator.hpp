/**
 * @file aggify_code_generator.hpp
 * Implements the Aggify algorithm to convert cursor loops to custom aggregates
 */

#pragma once
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
};

/**
 * It should be able to register the custom aggregate to DuckDB
 */
class AggifyCodeGenerator : public CFGCodeGenerator {
private:
  Vec<String> getOrginalCursorLoopCol(const json &ast);

public:
  AggifyCodeGenerator(const YAMLConfig &_config) : CFGCodeGenerator(_config) {}

  AggifyCodeGeneratorResult run(Function &f, const json &ast,
                                Vec<const Variable *> cursorVars,
                                Vec<const Variable *> usedVars,
                                const Variable *retVariable, size_t id);
};