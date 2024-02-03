/**
 * @file aggify_code_generator.hpp
 * Implements the Aggify algorithm to convert cursor loops to custom aggregates
*/

#pragma once
#include <iostream>
#include <json.hpp>
#include "utils.hpp"
#include "cfg.hpp"
#include "types.hpp"
#include "cfg_code_generator.hpp"
#include "aggify_dfa.hpp"

using json = nlohmann::json;

struct AggifyCodeGeneratorResult : CFGCodeGeneratorResult{
    // name of the custom aggregate
    String name;
    // the caller to the custom aggregate
    String caller;
};

/**
 * It should be able to register the custom aggregate to DuckDB
*/
class AggifyCodeGenerator : public CFGCodeGenerator{
private:
    Vec<String> getOrginalCursorLoopCol(const json &ast);
public:
    AggifyCodeGenerator(const YAMLConfig &_config)
        : CFGCodeGenerator(_config){
    }

    AggifyCodeGeneratorResult run(const Function &func, const json &ast, const AggifyDFA &dfaResult, size_t id);
};