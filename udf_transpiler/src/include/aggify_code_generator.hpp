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

/**
 * It should be able to register the custom aggregate to DuckDB
*/
class AggifyCodeGenerator : public CFGCodeGenerator{
private:
    // const YAMLConfig &config;
    // const json &ast;
    // const Function &func;
    Vec<String> getOrginalCursorLoopCol(const json &ast);
public:
    AggifyCodeGenerator(const YAMLConfig &_config)
        : CFGCodeGenerator(_config){
    }

    Vec<String> run(const Function &func, const json &ast, const AggifyDFA &dfaResult, size_t id);
};