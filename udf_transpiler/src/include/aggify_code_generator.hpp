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
using namespace std;
using json = nlohmann::json;

/**
 * It should be able to register the custom aggregate to DuckDB
*/
class AggifyCodeGenerator : public CFGCodeGenerator{
private:
    // const YAMLConfig &config;
    // const json &ast;
    // const Function &func;
    vector<string> getOrginalCursorLoopCol(const json &ast);
public:
    AggifyCodeGenerator(const YAMLConfig &_config)
        : CFGCodeGenerator(_config){
    }

    vector<string> run(const Function &func, const json &ast, const AggifyDFA &dfaResult, size_t id);
};