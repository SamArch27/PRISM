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
using namespace std;
using json = nlohmann::json;

class AggifyCodeGenerator : public CFGCodeGenerator{
private:
    // const YAMLConfig &config;
    // const json &ast;
    // const Function &func;
public:
    AggifyCodeGenerator(const YAMLConfig &_config)
        : CFGCodeGenerator(_config){
    }

    vector<string> run(const Function &func, const json &ast, size_t id);
};