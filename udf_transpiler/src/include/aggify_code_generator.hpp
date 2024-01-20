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
using namespace std;
using json = nlohmann::json;

class AggifyCodeGenerator {
private:
    const YAMLConfig &config;
    // const json &ast;
    // const Function &func;
public:
    vector<string> code;
    AggifyCodeGenerator(const YAMLConfig &_config)
        : config(_config) { 
    }

    vector<string> run(const Function &func, const json &ast);
};