/**
 * @file cfg_code_generator.hpp
 * @brief Generate C++ code from CFG of PL/pgSQL
 * 
*/

#pragma once
#include <iostream>
#include <string>
#include <vector>
#include "compiler.hpp"
#include "compiler_fmt/core.h"
#include "yaml-cpp/yaml.h"
#include "utils.hpp"
#include "logical_operator_code_generator.hpp"

class CodeContainer2 {
public:
    // // the declaration section of all function variable (argument are already delcared in the function signature)
    // Vec<String> declarations;
    // the code generation result of all the basic blocks
    Vec<String> basicBlockCodes;
    // the subfunction used as the loop body of the vectorized function
    String body;
    // the main vectorized function to be registered to DuckDB
    String main;
    // the registration codes called by DuckDB
    // Vec<String> registration;
    String registration;
};

// class CodeGenInfo{
// public:
//     int stringFunctionCount = 0;

// };

class CFGCodeGenerator {
protected:
    CodeContainer2 container;
    const YAMLConfig &config;
    // need a connection to DuckDB to get the query plans
    // duckdb::Connection *connection;
public:
    CFGCodeGenerator(const YAMLConfig &_config)
        : config(_config){
        };
    void basicBlockCodeGenerator(BasicBlock *bb, const Function &func, CodeGenInfo &function_info);
    String extractVarFromChunk(const Function &func);
    Vec<String> run(const Function &func);

private:
    String createReturnValue(const String &retName, const Type *retType, const String &retValue);

};