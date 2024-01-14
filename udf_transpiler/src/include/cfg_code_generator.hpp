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
#include "include/fmt/core.h"
#include "yaml-cpp/yaml.h"
#include "utils.hpp"
#include "logical_operator_code_generator.hpp"

using namespace std;

class CodeContainer2 {
public:
    // // the declaration section of all function variable (argument are already delcared in the function signature)
    // vector<string> declarations;
    // the code generation result of all the basic blocks
    vector<string> basicBlockCodes;
    // the subfunction used as the loop body of the vectorized function
    string body;
    // the main vectorized function to be registered to DuckDB
    string main;
    // the registration codes called by DuckDB
    vector<string> registration;
};

// class CodeGenInfo{
// public:
//     int stringFunctionCount = 0;

// };

class CFGCodeGenerator {
private:
    CodeContainer2 container;
    YAMLConfig config;
    // need a connection to DuckDB to get the query plans
    duckdb::Connection *connection;
public:
    CFGCodeGenerator(duckdb::Connection *connection)
        : connection(connection){
        };
    void basicBlockCodeGenerator(BasicBlock *bb, const Function &func, CodeGenInfo &function_info);
    std::string extractVarFromChunk(const Function &func);
    void run(const Function &func);
private:
    string createReturnValue(const string &retName, const Type *retType, const string &retValue);

};