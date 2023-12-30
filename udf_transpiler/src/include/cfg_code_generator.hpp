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

using namespace std;

class CodeContainer2 {
public:
//   bool query_macro = false;
//   vector<string> global_macros;
    // the declaration section of all function variable (argument are already delcared in the function signature)
    vector<string> declarations;
    // the loop body of the vectorized function
    vector<string> body;
    // the main vectorized function to be registered to DuckDB
    vector<string> main;
    // the registration codes called by DuckDB
    vector<string> registration;
};

class CFGCodeGenerator {
private:
    CodeContainer2 cc;
    YAMLConfig config;
    // need a connection to DuckDB to get the query plans
    duckdb::Connection *connection;
public:
    CFGCodeGenerator(duckdb::Connection *connection)
        : connection(connection){
        };
    void basicBlockCodeGenerator(BasicBlock *bb);
    void run(const Function &func);
};