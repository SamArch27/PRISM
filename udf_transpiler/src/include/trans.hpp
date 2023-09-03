#pragma once
#include "duckdb/main/connection.hpp"
#include <vector>
#include <iostream>
#include <regex>
#include "pg_query.h"
#define FMT_HEADER_ONLY
#include <include/fmt/core.h>
#include "yaml-cpp/yaml.h"
#include "json.hpp"
#include "utils.hpp"
#include "logical_operator_code_generator.hpp"
// #include "query.hpp"
using namespace std;
using json = nlohmann::json;

// vector<string> transpile_plpgsql_udf_str(string &&udf_str);
class Transpiler
{
public:
    // Transpiler();
    Transpiler(string &&udf_str, YAMLConfig *config, duckdb::Connection &connection) : 
        udf_str(udf_str), config(config), connection(connection){};
    vector<string> run();

private:
    CodeContainer cc;
    std::unique_ptr<FunctionInfo> function_info;
    string udf_str;
    YAMLConfig *config;
    duckdb::Connection &connection;             /** connection to duckdb, used by code gen*/
    // Catalog *catalog;

private:
    string translate_query(json &query, duckdb::CodeInsertionPoint &insert, UDF_Type *expected_type, bool query_is_assignment);
    string translate_expr(json &expr, duckdb::CodeInsertionPoint &insert, UDF_Type *expected_type, bool query_is_assignment);
    string translate_assign_stmt(json &stmt);
    string translate_return_stmt(json &stmt);
    string translate_cond_stmt(json &cond_stmt, duckdb::CodeInsertionPoint &insert);
    string translate_if_stmt(json &stmt);
    string translate_loop_stmt(json &stmt);
    string translate_for_stmt(json &stmt);
    string translate_while_stmt(json &stmt);
    string translate_stmt_block(json &stmt_block);
    string translate_exitcont_stmt(json &stmt);
    string get_function_vars(json &datums, string &udf_str);
    string translate_body(json &body, UDF_Type *expectd_type = NULL);
    string translate_action(json &action);
    vector<string> translate_function(json &ast, string &udf_str);
    void parse_assignment(const string &query, string &lvalue, string &rvalue);
};