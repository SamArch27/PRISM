#ifndef TRANS
#define TRANS
#include <vector>
#include <iostream>
#include <regex>
#include "pg_query.h"
#include <fmt/core.h>
#include <yaml-cpp/yaml.h>
#include "json.hpp"
#include "utils.hpp"
using namespace std;
using json = nlohmann::json;

// vector<string> transpile_plpgsql_udf_str(string &&udf_str);
class Transpiler
{
public:
    // Transpiler();
    Transpiler(string &&udf_str, YAMLConfig *config) : udf_str(udf_str), config(config){};
    vector<string> run();

private:
    CodeContainer cc;
    std::unique_ptr<FunctionInfo> function_info;
    string udf_str;
    YAMLConfig *config;

private:
    string translate_expr(json &expr, UDF_Type &expected_type, bool query_is_assignment);
    string translate_action(json &action);
    string get_function_vars(json &datums, string &udf_str);
    vector<string> translate_function(json &ast, string &udf_str);
};

#endif