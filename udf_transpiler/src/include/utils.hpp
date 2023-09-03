#pragma once
#include <string>
#include <iostream>
#include <utility>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <numeric>
#define FMT_HEADER_ONLY
#include <include/fmt/core.h>
#include <algorithm>
#include <regex>
#include <yaml-cpp/yaml.h>
using namespace std;

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::exit(EXIT_FAILURE); \
        } \
    } while (false)

#define ERROR(message)                                                                                   \
    do                                                                                                               \
    {                                                                                                                \
        std::cerr << "Error: " << message << " (" << __FILE__ << ":" << __LINE__ << ")" << std::endl; \
        std::terminate();                                                                                        \
    } while (false)

#define EXCEPTION(message)                                                                                   \
    do                                                                                                               \
    {                                                                                                                \
        std::cerr << "Exception: " << message << " (" << __FILE__ << ":" << __LINE__ << ")" << std::endl; \
        throw exception();                                                                                      \
    } while (false)
template <typename T>
std::string vec_join(std::vector<T> &vec, std::string sep){
    std::string result = "";
    for(auto &item : vec){
        result += std::to_string(item) + sep;
    }
    return result.substr(0, result.size() - sep.size());
}

std::string vec_join(std::vector<std::string> &vec, std::string sep);

void remove_spaces(std::string &str);

static unordered_map<string, string> alias_to_duckdb_type = {{"UNKNOWN", "UNKNOWN"}, {"BIGINT", "BIGINT"}, {"INT8", "BIGINT"}, {"LONG", "BIGINT"}, {"BIT", "BIT"}, {"BITSTRING", "BIT"}, {"BOOLEAN", "BOOLEAN"}, {"BOOL", "BOOLEAN"}, {"LOGICAL", "BOOLEAN"}, {"BLOB", "BLOB"}, {"BYTEA", "BLOB"}, {"BINARY", "BLOB"}, {"VARBINARY", "BLOB"}, {"DATE", "DATE"}, {"DOUBLE", "DOUBLE"}, {"FLOAT8", "DOUBLE"}, {"NUMERIC", "DOUBLE"}, {"DECIMAL", "DOUBLE"}, {"HUGEINT", "HUGEINT"}, {"INTEGER", "INTEGER"}, {"INT", "INTEGER"}, {"INT4", "INTEGER"}, {"SIGNED", "INTEGER"}, {"INTERVAL", "INTERVAL"}, {"REAL", "REAL"}, {"FLOAT4", "REAL"}, {"FLOAT", "REAL"}, {"SMALLINT", "SMALLINT"}, {"INT2", "SMALLINT"}, {"SHORT", "SMALLINT"}, {"TIME", "TIME"}, {"TIMESTAMP", "TIMESTAMP"}, {"DATETIME", "TIMESTAMP"}, {"TINYINT", "TINYINT"}, {"INT1", "TINYINT"}, {"UBIGINT", "UBIGINT"}, {"UINTEGER", "UINTEGER"}, {"USMALLINT", "USMALLINT"}, {"UTINYINT", "UTINYINT"}, {"UUID", "UUID"}, {"VARCHAR", "VARCHAR"}, {"CHAR", "VARCHAR"}, {"BPCHAR", "VARCHAR"}, {"TEXT", "VARCHAR"}, {"STRING", "VARCHAR"}};
static unordered_map<string, string> duckdb_to_cpp_type = {{"BOOLEAN", "bool"}, {"TINYINT", "int8_t"}, {"SMALLINT", "int16_t"}, {"DATE", "int32_t"}, {"TIME", "int32_t"}, {"INTEGER", "int32_t"}, {"BIGINT", "int64_t"}, {"TIMESTAMP", "int64_t"}, {"FLOAT", "float"}, {"DOUBLE", "double"}, {"DECIMAL", "double"}, {"VARCHAR", "string_t"}, {"CHAR", "string_t"}, {"BLOB", "string_t"}};

class UDF_Type
{
public:
    string duckdb_type;

    static string resolve_type(string type_name, const string &udf_str);
    static void get_decimal_width_scale(string &duckdb_type, int &width, int &scale);
    static string get_decimal_int_type(int width, int scale);

    UDF_Type(){};

    UDF_Type(const string &type_name){
        duckdb_type = UDF_Type::resolve_type(type_name, "");
    }

    UDF_Type(const string &type_name, const string &udf_str)
    {
        duckdb_type = UDF_Type::resolve_type(type_name, udf_str);
    }

    string get_duckdb_type()
    {
        return "duckdb::LogicalType::" + duckdb_type;
    }

    string get_cpp_type();
    
    string create_duckdb_value(const string &ret_name, const string &cpp_value);

    bool is_unknown()
    {
        return strcmp(duckdb_type.c_str(), "UNKNOWN") == 0;
    }
};

class VarInfo
{
public:
    int id;
    bool init;
    UDF_Type type;
    VarInfo(){};
    VarInfo(int id, string &type_name, const string &udf_str, bool i) : id(id), init(i), type(type_name, udf_str){};
};

class CodeContainer
{
public:
    bool query_macro = false;
    vector<string> global_macros;
    vector<string> global_variables;
    vector<string> global_functions;
};

class FunctionInfo
{
public:
    int function_count = 0;
    int vector_size = 2048;
    string func_name;
    UDF_Type func_return_type;
    int tmp_var_count = 0;
    vector<string> func_args_vec;
    unordered_map<string, VarInfo> func_args;
    unordered_map<string, VarInfo> func_vars;
    /**
     * 
    */
    unordered_map<string, string> tmp_var_substitutes;
    // temp_var_subs
    string new_variable(){
        tmp_var_count += 1;
        return "tmpvar" + std::to_string(tmp_var_count);
    }

    bool if_exists(const string &var_name){
        return func_vars.find(var_name) != func_vars.end() || func_args.find(var_name) != func_args.end();
    }

    VarInfo &get_var_info(const string &var_name){
        if(func_vars.find(var_name) != func_vars.end()){
            if(tmp_var_substitutes.count(var_name)){
                return func_vars[tmp_var_substitutes[var_name]];
            }
            return func_vars[var_name];
        }
        else if(func_args.find(var_name) != func_args.end()){
            return func_args[var_name];
        }
        else{
            ERROR(fmt::format("Variable {} not found.", var_name));
        }
    }

    vector<pair<const string &, const VarInfo &>> get_vars(){
        vector<pair<const string &, const VarInfo &>> ret;
        for(auto &var : func_vars){
            if(tmp_var_substitutes.count(var.first)) continue;
            // cout<<var.first<<" "<<var.second.type.duckdb_type<<endl;
            ret.push_back({var.first, var.second});
        }
        for(auto &var : func_args){
            // cout<<var.first<<" "<<var.second.type.duckdb_type<<endl;
            ret.push_back({var.first, var.second});
        }
        return ret;
    }

};

class YAMLConfig
{
public:
    YAML::Node query;
    YAML::Node function;
    YAML::Node control;
    YAMLConfig();
};

bool is_const_or_var(string &expr, FunctionInfo &funtion_info, string &res);




