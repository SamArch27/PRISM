#ifndef UTILS
#define UTILS
#include <iostream>
#include <utility>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <numeric>
#include <fmt/core.h>
#include <algorithm>
#include <regex>
using namespace std;


#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion failed: " << message << " (" << __FILE__ << ":" << __LINE__ << ")" << std::endl; \
            std::terminate(); \
        } \
    } while (false)

static unordered_map<string, string> alias_to_duckdb_type = {{"BIGINT", "BIGINT"}, {"INT8", "BIGINT"}, {"LONG", "BIGINT"}, {"BIT", "BIT"}, {"BITSTRING", "BIT"}, {"BOOLEAN", "BOOLEAN"}, {"BOOL", "BOOLEAN"}, {"LOGICAL", "BOOLEAN"}, {"BLOB", "BLOB"}, {"BYTEA", "BLOB"}, {"BINARY", "BLOB"}, {"VARBINARY", "BLOB"}, {"DATE", "DATE"}, {"DOUBLE", "DOUBLE"}, {"FLOAT8", "DOUBLE"}, {"NUMERIC", "DOUBLE"}, {"DECIMAL", "DOUBLE"}, {"HUGEINT", "HUGEINT"}, {"INTEGER", "INTEGER"}, {"INT", "INTEGER"}, {"INT4", "INTEGER"}, {"SIGNED", "INTEGER"}, {"INTERVAL", "INTERVAL"}, {"REAL", "REAL"}, {"FLOAT4", "REAL"}, {"FLOAT", "REAL"}, {"SMALLINT", "SMALLINT"}, {"INT2", "SMALLINT"}, {"SHORT", "SMALLINT"}, {"TIME", "TIME"}, {"TIMESTAMP", "TIMESTAMP"}, {"DATETIME", "TIMESTAMP"}, {"TINYINT", "TINYINT"}, {"INT1", "TINYINT"}, {"UBIGINT", "UBIGINT"}, {"UINTEGER", "UINTEGER"}, {"USMALLINT", "USMALLINT"}, {"UTINYINT", "UTINYINT"}, {"UUID", "UUID"}, {"VARCHAR", "VARCHAR"}, {"CHAR", "VARCHAR"}, {"BPCHAR", "VARCHAR"}, {"TEXT", "VARCHAR"}, {"STRING", "VARCHAR"}};

class UDF_Type{
    public:
    string duckdb_type;
    
    /**
     * @brief Resolve a type name to a C++ type name and a type size.
    */
    static string resolve_type(string &type_name, const string &udf_str){
        // std::cout<<type_name<<std::endl;
        // std::cout<<std::stoi("#28#")<<std::endl;
        if(type_name.starts_with('#')){
            if(udf_str.size() == 0){
                throw std::runtime_error("UDF string is empty");
            }
            int type_start = type_name.find('#');
            // int type_end = type_name.find('#', type_start);
            type_start = std::stoi(type_name.substr(type_start+1));
            int type_end = type_start;
            while(isalpha(udf_str[type_end])){
                type_end++;
            }
            string real_type_name = udf_str.substr(type_start, type_end - type_start);
            return resolve_type(real_type_name, udf_str);
        }

        std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::toupper);
        if(type_name.starts_with("DECIMAL")){
            std::regex type_pattern("^DECIMAL ?(\\(\\d+,\\d+\\))", std::regex_constants::icase);
            std::smatch tmp_types;
            std::regex_search(type_name, tmp_types, type_pattern);
            if(tmp_types.size() > 0){
                return type_name;
            }
        }
        if(alias_to_duckdb_type.contains(type_name)){
            return alias_to_duckdb_type.at(type_name);
        }
        else{
            throw runtime_error(fmt::format("Unknown type: {}", type_name));
        }
    }

    UDF_Type(){};

    UDF_Type(string &type_name, const string &udf_str){
        duckdb_type = UDF_Type::resolve_type(type_name, udf_str);
    }
    
    string get_duckdb_type(){
        return "duckdb::LogicalType::"+duckdb_type;
    }

    string get_cpp_type(){
        return "";
    }
    
    bool is_unknown(){
        return strcmp(duckdb_type.c_str(), "UNKNOWN") == 0;
    }
};

class VarInfo{
    public:
    int id;
    bool init;
    UDF_Type type;
    VarInfo(){};
    VarInfo(int id, string &type_name, const string &udf_str, bool i): id(id), init(i), type(type_name, udf_str){};

};

class CodeContainer{
    public:
    bool query_macro = false;
    vector<string> global_macros;
    vector<string> global_variables;
    vector<string> global_functions;
};

class GV{
    public:
    int function_count = 0;
    int vector_size = 2048;
    // func_vars
    string func_name;
    UDF_Type func_return_type;
    int temp_var_count = 0;
    unordered_map<string, VarInfo> func_args;
    // temp_var_subs
};

string vec_join(vector<string> &vec, const string &del);


#endif
