#pragma once
#include <vector>
#include <iostream>
#include <regex>
#include "pg_query.h"
#define FMT_HEADER_ONLY
#include <include/fmt/core.h>
#include <yaml-cpp/yaml.h>
#include "json.hpp"
#include "utils.hpp"
#include <unordered_map>
#include "VariadicTable.h"
using namespace std;
using json = nlohmann::json;

enum QueryNodeType{FUNCTION, EXPRESSION, CONSTANT, VARIABLE};
static const char *QueryNodeTypeString[] = {"FUNCTION", "EXPRESSION", "CONSTANT", "VARIABLE"};
const char * getQueryNodeTypeString(QueryNodeType type);

/**
 *  all the necessary information used to generate the code
*/
class HeaderFileFunctionInfo{
public:
    /**
     * function name in the header file 
    */
    string cpp_name;
    /**
     * if the function definition is templated
    */
    bool templated = false;
    /**
     * if use the default null handling method which is pass NULL return NULL
    */
    bool default_null = true;
    bool if_switch = false;
    bool if_string = false;
    /**
     * length of input_type should be the same as return_type because they are 
     * one to one mapping
    */
    vector<vector<UDF_Type>> input_type;
    vector<UDF_Type> return_type;
    HeaderFileFunctionInfo(){};
    HeaderFileFunctionInfo(string cpp_name, bool templated, bool default_null, bool if_switch, bool if_string, vector<vector<UDF_Type>> input_type, vector<UDF_Type> return_type):
                            cpp_name(cpp_name), templated(templated), default_null(default_null), if_switch(if_switch), if_string(if_string), input_type(input_type), return_type(return_type){};
};

class ConstantInfo{
public:
    string value;
    UDF_Type type;
    ConstantInfo(){};
    ConstantInfo(string value, UDF_Type type): value(value), type(type){};
};

/**
 * the binded info for each QueryNode
 * this class does not itself contain the actual info except for constants, but a pointer 
 * to the actual info for example, function and expression info are stored in the catalog
 * variable info are stored in the function_info  
 * only constant info are stored here 
*/
class QueryNodeBoundInfo{
public:
    union{
        /**
         * if the QueryNode is a function, this is the function info
        */
        HeaderFileFunctionInfo *function_info;
        /**
         * if the QueryNode is a variable, this is the variable info
        */
        VarInfo *variable_info;
        /**
         * if the QueryNode is a constant, this is the constant info
        */
        ConstantInfo *constant_info;
    };
    // HeaderFileFunctionInfo *func_info;
    /**
     * actual types of input used
    */
    vector<UDF_Type> *input_type;
    /**
     * actual return type | type of the variable or constant
    */
    UDF_Type *return_type;
    QueryNodeBoundInfo(){};
};

/**
 * Essentially a table containing the QueryNode name to QueryNodeBoundInfo mapping
 * owns all the objects of QueryNodeBoundInfo
*/
class Catalog{
    unordered_map<string, HeaderFileFunctionInfo> table;
public:
    Catalog(){
        YAML::Node header_file = YAML::LoadFile("../header_file.yaml");
        for(auto function : header_file){
            ASSERT(function.first.IsScalar() && function.second.IsSequence(), fmt::format("header file around is wrong around {}", function.first.Scalar()));
            string function_name = function.first.Scalar();
            auto &function_info = function.second;
            // const auto &function_info_iter = function_info;
            ASSERT(function_info.size() == 6, "function info is not complete");
            string cpp_name = function_info[0].Scalar();
            bool templated = function_info[1].Scalar() == "true";
            bool default_null = function_info[2].Scalar() == "true";
            bool if_switch = function_info[3].Scalar() == "true";
            bool if_string = function_info[4].Scalar() == "true";
            // cout<<function_name<<cpp_name<<templated<<default_null<<if_switch<<if_string<<endl;
            // cout<<function_info[5]["options"][0]["varchar"][0]<<endl;
            ASSERT(function_info[5].IsMap() && !function_info[5]["options"].IsNull(), "options should be a map");
            ASSERT(function_info[5]["options"].IsSequence(), "input and return options should be a sequence");
            vector<vector<UDF_Type>> input_type;
            vector<UDF_Type> return_type;
            for(auto ret_input_pair_tmp : function_info[5]["options"]){
                ASSERT(ret_input_pair_tmp.size() == 1, "input and return pair should be a map");
                // this loop should have only one iteration due to a wired behavior of yaml-cpp
                // that a map with only one pair will be parsed as a sequence of maps with one pair
                for(auto ret_input_pair : ret_input_pair_tmp){
                    // cout<<ret_input_pair.first.Scalar()<<endl;
                    ASSERT(ret_input_pair.first.IsScalar() && ret_input_pair.second.IsSequence(), "key should be string and value should be a sequence");
                    // cout<<ret_input_pair.first.Scalar()<<endl;
                    return_type.emplace_back(ret_input_pair.first.Scalar());
                    vector<UDF_Type> input_type_vec;
                    for(auto input_type_str : ret_input_pair.second){
                        input_type_vec.emplace_back(input_type_str.Scalar());
                    }
                    input_type.emplace_back(input_type_vec);
                }
            }
            table.emplace(function_name, HeaderFileFunctionInfo(cpp_name, templated, default_null, if_switch, if_string, input_type, return_type));
        }
    }
    static void Print(Catalog catalog){
        VariadicTable<string, string, string, string, string, string, string, string> vt({"SQL Name", "C++ Name", "Templated", "Default NULL", "Switch", "String Op", "Inputs", "Return"}, 10);
        for(auto &pair : catalog.table){
            string return_type_str;
            bool first = true;
            auto return_types = pair.second.return_type;
            auto input_types = pair.second.input_type;
            for(int i = 0; i < return_types.size(); i++){
                // return_type_str += return_type.duckdb_type + ", ";
                auto return_type = return_types[i];
                auto input_type = input_types[i];
                string return_type_str = return_type.duckdb_type;
                string input_type_str = "";
                for(int j = 0; j < input_type.size(); j++){
                    input_type_str += input_type[j].duckdb_type;
                    if(j != input_type.size()-1)
                        input_type_str += ", ";
                }
                if(first){
                    vt.addRow(pair.first, pair.second.cpp_name, pair.second.templated ? "true" : "false", pair.second.default_null ? "true" : "false", pair.second.if_switch ? "true" : "false", pair.second.if_string ? "true" : "false", input_type_str, return_type_str);
                    first = false;
                }
                else
                    vt.addRow("", "", "", "", "", "", input_type_str, return_type_str);
            }
            // cout<<endl;
        }
        vt.print(cout);
    }
};

class QueryNode{
public:
    /**
     * have only four types
    */
    QueryNodeType type;
    string name;
    bool bound = false;
    /**
     * was orginally empty before binding
    */
    QueryNodeBoundInfo bound_info;
    vector<QueryNode> args;
public:
    QueryNode(){};
    // QueryNode(const string &ast_type, string name): name(name){
    //     type = QueryNode::type_resolver(ast_type);
    // };
    QueryNode(QueryNodeType type, string name): type(type), name(name){};

    static void Print(const QueryNode &node, int indent = 0){
        // if(node.type == CONSTANT)
        //     cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]->{}"), "", indent, node.bound_info.constant_info->value, getQueryNodeTypeString(node.type), node.bound ? "bound" : "")<<endl;
        // else if(node.type == VARIABLE)
        //     cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]->{}"), "", indent, node.name, getQueryNodeTypeString(node.type), node.bound ? "bound" : "")<<endl;
        // else if(node.type == FUNCTION)
        //     cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]->{}"), "", indent, node.name, getQueryNodeTypeString(node.type), node.bound ? "bound" : "")<<endl;
        // else if(node.type == EXPRESSION)
        cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]->{}"), "", indent, node.name, getQueryNodeTypeString(node.type), node.bound ? "bound" : "")<<endl;
        for(const auto &arg : node.args){
            QueryNode::Print(arg, indent+4);
        }
    }
};

class QueryTranspiler;

/**
 * the self defined AST for the query, should leave room for the binding step
*/
class QueryAST{
public:
    vector<ConstantInfo> constant_infos;
    QueryNode root;
    static QueryNode resolve_node(json &ast);
    static void Print(const QueryAST &ast){
        QueryNode::Print(ast.root);
    }
public:
    /**
     * resolve the libpg_query returned ast recursively to QueryNode
     * use the transpiler pointer to bind consts in advanced
    */
    QueryAST(json &ast): 
        root(resolve_node(ast)){
        // ASSERT(ast.is_object() && ast.size() == 1, "The root ast should be a map.");
    }
};

/**
 * the transpiler class that transpile the QueryAST to C++ code
 * it controls the whole lifecycle of QueryAST including construction, binding and transpiling
*/
class QueryTranspiler{
private:
    FunctionInfo *function_info;
    /**
     * tried to use const reference but failed due to bug in nlohmann::json
    */
    string query_str;
    UDF_Type *expected_type;
    YAMLConfig *config;
    Catalog *catalog;
    bool bind(QueryNode &node);
    bool bind_variable(QueryNode &node);
    bool bind_function(QueryNode &node);
    bool bind_expression(QueryNode &node);
    /** no need make it public to let the QueryAST call this function*/
    bool bind_constant(QueryNode &node);
    QueryAST *curr_ast = NULL;
// public:
public:
    QueryTranspiler(FunctionInfo *function_info, string query_str, UDF_Type *expected_type, YAMLConfig *config, Catalog *catalog):
                    function_info(function_info), query_str(query_str), expected_type(expected_type), config(config), catalog(catalog){
        // cout<<query_str<<endl;
    }
    bool bind(QueryAST &ast);
    string run();
};