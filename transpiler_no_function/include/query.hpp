#pragma once
#include <vector>
#include <iostream>
#include <regex>
#include "pg_query.h"
#include <fmt/core.h>
#include <yaml-cpp/yaml.h>
#include "json.hpp"
#include "utils.hpp"
#include <unordered_map>
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

/**
 * the binded info for each QueryNode
*/
class QueryNodeBoundInfo{
public:
    HeaderFileFunctionInfo *func_info;
    /**
     * actual types of input used
    */
    vector<UDF_Type> *input_type;
    /**
     * actual return type
    */
    UDF_Type return_type;
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
                    cout<<ret_input_pair.first.Scalar()<<endl;
                    ASSERT(ret_input_pair.first.IsScalar() && ret_input_pair.second.IsSequence(), "key should be string and value should be a sequence");
                    cout<<ret_input_pair.first.Scalar()<<endl;
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
        for(auto &pair : catalog.table){
            cout<<pair.first<<endl;
            cout<<pair.second.cpp_name<<endl;
            cout<<pair.second.templated<<endl;
            cout<<pair.second.default_null<<endl;
            cout<<pair.second.if_switch<<endl;
            cout<<pair.second.if_string<<endl;
            for(auto &input_type_vec : pair.second.input_type){
                for(auto &input_type : input_type_vec){
                    cout<<input_type.duckdb_type<<endl;
                }
            }
            for(auto &return_type : pair.second.return_type){
                cout<<return_type.duckdb_type<<endl;
            }
            cout<<endl;
        }
    }
};

class QueryNode{
public:
    /**
     * have only four types
    */
    QueryNodeType type;
    string name;
    /**
     * was orginally empty before binding
    */
    QueryNodeBoundInfo info;
    vector<QueryNode> args;
public:
    QueryNode(){};
    // QueryNode(const string &ast_type, string name): name(name){
    //     type = QueryNode::type_resolver(ast_type);
    // };
    QueryNode(QueryNodeType type, string name): type(type), name(name){};

    static void Print(const QueryNode &node, int indent = 0){
        cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]"), "", indent, node.name, getQueryNodeTypeString(node.type))<<endl;
        for(const auto &arg : node.args){
            QueryNode::Print(arg, indent+4);
        }
    }
};

class QueryAST{
    QueryNode root;
public:
    static QueryNode node_resolver(json &ast);
    static void Print(const QueryAST &ast){
        QueryNode::Print(ast.root);
    }
public:
    QueryAST(json &ast): root(QueryAST::node_resolver(ast)){
        // ASSERT(ast.is_object() && ast.size() == 1, "The root ast should be a map.");
    }
};

class QueryTranspiler{
private:
    FunctionInfo *function_info;
    const string &query_str;
    UDF_Type *expected_type;
    YAMLConfig *config;
    Catalog *catalog;
public:
    QueryTranspiler(FunctionInfo *function_info, const string &query_str, UDF_Type *expected_type, YAMLConfig *config):
                    function_info(function_info), query_str(query_str), expected_type(expected_type), config(config), catalog(catalog){};
    bool bind(QueryAST &ast);
    string run();
};