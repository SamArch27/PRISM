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
    string name;
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
};

/**
 * the binded info for each QueryNode
*/
class QueryNodeBoundInfo{
public:
    const HeaderFileFunctionInfo &func_info;
    /**
     * actual types of input used
    */
    const vector<UDF_Type> &input_type;
    /**
     * actual return type
    */
    const UDF_Type &return_type;
};

/**
 * Essentially a table containing the QueryNode name to QueryNodeBoundInfo mapping
 * owns all the objects of QueryNodeBoundInfo
*/
class Catalog{
    unordered_map<string, QueryNodeBoundInfo> table;
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
        ASSERT(ast.is_object() && ast.size() == 1, "The root ast should be a map.");
    }
};

class QueryTranspiler{
private:
    FunctionInfo *function_info;
    const string &query_str;
    UDF_Type *expected_type;
    YAMLConfig *config;

public:
    QueryTranspiler(FunctionInfo *function_info, const string &query_str, UDF_Type *expected_type, YAMLConfig *config):
                    function_info(function_info), query_str(query_str), expected_type(expected_type), config(config){};
    void bind(QueryAST &ast);
    string run();
};