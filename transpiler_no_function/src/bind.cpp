#include "bind.hpp"

bool QueryTranspiler::bind_variable(QueryNode &node){
    if(!function_info->if_exists(node.name)){
        ERROR(fmt::format("Variable {} not found.", node.name));
    }
    node.bound_info.variable_info = &function_info->get_var_info(node.name);
    node.bound = true;
    return true;
}

bool QueryTranspiler::bind_constant(QueryNode &node){
    // cout<<node.name<<endl;
    json value = json::parse(node.name);
    // ASSERT(ast.contains("A_Const"), "The ast should be a A_Const");
    // json &value = ast["A_Const"];
    node.bound = true;
    UDF_Type const_type;
    string const_value;
    if(value.contains("fval")){
        // float constant
        const_type = UDF_Type("double");
        ASSERT(value["fval"]["fval"].is_string(), "unexpected json format");
        const_value = value["fval"]["fval"];
    }
    else if(value.contains("ival")){
        // integer constant
        const_type = UDF_Type("int");
        // this is to handle the quark in pg_query that 0 int has no ival
        if(value["ival"].empty()){
            const_value = "0";
        }
        else{
            ASSERT(value["ival"]["ival"].is_number_integer() || value["ival"]["ival"].is_boolean(), "unexpected json format");
            const_value = to_string(value["ival"]["ival"]);
        }
        
    }
    else if(value.contains("sval")){
        const_type = UDF_Type("varchar");
        ASSERT(value["sval"]["sval"].is_string(), "unexpected json format");
        const_value = value["sval"]["sval"];
    }
    else{
        ERROR("Unknown constant type in ast: \n" + value.dump(4, ' '));
    }
    curr_ast->constant_infos.emplace_back(const_value, const_type);
    node.bound_info.constant_info = &curr_ast->constant_infos.back();
    return true;
}

bool QueryTranspiler::bind_function(QueryNode &node){
    bool bind_res = true;
    for(auto &arg : node.args){
        bind_res &= bind(arg);
    }
    return bind_res;
}

bool QueryTranspiler::bind_expression(QueryNode &node){
    bool bind_res = true;
    for(auto &arg : node.args){
        bind_res &= bind(arg);
    }
    return bind_res;
}

bool QueryTranspiler::bind(QueryNode &node){
    ASSERT(node.bound == false, "The node should not be bound yet.");
    if(node.type == VARIABLE){
        if(!bind_variable(node))
            return false;
    }
    else if(node.type == FUNCTION){
        if(!bind_function(node))
            return false;
    }
    else if(node.type == CONSTANT){
        // constant should be bound already before this
        // ASSERT(false, "Unreachable");
        if(!bind_constant(node))
            return false;
    }
    else if(node.type == EXPRESSION){
        if(!bind_expression(node))
            return false;
    }
    else{
        ERROR(fmt::format("Not supported QueryNodeType: {}", getQueryNodeTypeString(node.type)));
    }
    return true;
}

/**
 * Use a depth first search to bind the QueryAST
*/
bool QueryTranspiler::bind(QueryAST &ast){
    curr_ast = &ast;
    return bind(ast.root);
    // return true;
}