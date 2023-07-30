#include "bind.hpp"

bool QueryTranspiler::bind_variable(QueryNode &node){
    if(!function_info->if_exists(node.name)){
        throw runtime_error(fmt::format("Variable {} not found.", node.name));
    }
    node.bound_info.variable_info = &function_info->get_var_info(node.name);
    node.bound = true;
    return true;
}

bool QueryTranspiler::bind(QueryNode &node){
    if(node.type == VARIABLE){
        if(!bind_variable(node))
            return false;
    }
    else if(node.type == FUNCTION){
        // if(!bind_function(node))
        //     return false;
    }
    else if(node.type == CONSTANT){
        // if(!bind_constant(node))
        //     return false;
    }
    else if(node.type == EXPRESSION){
        // if(!bind_expression(node))
        //     return false;
    }
    else{
        throw runtime_error(fmt::format("Not supported QueryNodeType: {}", getQueryNodeTypeString(node.type)));
    }
    return true;
}

/**
 * Use a depth first search to bind the QueryAST
*/
bool QueryTranspiler::bind(QueryAST &ast){
    QueryNode &root = ast.root;
    for(auto &arg : root.args){
        auto bind_res = bind(arg);
            // return false;
    }
    return bind(root);
    // return true;
}