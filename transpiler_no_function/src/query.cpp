#include "query.hpp"
#include "bind.hpp"
using namespace std;

const char * getQueryNodeTypeString(QueryNodeType type){
    return QueryNodeTypeString[type];
}

/**
 * resolve the libpg_query returned ast recursively to QueryNode
*/
QueryNode QueryAST::node_resolver(json &ast){
    ASSERT(ast.is_object() && ast.size() == 1, "The root ast should be a map.");
    const string &ast_type = ast.begin().key();
    json &value = ast.begin().value();
    // QueryNodeType type;
    QueryNode res;
    if(ast_type == "ColumnRef"){
        res.type = VARIABLE;
        res.name = value["fields"][0]["String"]["sval"];
    }
    else if(ast_type == "BoolExpr"){
        res.type = EXPRESSION;
    }
    else if(ast_type == "A_Expr"){
        res.type = EXPRESSION;
        res.name = value["name"][0]["String"]["sval"];
        if(!value["lexpr"].empty()){
            res.args.push_back(QueryAST::node_resolver(value["lexpr"]));
        }
        if(!value["rexpr"].empty()){
            res.args.push_back(QueryAST::node_resolver(value["rexpr"]));
        }
    }
    else if(ast_type == "CaseExpr"){
        res.type = EXPRESSION;
        
    }
    else if(ast_type == "TypeCast"){
        res.type = FUNCTION;
        res.name = "::";
    }
    else if(ast_type == "A_Const"){
        res.type = CONSTANT;
    }
    else if(ast_type == "FuncCall"){
        res.type = FUNCTION;
        res.name = value["funcname"][0]["String"]["sval"];
        for(auto &arg : value["args"]){
            res.args.push_back(QueryAST::node_resolver(arg));
        }
    }
    else{
        throw runtime_error(fmt::format("Not supported libpg_query type: {}", ast_type));
    }
    return res;
}

string QueryTranspiler::run(){
    cout<<query_str<<endl;
    auto result = pg_query_parse(("select" + query_str).c_str());
    json ast = json::parse(result.parse_tree);
    ast = ast["stmts"][0]["stmt"]["SelectStmt"]["targetList"][0]["ResTarget"]["val"];
    ASSERT(!ast.empty(), "The generated ast is empty.");
    QueryAST transpiler_ast(ast);
    bind(transpiler_ast);
    QueryAST::Print(transpiler_ast);
    cout<<ast.dump(4, ' ')<<endl;
    return "";
}