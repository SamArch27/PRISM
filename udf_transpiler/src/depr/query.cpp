#include "query.hpp"
#include "bind.hpp"
using namespace std;

const char * getQueryNodeTypeString(QueryNodeType type){
    return QueryNodeTypeString[type];
}

/**
 * resolve the libpg_query returned ast recursively to QueryNode
*/
QueryNode QueryAST::resolve_node(json &ast){
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
        // ERROR("Unsupported BoolExpr: \n"+ast.dump(4, ' '));
        if(value["boolop"] == "AND_EXPR")
            res.name = "and";
        else if(value["boolop"] == "OR_EXPR")
            res.name = "or";
        else
            ERROR("Unsupported BoolExpr: \n"+ast.dump(4, ' '));
        ASSERT(value["args"].size() == 2, "BoolExpr should have 2 args.");
        res.args.push_back(resolve_node(value["args"][0]));
        res.args.push_back(resolve_node(value["args"][1]));
    }
    else if(ast_type == "A_Expr"){
        res.type = EXPRESSION;
        res.name = value["name"][0]["String"]["sval"];
        if(!value["lexpr"].empty()){
            res.args.push_back(resolve_node(value["lexpr"]));
        }
        if(!value["rexpr"].empty()){
            res.args.push_back(resolve_node(value["rexpr"]));
        }
    }
    else if(ast_type == "CaseExpr"){
        res.type = EXPRESSION;
        ERROR("Unsupported CaseExpr");
    }
    else if(ast_type == "TypeCast"){
        res.type = FUNCTION;
        res.name = "::";
    }
    else if(ast_type == "A_Const"){
        res.type = CONSTANT;
        // ERROR("Unsupported ConstExpr");
        // transpiler->bind_constant(ast, res);
        res.name = value.dump();
    }
    else if(ast_type == "FuncCall"){
        res.type = FUNCTION;
        res.name = value["funcname"][0]["String"]["sval"];
        for(auto &arg : value["args"]){
            res.args.push_back(QueryAST::resolve_node(arg));
        }
    }
    else{
        ERROR(fmt::format("Not supported libpg_query type: \n{}", ast.dump(4, ' ')));
    }
    return res;
}

string QueryTranspiler::run(){
    // string tmp = query_str.substr(1, query_str.size() - 1);
    // if(query_str.starts_with('(') and query_str.ends_with(')'))
    //     query_str = query_str.substr(1, query_str.size() - 2);
    cout<<"select " + query_str<<endl;
    auto result = pg_query_parse(("select " + query_str).c_str());
    if(result.error){
        ERROR(result.error->message);
        // todo: think of the memory leak
    }
    json ast = json::parse(result.parse_tree);
    ast = ast["stmts"][0]["stmt"]["SelectStmt"]["targetList"][0]["ResTarget"]["val"];
    ASSERT(!ast.empty(), "The generated ast is empty.");
    QueryAST transpiler_ast(ast);
    bind(transpiler_ast);
    QueryAST::Print(transpiler_ast);
    // cout<<ast.dump(4, ' ')<<endl;
    pg_query_free_parse_result(result);
    return "";
}