#include "trans.hpp"
// #include "logical_operator_code_generator.hpp"
using namespace std;
using json = nlohmann::json;

/**
 * @brief Parse an assignment query. May raise exception if wrong.
*/
void Transpiler::parse_assignment(const string &query, string &lvalue, string &rvalue){
    std::regex assign_pattern("\\:?\\=", std::regex_constants::icase);
    std::smatch equal_match;
    // std::cout << tmp_string << '\n';
    std::regex_search(query, equal_match, assign_pattern);
    lvalue = equal_match.prefix();
    rvalue = equal_match.suffix();
}

// todo
string Transpiler::translate_query(json &query, duckdb::CodeInsertionPoint &insert, UDF_Type *expected_type, bool query_is_assignment = false){
    ASSERT(query.is_string(), "Query statement should be a string.");
    // cout<<query<<endl;
    // todo substitute variable
    // todo query is assignment
    string result;
    if(query_is_assignment){
        ASSERT(expected_type == NULL, "In assignment, expected_type should be NULL.");
        string lvalue;
        string rvalue;
        parse_assignment(query, lvalue, rvalue);
        
        // QueryTranspiler query_transpiler(function_info.get(), rvalue, expected_type, config, catalog);
        // result = query_transpiler.run();
        // duckdb::CodeInsertionPoint insert(function_info->string_function_count);
        duckdb::LogicalOperatorCodeGenerator code_generator;
        result += fmt::format("{} = ", lvalue);
        result += code_generator.run(connection, "select " + rvalue, function_info->get_vars(), insert);
    }
    else{
        // QueryTranspiler query_transpiler(function_info.get(), query, expected_type, config, catalog);
        // result = query_transpiler.run();
        // duckdb::CodeInsertionPoint insert(function_info->string_function_count);
        duckdb::LogicalOperatorCodeGenerator code_generator;
        // string quer
        result = code_generator.run(connection, "select " + query.get<string>(), function_info->get_vars(), insert);
    }
    // cout<<result<<endl;
    // return fmt::format("[Unresolved Query: {}]", query.dump());
    return result;
}

string Transpiler::translate_expr(json &expr, duckdb::CodeInsertionPoint &insert, UDF_Type *expected_type, bool query_is_assignment = false){
    if(expr.contains("query") and expr.size() == 1){
        return translate_query(expr["query"], insert, expected_type, query_is_assignment);
    }
    else{
        EXCEPTION(fmt::format("Unsupport PLpgSQL_expr: {}", expr.dump()));
    }
    return "";
}

/**
 * example: {'lineno': 5, 'varno': 3, 'expr': {'PLpgSQL_expr': {'query': 'pd2 := pd'}}
*/
string Transpiler::translate_assign_stmt(json &assign){
    ASSERT(assign["expr"].contains("PLpgSQL_expr"), "Assignment must be in form of expression.");
    duckdb::CodeInsertionPoint insert(function_info->string_function_count);
    auto res = translate_expr(assign["expr"]["PLpgSQL_expr"], insert, NULL, true);
    return insert.to_string() + res + ";\n";
}

string Transpiler::translate_return_stmt(json &stmt){
    ASSERT(stmt.size() == 2, "Return_stmt should only have lineno and expr.");
    ASSERT(stmt["expr"].contains("PLpgSQL_expr"), "Return_stmt expression should have PLpgSQL_expr.");
    duckdb::CodeInsertionPoint insert(function_info->string_function_count);
    string ret = fmt::format("{};\nreturn;\n", function_info->func_return_type.create_duckdb_value(\
                            config->function["return_name"].Scalar(),\
                            translate_expr(stmt["expr"]["PLpgSQL_expr"], insert, &function_info->func_return_type)));
    return insert.to_string() + ret;
}

string Transpiler::translate_cond_stmt(json &cond_stmt, duckdb::CodeInsertionPoint &insert){
    ASSERT(cond_stmt.contains("PLpgSQL_expr"), "cond stmt should contain PLpgSQL_expr.");
    string tmp = "BOOL";
    UDF_Type cond_type(tmp);
    auto res = translate_expr(cond_stmt["PLpgSQL_expr"], insert, &cond_type);
    return res;
}

string Transpiler::translate_if_stmt(json &if_stmt){
    ASSERT(if_stmt.contains("cond"), "if stmt should have cond.");
    duckdb::CodeInsertionPoint insert(function_info->string_function_count);
    string cond_expr = translate_cond_stmt(if_stmt["cond"], insert);
    string then_body = "";
    string elseifs = "";
    string _else = "";
    if(if_stmt.contains("then_body")){
        then_body = translate_body(if_stmt["then_body"]);
    }
    if(if_stmt.contains("elsif_list")){
        for(auto &elseif_outer : if_stmt["elsif_list"]){
            ASSERT(elseif_outer.contains("PLpgSQL_if_elsif"),\
                       "elsif should be PLpgSQL_if_elsif.");
            auto &elseif = elseif_outer["PLpgSQL_if_elsif"];
            ASSERT(elseif.contains("cond"), "elseif should have cond.");
            string elseif_then_body;
            if(elseif.contains("stmts")){
                elseif_then_body = translate_body(elseif["stmts"]);
            }
            else{
                elseif_then_body = "";
            }
            elseifs += fmt::format(fmt::runtime(config->control["else_if"].Scalar()), \
                                    fmt::arg("condition", translate_cond_stmt(elseif["cond"], insert)), \
                                    fmt::arg("then_body", elseif_then_body));
            elseifs += "\n";

        }
    }
    if(if_stmt.contains("else_body")){
        _else = fmt::format(fmt::runtime(config->control["else"].Scalar()), \
                            fmt::arg("else_body", translate_body(if_stmt["else_body"])));
        _else += "\n";                                        
    }
    return insert.to_string() + fmt::format(fmt::runtime(config->control["if_block"].Scalar()), \
                                        fmt::arg("condition", cond_expr), \
                                        fmt::arg("then_body", then_body), \
                                        fmt::arg("elseifs", elseifs), \
                                        fmt::arg("else", _else));
}

/**
 * loop is a while loop with the condition always being true
*/
string Transpiler::translate_loop_stmt(json &loop_stmt){
    return fmt::format(fmt::runtime(config->control["simple"].Scalar()), \
                        fmt::arg("body", translate_body(loop_stmt["body"])));
}

string Transpiler::translate_for_stmt(json &for_stmt){
    auto &for_var = for_stmt["var"]["PLpgSQL_var"];
    string for_var_name = for_var["refname"];

    string prev_sub = "";

    // cache the previous name if any
    if(function_info->tmp_var_substitutes.contains(for_var_name)){
        prev_sub = function_info->tmp_var_substitutes[for_var_name];
    }

    string tmp_var_name = function_info->new_variable();
    function_info->tmp_var_substitutes[for_var_name] = tmp_var_name;
    ASSERT(!function_info->func_vars.contains(tmp_var_name) and !function_info->tmp_var_substitutes.contains(tmp_var_name), \
            fmt::format("temporary loop variable {} cannot be used elsewhere", tmp_var_name));
    string for_var_type = "INTEGER";
    function_info->func_vars[tmp_var_name] = VarInfo(-1, for_var_type, udf_str, true);
    
    bool is_reverse = for_stmt.contains("reverse");
    if(is_reverse)
        ASSERT(for_stmt["reverse"], "reverse must be true.");
    
    UDF_Type tmp_int_type(for_var_type, udf_str);
    string step_size = "1";
    duckdb::CodeInsertionPoint insert(function_info->string_function_count);
    if(for_stmt.contains("step")){
        step_size = translate_expr(for_stmt["step"]["PLpgSQL_expr"], insert, &tmp_int_type, false);
    }
    // todo: uncomment when trans_expr is done
    // ASSERT(stoi(step_size) > 0, "Step size should be positive.");
    string start = translate_expr(for_stmt["lower"]["PLpgSQL_expr"], insert, &tmp_int_type, false);
    string end = translate_expr(for_stmt["upper"]["PLpgSQL_expr"], insert, &tmp_int_type, false);
    string output = insert.to_string() + fmt::format(fmt::runtime(config->control[is_reverse ? "revfor" : "for"].Scalar()), \
                                                    fmt::arg("body", translate_body(for_stmt["body"])), \
                                                    fmt::arg("start", start), \
                                                    fmt::arg("end", end), \
                                                    fmt::arg("name", tmp_var_name), \
                                                    fmt::arg("step", step_size));
    // clean all temporary variables                       
    function_info->func_vars.erase(tmp_var_name);
    if(prev_sub.compare("") == 0){
        function_info->tmp_var_substitutes.erase(for_var_name);
    }
    else{
        function_info->tmp_var_substitutes[for_var_name] = prev_sub;
    }
    return output + "\n";
}

string Transpiler::translate_while_stmt(json &while_stmt){
    string body = translate_body(while_stmt["body"]);
    duckdb::CodeInsertionPoint insert(function_info->string_function_count);
    string condition = translate_cond_stmt(while_stmt["cond"], insert);
    return insert.to_string() + fmt::format(fmt::runtime(config->control["while"].Scalar()), \
                                            fmt::arg("body", body), \
                                            fmt::arg("condition", condition));
}

string Transpiler::translate_exitcont_stmt(json &stmt){
    if(stmt.contains("is_exit")){
        ASSERT(stmt["is_exit"], "if_exit should be true.");
        return "break;\n";
    }
    return "continue;\n";
}

// todo
/**
 * Transpile a body from PL/pgSQL to C++.
 * This function might be called recursively inside trans_*. If there is called
 * by return type determined stmts like "cond":{"PLpgSQL_expr"..., should set 
 * argument expected_type.
*/
string Transpiler::translate_body(json &body, UDF_Type *expected_type){
    string output = "";
    for(auto &stmt : body){
        if (stmt.contains("PLpgSQL_stmt_if"))
            output += translate_if_stmt(stmt["PLpgSQL_stmt_if"]);
        else if (stmt.contains("PLpgSQL_stmt_return"))
            output += translate_return_stmt(stmt["PLpgSQL_stmt_return"]);
        // else if (stmt.contains("PLpgSQL_expr"))
        //     output += translate_expr(stmt["PLpgSQL_expr"], \
        //                              expected_type, false);
        else if(stmt.contains("PLpgSQL_stmt_assign"))
            output += translate_assign_stmt(stmt["PLpgSQL_stmt_assign"]);
        else if(stmt.contains("PLpgSQL_stmt_loop"))
            output += translate_loop_stmt(stmt["PLpgSQL_stmt_loop"]);
        else if(stmt.contains("PLpgSQL_stmt_fori"))
            output += translate_for_stmt(stmt["PLpgSQL_stmt_fori"]);
        else if(stmt.contains("PLpgSQL_stmt_while"))
            output += translate_while_stmt(stmt["PLpgSQL_stmt_while"]);
        else if(stmt.contains("PLpgSQL_stmt_exit"))
            output += translate_exitcont_stmt(stmt["PLpgSQL_stmt_exit"]);
        else
            ERROR(fmt::format("Unknown statement type: {}", stmt));
    }
    return output;
}

string Transpiler::translate_stmt_block(json &stmt_block){
    UDF_Type tmp;
    return translate_body(stmt_block["body"], NULL);
}

string Transpiler::translate_action(json &action){
    return translate_stmt_block(action["PLpgSQL_stmt_block"]);
}

// todo
/**
 * Get the function arguments from the datums. Datums contain arguments (before found) and local
 * variables (after found). 
*/
std::string Transpiler::get_function_vars(json &datums, string &udf_str){
    std::string vars_init;
    ASSERT(datums.is_array(), "Datums is not an array.");
    bool scanning_func_args = true;
    for(size_t i=0;i<datums.size();i++){
        auto &datum = datums[i];
        ASSERT(datum.contains("PLpgSQL_var"), "Datum does not contain PLpgSQL_var.");
        auto var = datum["PLpgSQL_var"];
        string name = var["refname"];
        if(strcmp(name.c_str(), "found") == 0){
            scanning_func_args = false;
            continue;
        } 
        string typ = var["datatype"]["PLpgSQL_type"]["typname"];
        VarInfo var_info(i, typ, udf_str, false);
        if(var_info.type.is_unknown()) continue;    // variables with UNKNOWN types are created by for loops later in the code
                                                    // We don't need to do anything with them here
        if(scanning_func_args){
            var_info.init = true;
            function_info->func_args[name] = var_info;
            function_info->func_args_vec.push_back(name);
            string extract_data_from_value = fmt::format("v{}.GetValueUnsafe<{}>()", i, var_info.type.get_cpp_type());
            vars_init += fmt::format("{} {} = {};\n", var_info.type.get_cpp_type(), name, extract_data_from_value);
        }
        else{
            if(var.contains("default_val")){
                duckdb::CodeInsertionPoint insert(function_info->string_function_count);
                string query_result = translate_expr(var["default_val"]["PLpgSQL_expr"], insert, &var_info.type, false);
                vars_init += insert.to_string() + fmt::format("{} {} = {};\n", var_info.type.get_cpp_type(), name, query_result);
                var_info.init = true;
                function_info->func_vars[name] = var_info;
            }   
            else{
                function_info->func_vars[name] = var_info;
                vars_init += fmt::format("{} {};\n", var_info.type.get_cpp_type(), name);
            }     
        }                                    
    }
    return vars_init;
}

/**
 * return of this function is not used
*/
vector<string> Transpiler::translate_function(json &ast, string &udf_str){
    string vars_init = get_function_vars(ast["datums"], udf_str);
    // for(auto i : function_info->func_args){
    //     std::cout<<i.first<<i.second.type.duckdb_type<<i.second.init<<std::endl;
    // }
    string function_args = "";
    string arg_indexes = "";
    string subfunc_args = "";
    string fbody_args = "";
    vector<string> check_null;
    auto actual_body = translate_action(ast["action"]);
    // int count = 0;
    for(auto &name : function_info->func_args_vec){
        function_args += fmt::format(fmt::runtime(config->function["fargs2"].Scalar()), \
                                    fmt::arg("var_name", name), \
                                    fmt::arg("i", function_info->func_args[name].id));
        function_args += "\n";                            
        arg_indexes += fmt::format(fmt::runtime(config->function["argindex"].Scalar()), \
                                    fmt::arg("var_name", name));    
        arg_indexes += "\n";     
        subfunc_args += fmt::format(fmt::runtime(config->function["subfunc_arg"].Scalar()), \
                                    fmt::arg("var_name", name));    
        subfunc_args += ", "; 
        fbody_args += fmt::format(fmt::runtime(config->function["fbody_arg"].Scalar()), \
                                    fmt::arg("i", function_info->func_args[name].id), \
                                    fmt::arg("var_name", name));    
        fbody_args += ", "; 
        // count++;
        check_null.push_back(name + "_null");
    }

    string vector_create = "";
    if(function_info->string_function_count > 0){
        vector_create = fmt::format(fmt::runtime(config->function["vector_create"].Scalar()), \
                                    fmt::arg("vector_count", function_info->string_function_count));
        for(int i=0;i<function_info->string_function_count;i++){
            subfunc_args += fmt::format("tmp_chunk.data[{}], ", i);
            fbody_args += fmt::format("Vector &tmp_vec{}, ", i);
        }
    }

    cc.global_functions.push_back(fmt::format(fmt::runtime(config->function["fbodyshell"].Scalar()), \
                                                fmt::arg("function_name", function_info->func_name), \
                                                fmt::arg("fbody_args", fbody_args), \
                                                fmt::arg("check_null", vec_join(check_null, " or ")), \
                                                fmt::arg("vars_init", vars_init), \
                                                fmt::arg("action", actual_body)));

    cc.global_functions.push_back(fmt::format(fmt::runtime(config->function["fshell2"].Scalar()), \
                                            fmt::arg("function_name", function_info->func_name), \
                                            fmt::arg("function_args", function_args), \
                                            fmt::arg("arg_indexes", arg_indexes), \
                                            fmt::arg("vector_create", vector_create), \
                                            fmt::arg("subfunc_args", subfunc_args)));

    // string decl = fmt::format(fmt::runtime(config->function["func_dec"].Scalar()), \
    //                                         fmt::arg("function_name", function_info->func_name), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));

    // string fcreate = fmt::format(fmt::runtime(config->function["fcreate"].Scalar()), \
    //                                         fmt::arg("duck_ret_type", function_info->func_return_type.get_duckdb_type()), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));                                        
    vector<string> ret;
    return ret;                                   
}

/**
 * @brief transpile a PL/PgSQL function definition to the C++ implementation
 * @return a vector of three strings: the udf file; udf register; udf declaration
*/
vector<string> Transpiler::run(){
    // collect the function return types
    std::vector<std::string> return_types;
    std::regex return_pattern("RETURNS\\s+(\\w+(\\(\\d+, ?\\d+\\))?)", std::regex_constants::icase);
    std::smatch tmp_types;
    std::string tmp_string = udf_str;
    while(std::regex_search(tmp_string, tmp_types, return_pattern)){
        return_types.push_back(tmp_types[1]);
        tmp_string = tmp_types.suffix();
    }
    // for (size_t i = 0; i < return_types.size(); ++i) 
    //             std::cout << i << ": " << return_types[i] << '\n';
    // collect the function names
    std::vector<std::string> func_names;
    std::regex name_pattern("CREATE\\s+FUNCTION\\s+(\\w+)", std::regex_constants::icase);
    std::smatch tmp_names;
    tmp_string = udf_str;
    // std::cout << tmp_string << '\n';
    while(std::regex_search(tmp_string, tmp_names, name_pattern)){
        func_names.push_back(tmp_names[1]);
        tmp_string = tmp_names.suffix();
    }
    // for (size_t i = 0; i < func_names.size(); ++i) 
    //             std::cout << i << ": " << func_names[i] << '\n';

    PgQueryPlpgsqlParseResult result;
    result = pg_query_parse_plpgsql(udf_str.c_str());
    

    if (result.error)
    {
        printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
        ERROR(fmt::format("Error when parsing the plpgsql: {}", result.error->message));
    }
    // printf("%s\n", result.plpgsql_funcs);
    json ast = json::parse(result.plpgsql_funcs);
    ASSERT(return_types.size() >= ast.size(), "Return type not specified for all functions");
    ASSERT(func_names.size() >= ast.size(), "Function name not specified for all functions");
    // cc = std::make_shared<CodeContainer>();
    // std::string code = fmt::format(fmt::runtime(config->query["macro2"].Scalar()),\
    //                                             fmt::arg("db_name", "db"),\
    //                                             fmt::arg("vector_size", 2048));
    // cc.global_macros.push_back(code);
    cc.query_macro = true;
    for(size_t i=0;i<ast.size();i++){
        if(ast[i].contains("PLpgSQL_function")){
            function_info = std::make_unique<FunctionInfo>();
            function_info->func_name = func_names[i];
            function_info->func_return_type = UDF_Type(return_types[i], udf_str);
            vector<string> tmp_ret = translate_function(ast[i]["PLpgSQL_function"], udf_str);
            // std::cout<<function_info->func_return_type.duckdb_type<<std::endl;

        }
    }
    pg_query_free_plpgsql_parse_result(result);
    // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind) 
    // (should not be used here since this is a recurring call)
    // pg_query_exit();
    vector<string> ret;
    // ret.push_back("This is the udf C++.");
    ret.push_back(vec_join(cc.global_macros, "\n")+"\n"+vec_join(cc.global_functions, "\n"));
    ret.push_back("This is the udf register.");
    ret.push_back("This is the declaration.");
    return ret;
}