#include "trans.hpp"
using json = nlohmann::json;

// todo
string Transpiler::translate_expr(json &expr, UDF_Type &expected_type, bool query_is_assignment = false){
    return "";
}

// todo
string Transpiler::translate_action(json &action){
    return "";
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
            function_info->func_args[name] = var_info;
        }
        else{
            if(var.contains("default_val")){
                string query_result = translate_expr(var["default_val"]["PLpgSQL_expr"], var_info.type, false);
                vars_init += fmt::format("{} {} = {};\n", var_info.type.get_cpp_type(), name, query_result);
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
    // int count = 0;
    for(auto &pair : function_info->func_args){
        function_args += fmt::format(fmt::runtime(config->function["fargs2"].Scalar()), \
                                    fmt::arg("var_name", pair.first), \
                                    fmt::arg("i", pair.second.id));
        function_args += "\n";                            
        arg_indexes += fmt::format(fmt::runtime(config->function["argindex"].Scalar()), \
                                    fmt::arg("var_name", pair.first));    
        arg_indexes += "\n";     
        subfunc_args += fmt::format(fmt::runtime(config->function["subfunc_arg"].Scalar()), \
                                    fmt::arg("var_name", pair.first));    
        subfunc_args += ", "; 
        fbody_args += fmt::format(fmt::runtime(config->function["fbody_arg"].Scalar()), \
                                    fmt::arg("i", pair.second.id), \
                                    fmt::arg("var_name", pair.first));    
        fbody_args += ", "; 
        // count++;
        check_null.push_back(pair.first + "_null");
    }
    string output = fmt::format(fmt::runtime(config->function["fshell2"].Scalar()), \
                                            fmt::arg("function_name", function_info->func_name), \
                                            fmt::arg("function_args", function_args), \
                                            fmt::arg("arg_indexes", arg_indexes), \
                                            fmt::arg("subfunc_args", subfunc_args));
    std::cout<<output<<std::endl;
    
    cc.global_functions.push_back(fmt::format(fmt::runtime(config->function["fbodyshell"].Scalar()), \
                                                fmt::arg("function_name", function_info->func_name), \
                                                fmt::arg("fbody_args", fbody_args), \
                                                fmt::arg("check_null", vec_join(check_null, " or ")), \
                                                fmt::arg("vars_init", vars_init)));
    // string decl = fmt::format(fmt::runtime(config->function["func_dec"].Scalar()), \
    //                                         fmt::arg("function_name", function_info->func_name), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));

    // string fcreate = fmt::format(fmt::runtime(config->function["fcreate"].Scalar()), \
    //                                         fmt::arg("duck_ret_type", function_info->func_return_type.get_duckdb_type()), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));                                        
    vector<string> ret;
    ret.push_back(output);
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
        throw std::runtime_error(fmt::format("Error when parsing the plpgsql: {}", result.error->message));
    }
    printf("%s\n", result.plpgsql_funcs);
    json ast = json::parse(result.plpgsql_funcs);
    ASSERT(return_types.size() >= ast.size(), "Return type not specified for all functions");
    ASSERT(func_names.size() >= ast.size(), "Function name not specified for all functions");
    // cc = std::make_shared<CodeContainer>();
    std::string code = fmt::format(fmt::runtime(config->query["macro2"].Scalar()),\
                                                fmt::arg("db_name", "db"),\
                                                fmt::arg("vector_size", 2048));
    cc.global_macros.push_back(code);
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
    // pg_query_free_parse_result(result);
    // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind)
    pg_query_exit();
    vector<string> ret;
    // ret.push_back("This is the udf C++.");
    ret.push_back(vec_join(cc.global_macros, "\n")+"\n"+vec_join(cc.global_functions, "\n"));
    ret.push_back("This is the udf register.");
    ret.push_back("This is the declaration.");
    return ret;
}