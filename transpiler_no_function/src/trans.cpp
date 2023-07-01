#include "trans.hpp"
using json = nlohmann::json;

std::shared_ptr<CodeContainer> cc;
std::shared_ptr<GV> gv;
YAML::Node query_config = YAML::LoadFile("../query.yaml");
YAML::Node function_config = YAML::LoadFile("../function.yaml");

// todo
string translate_expr(json &expr, UDF_Type &expected_type, bool query_is_assignment = false){
    return "";
}

// todo
string translate_action(json &action){
    return "";
}

// todo
/**
 * Get the function arguments from the datums.
 * Returns a tuple with a list of the formatted function arguments, and a
 * list of initializations/declarations for function variables.
*/
void get_function_vars(json &datums, string &udf_str){
    vector<string> initializations;
    ASSERT(datums.is_array(), "Datums is not an array.");
    for(int i=0;i<datums.size();i++){
        auto &datum = datums[i];
        ASSERT(datum.contains("PLpgSQL_var"), "Datum does not contain PLpgSQL_var.");
        auto var = datum["PLpgSQL_var"];
        string name = var["refname"];
        if(strcmp(name.c_str(), "found") == 0) continue;
        string typ = var["datatype"]["PLpgSQL_type"]["typname"];
        VarInfo var_info(i, typ, udf_str, false);
        if(var_info.type.is_unknown()) continue;    // variables with UNKNOWN types are created by for loops later in the code
                                                    // We don't need to do anything with them here
        if(var.contains("default_val")){
            string query_result = translate_expr(var["default_val"]["PLpgSQL_expr"], var_info.type, false);
            // todo initialization
            var_info.init = true;
            gv->func_args[name] = var_info;
        }   
        else{
            // todo initialization
            gv->func_args[name] = var_info;
        }                                         
    }
    return;
}

vector<string> translate_function(json &ast, string &udf_str){
    get_function_vars(ast["datums"], udf_str);
    // for(auto i : gv->func_args){
    //     std::cout<<i.first<<i.second.type.duckdb_type<<i.second.init<<std::endl;
    // }
    string function_args = "";
    string arg_indexes = "";
    string subfunc_args = "";
    string fbody_args = "";
    vector<string> check_null;
    // int count = 0;
    for(auto &pair : gv->func_args){
        function_args += fmt::format(fmt::runtime(function_config["fargs2"].Scalar()), \
                                    fmt::arg("var_name", pair.first), \
                                    fmt::arg("i", pair.second.id));
        function_args += "\n";                            
        arg_indexes += fmt::format(fmt::runtime(function_config["argindex"].Scalar()), \
                                    fmt::arg("var_name", pair.first));    
        arg_indexes += "\n";     
        subfunc_args += fmt::format(fmt::runtime(function_config["subfunc_arg"].Scalar()), \
                                    fmt::arg("var_name", pair.first));    
        subfunc_args += ", "; 
        fbody_args += fmt::format(fmt::runtime(function_config["fbody_arg"].Scalar()), \
                                    fmt::arg("i", pair.second.id), \
                                    fmt::arg("var_name", pair.first));    
        fbody_args += ", "; 
        // count++;
        check_null.push_back(pair.first + "_null");
    }
    string output = fmt::format(fmt::runtime(function_config["fshell2"].Scalar()), \
                                            fmt::arg("function_name", gv->func_name), \
                                            fmt::arg("function_args", function_args), \
                                            fmt::arg("arg_indexes", arg_indexes), \
                                            fmt::arg("subfunc_args", subfunc_args));
    std::cout<<output<<std::endl;
    
    cc->global_functions.push_back(fmt::format(fmt::runtime(function_config["fbodyshell"].Scalar()), \
                                                fmt::arg("function_name", gv->func_name), \
                                                fmt::arg("fbody_args", fbody_args), \
                                                fmt::arg("check_null", vec_join(check_null, " | "))));
    // string decl = fmt::format(fmt::runtime(function_config["func_dec"].Scalar()), \
    //                                         fmt::arg("function_name", gv->func_name), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));

    // string fcreate = fmt::format(fmt::runtime(function_config["fcreate"].Scalar()), \
    //                                         fmt::arg("duck_ret_type", gv->func_return_type.get_duckdb_type()), \
    //                                         fmt::arg("function_args", args_str), \
    //                                         fmt::arg("initializations", ""));                                        
    vector<string> ret;
    ret.push_back(output);
    return ret;                                   
}

/**
 * @brief transpile a PL/PgSQL function definition to the C++ implementation
 * @param udf_str a plpgsql string
 * @return a vector of three strings: the udf file; udf register; udf declaration
*/
vector<string> transpile_plpgsql_udf_str(string &&udf_str){
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
    cc = std::make_shared<CodeContainer>();
    std::string code = fmt::format(fmt::runtime(query_config["macro2"].Scalar()),\
                                                fmt::arg("db_name", "db"),\
                                                fmt::arg("vector_size", 2048));
    cc->global_macros.push_back(code);
    cc->query_macro = true;
    for(int i=0;i<ast.size();i++){
        if(ast[i].contains("PLpgSQL_function")){
            gv = std::make_shared<GV>();
            gv->func_name = func_names[i];
            gv->func_return_type = UDF_Type(return_types[i], udf_str);
            vector<string> tmp_ret = translate_function(ast[i]["PLpgSQL_function"], udf_str);
            // std::cout<<gv->func_return_type.duckdb_type<<std::endl;

        }
    }
    pg_query_free_plpgsql_parse_result(result);
    // pg_query_free_parse_result(result);
    // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind)
    pg_query_exit();
    vector<string> ret;
    // ret.push_back("This is the udf C++.");
    ret.push_back(vec_join(cc->global_macros, "\n")+"\n"+vec_join(cc->global_functions, "\n"));
    ret.push_back("This is the udf register.");
    ret.push_back("This is the declaration.");
    return ret;
}