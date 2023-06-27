#include "trans.hpp"

vector<string> transpile_plpgsql_udf_str(string &&udf_str){
    PgQueryPlpgsqlParseResult result;
//     string sql = "\
// CREATE FUNCTION retrieve_parents(cid integer) RETURNS text AS $$\
// DECLARE pd text;    \
// BEGIN\
//     pd = 'function';\
//     RETURN concat(cid,pd);\
// END; $$\
// LANGUAGE PLPGSQL";
    // printf("%s", sql);

    result = pg_query_parse_plpgsql(udf_str.c_str());
    if (result.error)
    {
        printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    }
    else
    {
        printf("%s\n", result.plpgsql_funcs);
        // printf("%s\n", result.parse_tree);
    }

    pg_query_free_plpgsql_parse_result(result);
    // pg_query_free_parse_result(result);

    // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind)
    pg_query_exit();

    YAML::Node config = YAML::LoadFile("../control.yaml");

    
    vector<string> ret;
    ret.push_back("This is the udf C++.");
    ret.push_back("This is the udf register.");
    ret.push_back("This is the declaration.");
    return ret;
}