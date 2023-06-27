#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <ctype.h>
#include <stdio.h>
// #include "duckdb.hpp"
#include <unistd.h>
#include "pg_query.h"

using namespace std;

const string HELP_MES =
    "Allowed options:\n\
-h print this helper message;\n\
-f output to a file;\n\
-d output to a directory together with a test file;\n";

int main(int argc, char const *argv[])
{
    int aflag = 0;
    int bflag = 0;
    char *cvalue = NULL;
    int index;
    int c;

    opterr = 0;

    while ((c = getopt(argc, (char *const *)argv, "hf:d:")) != -1)
        switch (c)
        {
        case 'h':
            std::cout << HELP_MES << std::endl;
            break;
        case 'f':
            bflag = 1;
            break;
        case 'd':
            cvalue = optarg;
            break;
        default:
            abort();
        }

    printf("aflag = %d, bflag = %d, cvalue = %s\n",
           aflag, bflag, cvalue);
    PgQueryPlpgsqlParseResult result;
    string sql = "\
CREATE FUNCTION retrieve_parents(cid integer) RETURNS text AS $$\
DECLARE pd text;    \
BEGIN\
    pd = 'function';\
    RETURN concat(cid,pd);\
END; $$\
LANGUAGE PLPGSQL";
    // printf("%s", sql);

    result = pg_query_parse_plpgsql(sql.c_str());
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
    for (index = optind; index < argc; index++)
        printf("Non-option argument %s\n", argv[index]);
    return 0;
}
