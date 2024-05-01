#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  PgQueryPlpgsqlParseResult result;

//   result = pg_query_parse_plpgsql(" \
//   CREATE OR REPLACE FUNCTION cs_fmt_browser_version(v_name varchar, \
//                                                   v_version varchar) \
// RETURNS varchar AS $$ \
// BEGIN \
//     IF v_version IS NULL THEN \
//         RETURN v_name; \
//     END IF; \
//     RETURN v_name || '/' || v_version; \
// END; \
// $$ LANGUAGE plpgsql;");

  char *sql = "\
  create function q3conditions(cmkt int, odate date, shipdate date)\
  returns int as $$ \
  declare thedate varchar(30) := '1995-03-15'; begin\
  if odate < 'asd' then return 2; end if;\
  return thedata; end; \
  $$ LANGUAGE plpgsql;";
  printf("%s", sql);

  result = pg_query_parse_plpgsql(sql);
  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
  } else {
    printf("%s\n", result.plpgsql_funcs);
  }

  pg_query_free_plpgsql_parse_result(result);

  // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind)
  pg_query_exit();

  return 0;
}
