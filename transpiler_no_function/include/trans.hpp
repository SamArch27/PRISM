#ifndef TRANS
#define TRANS
#include <vector>
#include "pg_query.h"
#include <fmt/core.h>
#include <yaml-cpp/yaml.h>
using namespace std;

vector<string> transpile_plpgsql_udf_str(string &&udf_str);

#endif