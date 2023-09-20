#include <string>
#include <filesystem>
#include <fstream>
#include "duckdb.hpp"

void insert_def_and_reg(const std::string & defs, const std::string & regs, int udf_count);
void compile_udf(int udf_count);
void load_udf(duckdb::Connection &connection, int udf_count);