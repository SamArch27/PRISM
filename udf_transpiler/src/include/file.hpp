#include <string>
#include <filesystem>
#include <fstream>
#include "duckdb.hpp"

void insert_def_and_reg(const std::string & defs, const std::string & regs);
void compile_udf();
void load_udf(duckdb::Connection &connection);