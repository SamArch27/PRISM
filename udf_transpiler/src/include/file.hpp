#include <string>
#include <filesystem>
#include <fstream>
#include "duckdb.hpp"

#define UDF_EXTENSION_TEMPLATE_DIR "../udf_template/"
#define UDF_EXTENSION_OUTPUT_DIR "../udf1/"
#define UDAF_EXTENSION_OUTPUT_DIR "../udf_agg/"

void insert_def_and_reg(const std::string & defs, const std::string & regs, int udf_count);
void compile_udf(int udf_count);
void load_udf(duckdb::Connection &connection, int udf_count);
void compile_udaf(int udf_count);
void load_udaf(duckdb::Connection &connection, int udf_count);