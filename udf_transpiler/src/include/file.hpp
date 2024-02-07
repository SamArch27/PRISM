#include "duckdb.hpp"
#include <filesystem>
#include <fstream>
#include <string>
#include "utils.hpp"

#define UDF_EXTENSION_TEMPLATE_DIR "../udf_template/"
#define UDF_EXTENSION_OUTPUT_DIR "../udf1/"
#define UDAF_EXTENSION_OUTPUT_DIR "../udf_agg/"

void insertDefAndReg(const String &defs, const String &regs,
                        int udfCount);
void compileUDF();
void loadUDF(duckdb::Connection &connection);
void compileUDAF();
void loadUDAF(duckdb::Connection &connection);