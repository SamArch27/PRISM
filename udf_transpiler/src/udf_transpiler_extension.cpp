#define DUCKDB_EXTENSION_MAIN

#include "udf_transpiler_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/binder.hpp"
// #include "logical_operator_code_generator.hpp"
#include "cfg_code_generator.hpp"
#include <fstream>
#include <iostream>
#include <stdio.h>
// #include <unistd.h>
#include "file.hpp"
#include "trans.hpp"
#include "utils.hpp"
#include "compiler.hpp"
#include <filesystem>

namespace duckdb {
duckdb::DuckDB *db_instance;
int udf_count = 0;

inline string Udf_transpilerPragmaFun(ClientContext &context,
                                      const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<string>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    std::string err = "Input file is empty or does not exist: " + file_name;
    return "select '" + err + "' as 'Transpilation Failed.';";
  }
  YAMLConfig config;
  Connection con(*db_instance);

  // Transpiler transpiler(buffer.str(), &config, con);
  // std::vector<std::string> ret = transpiler.run();
  std::string code, registration;
  auto compiler = Compiler(&con, buffer.str());
  compiler.run(code, registration);
  udf_count++;
  std::cout << "Transpiling the UDF..." << std::endl;
  insert_def_and_reg(code, registration, udf_count);
  // compile the template
  std::cout << "Compiling the UDF..." << std::endl;
  compile_udf();
  // load the compiled library
  cout << "Installing and loading the UDF..." << endl;
  load_udf(con);
  return "select '' as 'Transpilation Done.';";
}

/**
 * transpile udfs from a file but not link it
 */
inline string Udf_CodeGeneratorPragmaFun(ClientContext &context,
                                         const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<string>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    std::string err = "Input file is empty or does not exist: " + file_name;
    return "select '" + err + "' as 'Transpilation Failed.';";
  }
  YAMLConfig config;
  Connection con(*db_instance);
  std::string code, registration;
  auto compiler = Compiler(&con, buffer.str());
  compiler.run(code, registration);
  udf_count++;
  cout << "Transpiling the UDF..." << endl;
  insert_def_and_reg(code, registration, udf_count);
  return "select '' as 'Code Generation Done.';";
}

/**
 * rebuild and load the last udf set
 */
inline string Udf_BuilderPragmaFun(ClientContext &context,
                                   const FunctionParameters &parameters) {
  cout << "Compiling the UDF..." << endl;
  compile_udf();
  // load the compiled library
  cout << "Installing and loading the UDF..." << endl;
  Connection con(*db_instance);
  load_udf(con);
  return "select '' as 'Building and linking Done.';";
}

inline string Udaf_BuilderPragmaFun(ClientContext &context,
                                    const FunctionParameters &parameters) {
  cout << "Compiling the UDAF..." << endl;
  compile_udaf();
  // load the compiled library
  cout << "Installing and loading the UDF..." << endl;
  Connection con(*db_instance);
  load_udaf(con);
  return "select '' as 'Building and linking Done.';";
}

static void LoadInternal(DatabaseInstance &instance) {
  // auto udf_transpiler_scalar_function = ScalarFunction("udf_transpiler",
  // {LogicalType::VARCHAR},
  // LogicalType::VARCHAR, Udf_transpilerScalarFun);
  // ExtensionUtil::RegisterFunction(instance, udf_transpiler_scalar_function);

  auto udf_transpiler_pragma_function = PragmaFunction::PragmaCall(
      "transpile", Udf_transpilerPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, udf_transpiler_pragma_function);
  auto udf_codegen_pragma_function = PragmaFunction::PragmaCall(
      "codegen", Udf_CodeGeneratorPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, udf_codegen_pragma_function);
  auto udf_builder_pragma_function =
      PragmaFunction::PragmaCall("build", Udf_BuilderPragmaFun, {});
  ExtensionUtil::RegisterFunction(instance, udf_builder_pragma_function);
  auto udaf_builder_pragma_function =
      PragmaFunction::PragmaCall("build_agg", Udaf_BuilderPragmaFun, {});
  ExtensionUtil::RegisterFunction(instance, udaf_builder_pragma_function);
  // auto itos_scalar_function = ScalarFunction("itos", {LogicalType::INTEGER},
  // 													 LogicalType::VARCHAR,
  // itos); ExtensionUtil::RegisterFunction(instance, itos_scalar_function);
}

void UdfTranspilerExtension::Load(DuckDB &db) {
  this->db = &db;
  db_instance = &db;
  LoadInternal(*db.instance);
}
std::string UdfTranspilerExtension::Name() { return "udf_transpiler"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void udf_transpiler_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *udf_transpiler_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
