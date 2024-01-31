#define DUCKDB_EXTENSION_MAIN

#include "udf_transpiler_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
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
#include "compiler.hpp"
#include "file.hpp"
#include "utils.hpp"
#include <filesystem>

namespace duckdb {
duckdb::DuckDB *db_instance;
size_t udf_count = 0;

inline String Udf_transpilerPragmaFun(ClientContext &context,
                                      const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<String>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    String err = "Input file is empty or does not exist: " + file_name;
    return "select '" + err + "' as 'Transpilation Failed.';";
  }
  YAMLConfig config;
  Connection con(*db_instance);

  // Transpiler transpiler(buffer.str(), &config, con);
  // Vec<String> ret = transpiler.run();
  // String code, registration;
  auto compiler = Compiler(&con, buffer.str(), udf_count);
  auto res = compiler.run();
  udf_count++;
  std::cout << "Transpiling the UDF..." << std::endl;
  insert_def_and_reg(res.code, res.registration, udf_count);
  // compile the template
  std::cout << "Compiling the UDF..." << std::endl;
  compile_udf();
  // load the compiled library
  std::cout << "Installing and loading the UDF..." << std::endl;
  load_udf(con);
  return "select '' as 'Transpilation Done.';";
}

/**
 * transpile udfs from a file but not link it
 */
inline String Udf_CodeGeneratorPragmaFun(ClientContext &context,
                                         const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<String>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    String err = "Input file is empty or does not exist: " + file_name;
    return "select '" + err + "' as 'Transpilation Failed.';";
  }
  YAMLConfig config;
  Connection con(*db_instance);
  String code, registration;
  auto compiler = Compiler(&con, buffer.str(), udf_count);
  auto res = compiler.run();
  udf_count++;
  std::cout << "Transpiling the UDF..." << std::endl;
  insert_def_and_reg(res.code, res.registration, udf_count);
  return "select '' as 'Code Generation Done.';";
}

/**
 * rebuild and load the last udf set
 */
inline String Udf_BuilderPragmaFun(ClientContext &context,
                                   const FunctionParameters &parameters) {
  std::cout << "Compiling the UDF..." << std::endl;
  compile_udf();
  // load the compiled library
  std::cout << "Installing and loading the UDF..." << std::endl;
  Connection con(*db_instance);
  load_udf(con);
  return "select '' as 'Building and linking Done.';";
}

inline String Udaf_BuilderPragmaFun(ClientContext &context,
                                    const FunctionParameters &parameters) {
  std::cout << "Compiling the UDAF..." << std::endl;
  compile_udaf();
  // load the compiled library
  std::cout << "Installing and loading the UDF..." << std::endl;
  Connection con(*db_instance);
  load_udaf(con);
  return "select '' as 'Building and linking Done.';";
}

inline String LOCodeGenPragmaFun(ClientContext &_context,
                                 const FunctionParameters &parameters) {
  auto sql = parameters.values[0].GetValue<String>();
  auto enable_optimizer = parameters.values[1].GetValue<bool>();
  LogicalOperatorCodeGenerator locg;
  // CodeGenInfo insert;
  Connection con(*db_instance);
  auto context = con.context.get();
  // bool mem = context->config.enable_optimizer;
  context->config.enable_optimizer = enable_optimizer;
  // if(enable_optimizer) context->config.disableStatisticsPropagation = true;
  auto &config = DBConfig::GetConfig(*context);
  std::set<OptimizerType> disable_optimizers_should_delete;

  if (enable_optimizer) {
    if (config.options.disabled_optimizers.count(
            OptimizerType::STATISTICS_PROPAGATION) == 0)
      disable_optimizers_should_delete.insert(
          OptimizerType::STATISTICS_PROPAGATION);
    config.options.disabled_optimizers.insert(
        OptimizerType::STATISTICS_PROPAGATION);
  }

  try {
    auto plan = context->ExtractPlan(sql);
    locg.VisitOperator(*plan);
    for (auto &type : disable_optimizers_should_delete) {
      config.options.disabled_optimizers.erase(type);
    }
  } catch (Exception &e) {
    for (auto &type : disable_optimizers_should_delete) {
      config.options.disabled_optimizers.erase(type);
    }
    EXCEPTION(e.what());
    return "select '" + e.GetStackTrace(5) +
           "' as 'Partial Evaluation Failed.';";
  }
  // context->config.enable_optimizer = mem;
  std::cout << locg.getResult().first << std::endl;
  std::cout << std::endl;
  std::cout << locg.getResult().second << std::endl;
  return "select '' as 'Partial Evaluation Done.';";
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
  auto lo_codegen_pragma_function =
      PragmaFunction::PragmaCall("partial", LOCodeGenPragmaFun,
                                 {LogicalType::VARCHAR, LogicalType::INTEGER});
  ExtensionUtil::RegisterFunction(instance, lo_codegen_pragma_function);
  // auto itos_scalar_function = ScalarFunction("itos", {LogicalType::INTEGER},
  // 													 LogicalType::VARCHAR,
  // itos); ExtensionUtil::RegisterFunction(instance, itos_scalar_function);
}

void UdfTranspilerExtension::Load(DuckDB &db) {
  this->db = &db;
  db_instance = &db;
  LoadInternal(*db.instance);
}
String UdfTranspilerExtension::Name() { return "udf_transpiler"; }

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
