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
size_t udfCount = 0;
Map<String, bool> optimizerPassOnMap = {
    {"SSAConstruction", true},
    {"SSADestruction", true},
    {"DeadCodeElimination", true},
    {"QueryMotion", true},
    {"BreakPhiInterference", true},
    {"MergeRegions", true},
    {"AggressiveMergeRegions", true},
    {"ExpressionPropagation", true},
    {"AggressiveExpressionPropagation", true},
    {"OutliningPass", true}};

// replace every single quote with two single quotes
static String doubleQuote(const String &str) {
  String result;
  for (auto &c : str) {
    if (c == '\'') {
      result += '\'';
    }
    result += c;
  }
  return result;
}

static String CompilerRun(String udfString) {
  YAMLConfig config;
  Connection con(*db_instance);

  udfCount++;
  auto compiler =
      Compiler(optimizerPassOnMap, &con, udfString, config, udfCount);
  auto res = compiler.run();
  return "select '' as 'Transpilation Done.';";
}

inline String ListCompilerPassPragmaFun(ClientContext &context,
                                        const FunctionParameters &parameters) {
  String result;
  for (auto &pass : optimizerPassOnMap) {
    result +=
        fmt::format("('{}', {}),", pass.first, pass.second ? "true" : "false");
  }
  result = result.substr(0, result.size() - 1);
  return fmt::format("select col0 as Pass, col1 as \"Enabled?\" from (values "
                     "{}) order by Pass;",
                     result);
}

inline String
EnableCompilerPassPragmaFun(ClientContext &context,
                            const FunctionParameters &parameters) {
  auto passName = parameters.values[0].GetValue<String>();
  if (optimizerPassOnMap.count(passName) == 0) {
    String err = "Invalid compiler pass: " + doubleQuote(passName);
    return "select '" + err + "' as 'Enable Failed.';";
  }

  optimizerPassOnMap[passName] = true;
  return "select '' as 'Enable Done.';";
}

inline String
DisableCompilerPassPragmaFun(ClientContext &context,
                             const FunctionParameters &parameters) {
  auto passName = parameters.values[0].GetValue<String>();
  if (optimizerPassOnMap.count(passName) == 0) {
    String err = "Invalid compiler pass: " + doubleQuote(passName);
    return "select '" + err + "' as 'Disable Failed.';";
  }

  optimizerPassOnMap[passName] = false;
  return "select '' as 'Disable Done.';";
}

inline String UdfTranspilerPragmaFun(ClientContext &context,
                                     const FunctionParameters &parameters) {
  auto udfString = parameters.values[0].GetValue<String>();

  return CompilerRun(udfString);
}

inline String UdfFileTranspilerPragmaFun(ClientContext &context,
                                         const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<String>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    String err =
        "Input file is empty or does not exist: " + doubleQuote(file_name);
    return "select '" + err + "' as 'Transpilation Failed.';";
  }

  return CompilerRun(buffer.str());
}

/**
 * transpile udfs from a file but not link it
 */
inline String UdfCodeGeneratorPragmaFun(ClientContext &context,
                                        const FunctionParameters &parameters) {
  auto file_name = parameters.values[0].GetValue<String>();
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    String err =
        "Input file is empty or does not exist: " + doubleQuote(file_name);
    return "select '" + err + "' as 'Transpilation Failed.';";
  }
  YAMLConfig config;
  Connection con(*db_instance);
  String code, registration;
  udfCount++;
  auto compiler =
      Compiler(optimizerPassOnMap, &con, buffer.str(), config, udfCount);
  auto res = compiler.run();
  COUT << "Transpiling the UDF..." << ENDL;
  insertDefAndReg(res.code, res.registration, udfCount);
  return "select '' as 'Code Generation Done.';";
}

/**
 * rebuild and load the last udf set
 */
inline String UdfBuilderPragmaFun(ClientContext &context,
                                  const FunctionParameters &parameters) {
  COUT << "Compiling the UDF..." << ENDL;
  compileUDF();
  // load the compiled library
  std::cout << "Installing and loading the UDF..." << std::endl;
  Connection con(*db_instance);
  loadUDF(con);
  return "select '' as 'Building and linking Done.';";
}

inline String UdafBuilderPragmaFun(ClientContext &context,
                                   const FunctionParameters &parameters) {
  COUT << "Compiling the UDAF..." << ENDL;
  compileUDAF();
  // load the compiled library
  std::cout << "Installing and loading the UDF..." << std::endl;
  Connection con(*db_instance);
  loadUDAF(con);
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
  std::set<OptimizerType> tmpOptimizerDisableFlag;

  if (enable_optimizer) {
    if (config.options.disabled_optimizers.count(
            OptimizerType::STATISTICS_PROPAGATION) == 0)
      tmpOptimizerDisableFlag.insert(OptimizerType::STATISTICS_PROPAGATION);
    config.options.disabled_optimizers.insert(
        OptimizerType::STATISTICS_PROPAGATION);
    if (config.options.disabled_optimizers.count(
            OptimizerType::COMMON_SUBEXPRESSIONS) == 0)
      tmpOptimizerDisableFlag.insert(OptimizerType::COMMON_SUBEXPRESSIONS);
    config.options.disabled_optimizers.insert(
        OptimizerType::COMMON_SUBEXPRESSIONS);
  }

  try {
    auto plan = context->ExtractPlan(sql);
    locg.VisitOperator(*plan);
    for (auto &type : tmpOptimizerDisableFlag) {
      config.options.disabled_optimizers.erase(type);
    }
  } catch (Exception &e) {
    for (auto &type : tmpOptimizerDisableFlag) {
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

  auto udf_transpiler_pragma_function = PragmaFunction::PragmaCall(
      "transpile", UdfTranspilerPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, udf_transpiler_pragma_function);
  auto udf_file_transpiler_pragma_function = PragmaFunction::PragmaCall(
      "transpile_file", UdfFileTranspilerPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance,
                                  udf_file_transpiler_pragma_function);
  auto udf_codegen_pragma_function = PragmaFunction::PragmaCall(
      "codegen", UdfCodeGeneratorPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, udf_codegen_pragma_function);
  auto udf_builder_pragma_function =
      PragmaFunction::PragmaCall("build", UdfBuilderPragmaFun, {});
  ExtensionUtil::RegisterFunction(instance, udf_builder_pragma_function);
  auto udaf_builder_pragma_function =
      PragmaFunction::PragmaCall("build_agg", UdafBuilderPragmaFun, {});
  ExtensionUtil::RegisterFunction(instance, udaf_builder_pragma_function);
  auto lo_codegen_pragma_function =
      PragmaFunction::PragmaCall("partial", LOCodeGenPragmaFun,
                                 {LogicalType::VARCHAR, LogicalType::INTEGER});
  ExtensionUtil::RegisterFunction(instance, lo_codegen_pragma_function);
  auto list_compiler_pass_pragma_function =
      PragmaFunction::PragmaCall("list", ListCompilerPassPragmaFun, {});
  ExtensionUtil::RegisterFunction(instance, list_compiler_pass_pragma_function);
  auto enable_compiler_pass_pragma_function = PragmaFunction::PragmaCall(
      "enable", EnableCompilerPassPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance,
                                  enable_compiler_pass_pragma_function);
  auto disable_compiler_pass_pragma_function = PragmaFunction::PragmaCall(
      "disable", DisableCompilerPassPragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance,
                                  disable_compiler_pass_pragma_function);
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
