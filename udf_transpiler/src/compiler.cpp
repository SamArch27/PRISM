#include "compiler.hpp"
#include "pg_query.h"
#include "utils.hpp"
#include <iostream>
#include <string>

Compiler::Compiler(const std::string &udfs, bool optimize) {

  auto json = parseJson(udfs);
  auto functionMetadata = getFunctionMetadata(udfs);

  for (const auto &metadata : functionMetadata) {
    std::cout << "Function Name: " << metadata.functionName << std::endl;
    std::cout << "Return Type: " << *(metadata.returnType) << std::endl;
  }

  auto header = "PLpgSQL_function";
  for (const auto &udf : json) {
    ASSERT(udf.contains(header),
           std::string("UDF is missing the magic header string: ") + header);
  }

  /*
          std::cout << function << std::endl;
          auto datums = function["datums"];
          ASSERT(datums.is_array(), "Datums is not an array.");

          for (auto &datum : datums)
          {
              ASSERT(datum.contains("PLpgSQL_var"), "Datum does not contain
     PLpgSQL_var."); auto var  = datum["PLpgSQL_var"]; auto name =
     var["refname"]; auto type = var["datatype"]["PLpgSQL_type"]["typname"];
              // addVariable(type, name);
          }
         */
}

json Compiler::parseJson(const std::string &udf) const {
  auto result = pg_query_parse_plpgsql(udf.c_str());
  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    ERROR(fmt::format("Error when parsing the plpgsql: {}",
                      result.error->message));
  }
  auto json = json::parse(result.plpgsql_funcs);
  pg_query_free_plpgsql_parse_result(result);
  return json;
}

std::vector<FunctionMetadata>
Compiler::getFunctionMetadata(const std::string &udfs) const {
  std::vector<FunctionMetadata> functionMetadata;

  // Collect function names from UDFs
  auto functionNames = extractMatches(udfs, FUNCTION_NAME_PATTERN, 1);

  // Collect return type strings from UDFs
  // Need to transform them from PL/pgSQL to DuckDB types
  auto returnTypes = extractMatches(udfs, RETURN_TYPE_PATTERN, 1);

  // Ensure that #ReturnTypes = #FunctionNames
  ASSERT(returnTypes.size() >= functionNames.size(),
         "Return type not specified for all functions");
  ASSERT(functionNames.size() >= returnTypes.size(),
         "Function name not specified for all functions");

  // Construct the FunctionMetadata and return
  for (std::size_t i = 0; i < functionNames.size(); ++i) {
    functionMetadata.emplace_back(
        functionNames[i], Type::getTypeFromPostgresName(returnTypes[i], udfs));
  }
  return functionMetadata;
}
