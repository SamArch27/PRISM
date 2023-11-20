#include "compiler.hpp"
#include "pg_query.h"
#include "utils.hpp"
#include <iostream>
#include <string>

void Compiler::run() {

  auto json = parseJson();
  auto functionMetadata = getFunctionMetadata();

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

json Compiler::parseJson() const {
  auto result = pg_query_parse_plpgsql(programText.c_str());
  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    ERROR(fmt::format("Error when parsing the plpgsql: {}",
                      result.error->message));
  }
  auto json = json::parse(result.plpgsql_funcs);
  pg_query_free_plpgsql_parse_result(result);
  return json;
}

std::vector<FunctionMetadata> Compiler::getFunctionMetadata() const {
  std::vector<FunctionMetadata> functionMetadata;

  // Collect function names from program text
  auto functionNames = extractMatches(programText, FUNCTION_NAME_PATTERN, 1);

  // Collect return type strings from program text
  // Need to transform them from PL/pgSQL to DuckDB types
  auto returnTypes = extractMatches(programText, RETURN_TYPE_PATTERN, 1);

  // Ensure that #ReturnTypes = #FunctionNames
  ASSERT(returnTypes.size() >= functionNames.size(),
         "Return type not specified for all functions");
  ASSERT(functionNames.size() >= returnTypes.size(),
         "Function name not specified for all functions");

  // Construct the FunctionMetadata and return
  for (std::size_t i = 0; i < functionNames.size(); ++i) {
    functionMetadata.emplace_back(functionNames[i],
                                  getTypeFromPostgresName(returnTypes[i]));
  }
  return functionMetadata;
}

PostgresTypeTag Compiler::getPostgresTag(const std::string &type) {
  // remove spaces and capitalize the name
  std::string upper = toUpper(removeSpaces(type));

  std::unordered_map<std::string, PostgresTypeTag> nameToTag = {
      {"BIGINT", PostgresTypeTag::BIGINT},
      {"BINARY", PostgresTypeTag::BINARY},
      {"BIT", PostgresTypeTag::BIT},
      {"BITSTRING", PostgresTypeTag::BITSTRING},
      {"BLOB", PostgresTypeTag::BLOB},
      {"BOOL", PostgresTypeTag::BOOL},
      {"BOOLEAN", PostgresTypeTag::BOOLEAN},
      {"BPCHAR", PostgresTypeTag::BPCHAR},
      {"BYTEA", PostgresTypeTag::BYTEA},
      {"CHAR", PostgresTypeTag::CHAR},
      {"DATE", PostgresTypeTag::DATE},
      {"DATETIME", PostgresTypeTag::DATETIME},
      {"DECIMAL", PostgresTypeTag::DECIMAL},
      {"DOUBLE", PostgresTypeTag::DOUBLE},
      {"FLOAT", PostgresTypeTag::FLOAT},
      {"FLOAT4", PostgresTypeTag::FLOAT4},
      {"FLOAT8", PostgresTypeTag::FLOAT8},
      {"HUGEINT", PostgresTypeTag::HUGEINT},
      {"INT", PostgresTypeTag::INT},
      {"INT1", PostgresTypeTag::INT1},
      {"INT2", PostgresTypeTag::INT2},
      {"INT4", PostgresTypeTag::INT4},
      {"INT8", PostgresTypeTag::INT8},
      {"INTEGER", PostgresTypeTag::INTEGER},
      {"INTERVAL", PostgresTypeTag::INTERVAL},
      {"LOGICAL", PostgresTypeTag::LOGICAL},
      {"LONG", PostgresTypeTag::LONG},
      {"NUMERIC", PostgresTypeTag::NUMERIC},
      {"REAL", PostgresTypeTag::REAL},
      {"SHORT", PostgresTypeTag::SHORT},
      {"SIGNED", PostgresTypeTag::SIGNED},
      {"SMALLINT", PostgresTypeTag::SMALLINT},
      {"STRING", PostgresTypeTag::STRING},
      {"TEXT", PostgresTypeTag::TEXT},
      {"TIME", PostgresTypeTag::TIME},
      {"TIMESTAMP", PostgresTypeTag::TIMESTAMP},
      {"TINYINT", PostgresTypeTag::TINYINT},
      {"UBIGINT", PostgresTypeTag::UBIGINT},
      {"UINTEGER", PostgresTypeTag::UINTEGER},
      {"UNKNOWN", PostgresTypeTag::UNKNOWN},
      {"USMALLINT", PostgresTypeTag::USMALLINT},
      {"UTINYINT", PostgresTypeTag::UTINYINT},
      {"UUID", PostgresTypeTag::UUID},
      {"VARBINARY", PostgresTypeTag::VARBINARY},
      {"VARCHAR", PostgresTypeTag::VARCHAR}};

  // Edge case for DECIMAL(width,scale)
  if (upper.starts_with("DECIMAL")) {
    return nameToTag.at("DECIMAL");
  }

  if (nameToTag.find(upper) == nameToTag.end()) {
    ERROR(type + " is not a valid Postgres Type.");
  }
  return nameToTag.at(upper);
};

std::optional<Compiler::WidthScale>
Compiler::getDecimalWidthScale(const std::string &type) {
  std::regex decimalPattern("DECIMAL\\((\\d+),(\\d+)\\)",
                            std::regex_constants::icase);
  std::smatch decimalMatch;
  auto strippedString = removeSpaces(type);
  std::regex_search(strippedString, decimalMatch, decimalPattern);
  if (decimalMatch.size() == 3) {
    auto width = std::stoi(decimalMatch[1]);
    auto scale = std::stoi(decimalMatch[2]);
    return {std::make_pair(width, scale)};
  }
  return {};
}

std::unique_ptr<Type>
Compiler::getTypeFromPostgresName(const std::string &name) const {
  auto tag = getPostgresTag(resolveTypeName(name));
  if (tag == PostgresTypeTag::DECIMAL) {
    // provide width, scale info if available
    auto widthScale = getDecimalWidthScale(name);
    if (widthScale) {
      auto [width, scale] = *widthScale;
      return std::make_unique<DecimalType>(tag, width, scale);
    } else {
      return std::make_unique<DecimalType>(tag);
    }
  } else {
    return std::make_unique<NonDecimalType>(tag);
  }
}

std::string Compiler::resolveTypeName(const std::string &type) const {
  if (!type.starts_with('#')) {
    ASSERT(!type.empty(), "Type name is empty");
    return type;
  }

  auto offset = type.find('#');
  auto typeOffset = std::stoi(type.substr(offset + 1));
  std::regex typeRegex("(\\w+ *(\\((\\d+, *)?\\d+\\))?)",
                       std::regex_constants::icase);

  std::smatch matches;
  std::regex_search(programText.begin() + typeOffset, programText.end(),
                    matches, typeRegex);
  ASSERT(matches.size() == 4, "Invalid type format.");
  return matches[0];
}
