#include "compiler.hpp"
#include "pg_query.h"
#include "utils.hpp"
#include <iostream>
#include <sstream>
#include <string>

void Compiler::run() {

  auto json = parseJson();

  std::cout << json << std::endl;

  auto functions = getFunctions();

  for (const auto &function : functions) {
    std::cout << "Function Name: " << function.getFunctionName() << std::endl;
    std::cout << "Return Type: " << *(function.getReturnType()) << std::endl;
  }

  auto header = "PLpgSQL_function";
  for (const auto &udf : json) {
    ASSERT(udf.contains(header),
           std::string("UDF is missing the magic header string: ") + header);
  }

  for (std::size_t i = 0; i < functions.size(); ++i) {

    auto datums = json[i]["PLpgSQL_function"]["datums"];
    ASSERT(datums.is_array(), "Datums is not an array.");

    bool readingArguments = true;

    for (const auto &datum : datums) {

      ASSERT(datum.contains("PLpgSQL_var"),
             "Datum does not contain PLpgSQL_var.");
      auto variable = datum["PLpgSQL_var"];
      auto variableName = variable["refname"];
      if (variableName == "found") {
        readingArguments = false;
        continue;
      }
      auto variableType = variable["datatype"]["PLpgSQL_type"]["typname"];

      if (readingArguments) {
        functions[i].addArgument(variableName,
                                 getTypeFromPostgresName(variableType));
      } else {
        // If the declared variable has a default value (i.e. DECLARE x = 0;)
        // then get it (otherwise assign to NULL)
        std::string defaultVal =
            variable.contains("default_val")
                ? variable["default_val"]["PLpgSQL_expr"]["query"]
                      .get<std::string>()
                : "NULL";
        functions[i].addVariable(variableName,
                                 getTypeFromPostgresName(variableType),
                                 bindExpression(functions[i], defaultVal));
      }
    }
  }

  for (const auto &function : functions) {
    std::cout << "-----------------------------" << std::endl;
    std::cout << "Function Name: " << function.getFunctionName() << std::endl;
    std::cout << "Return Type: " << *(function.getReturnType()) << std::endl;
    std::cout << "Arguments: " << std::endl;
    for (const auto &argument : function.getArguments()) {
      std::cout << "\t[" << argument->getName() << "," << *(argument->getType())
                << "]" << std::endl;
    }
    std::cout << "Variables: " << std::endl;
    for (const auto &variable : function.getVariables()) {
      std::cout << "\t[" << variable->getName() << "," << *(variable->getType())
                << "]" << std::endl;
    }
    std::cout << "Declarations: " << std::endl;
    for (const auto &declaration : function.getDeclarations()) {
      std::cout << "\t" << declaration->getVar()->getName() << " = "
                << std::endl;
      std::cout << declaration->getExpr()->ToString() << std::endl;
    }
    std::cout << "-----------------------------" << std::endl;
  }

  // TODO:
  // 1. Create a BasicBlock class (VecOwn<Instruction>) with addInstruction(...)
  // 2. Create a ControlFlowGraph class which holds connectivity info for
  // BasicBlocks (successors, predecessors, etc.)
  // 3. Visit the AST and construct the corresponding ControlFlowGraph correctly
  // (think about cursor loops)
  // 4. Visit and code gen to C++ (using Yuchen's visitor)
}

Own<Expression> Compiler::bindExpression(const Function &function,
                                         const std::string &expression) {

  std::stringstream createTableString;
  createTableString << "CREATE TABLE tmp(";
  std::stringstream insertTableString;
  insertTableString << "INSERT INTO tmp VALUES(";

  bool first = true;
  for (const auto &binding : function.getBindings()) {
    auto &[name, type] = binding;
    createTableString << (first ? "" : ", ");
    insertTableString << (first ? "" : ", ");
    if (first) {
      first = false;
    }
    createTableString << name << " " << *type;
    insertTableString << "(NULL) ";
  }
  createTableString << ");";
  insertTableString << ");";

  // Create commands
  std::string createTableCommand = createTableString.str();
  std::string insertTableCommand = insertTableString.str();
  std::string selectExpressionCommand = "SELECT " + expression + " FROM tmp;";
  std::string dropTableCommand = "DROP TABLE tmp;";

  // CREATE TABLE
  auto res = connection->Query(createTableCommand);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }

  // INSERT (NULL,NULL,...)
  res = connection->Query(insertTableCommand);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }

  auto connectionContext = connection->context.get();

  // SELECT <expr> FROM tmp
  auto boundExpression =
      connectionContext->ExtractPlan(selectExpressionCommand);

  // DROP tmp
  connection->Query(dropTableCommand);

  std::cout << boundExpression->ToString() << std::endl;

  return std::move(boundExpression);
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

Vec<Function> Compiler::getFunctions() const {
  Vec<Function> functions;

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

  // Construct the function and return
  for (std::size_t i = 0; i < functionNames.size(); ++i) {
    functions.emplace_back(functionNames[i],
                           getTypeFromPostgresName(returnTypes[i]));
  }
  return functions;
}

PostgresTypeTag Compiler::getPostgresTag(const std::string &type) {
  // remove spaces and capitalize the name
  std::string upper = toUpper(removeSpaces(type));

  Map<std::string, PostgresTypeTag> nameToTag = {
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

Opt<Compiler::WidthScale>
Compiler::getDecimalWidthScale(const std::string &type) {
  std::regex decimalPattern("DECIMAL\\((\\d+),(\\d+)\\)",
                            std::regex_constants::icase);
  std::smatch decimalMatch;
  auto strippedString = removeSpaces(type);
  std::regex_search(strippedString, decimalMatch, decimalPattern);
  if (decimalMatch.size() == 3) {
    auto width = std::stoi(decimalMatch[1]);
    auto scale = std::stoi(decimalMatch[2]);
    std::cout << "Width: " << width << std::endl;
    std::cout << "Scale: " << scale << std::endl;
    return {std::make_pair(width, scale)};
  }
  return {};
}

Own<Type> Compiler::getTypeFromPostgresName(const std::string &name) const {
  auto resolvedName = resolveTypeName(name);
  auto tag = getPostgresTag(resolvedName);
  if (tag == PostgresTypeTag::DECIMAL) {
    // provide width, scale info if available
    auto widthScale = getDecimalWidthScale(resolvedName);
    if (widthScale) {
      auto [width, scale] = *widthScale;
      return Make<DecimalType>(tag, width, scale);
    } else {
      return Make<DecimalType>(tag);
    }
  } else {
    return Make<NonDecimalType>(tag);
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
