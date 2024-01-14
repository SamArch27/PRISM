#include "compiler.hpp"
#include "expression_printer.hpp"
#include "pg_query.h"
#include "utils.hpp"
#include "cfg_code_generator.hpp"
#include <iostream>
#include <sstream>
#include <string>

std::ostream &operator<<(std::ostream &os, const Expression &expr) {
  ExpressionPrinter printer(os);
  printer.VisitOperator(expr);
  return os;
}

BasicBlock *
Compiler::constructAssignmentCFG(const json &assignmentJson, Function &function,
                                 List<json> &statements,
                                 const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  std::string assignmentText = getJsonExpr(assignmentJson["expr"]);
  const auto &[left, right] = unpackAssignment(assignmentText);
  auto *var = function.getBinding(left);
  auto expr = bindExpression(function, right);
  newBlock->addInstruction(Make<Assignment>(var, std::move(expr)));
  newBlock->addInstruction(
      Make<BranchInst>(constructCFG(function, statements, continuations)));
  return newBlock;
}

BasicBlock *Compiler::constructReturnCFG(const json &returnJson,
                                         Function &function,
                                         List<json> &statements,
                                         const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  std::string ret = getJsonExpr(returnJson["expr"]);
  newBlock->addInstruction(Make<ReturnInst>(bindExpression(function, ret),
                                            continuations.functionExit));
  return newBlock;
}

BasicBlock *Compiler::constructIfCFG(const json &ifJson, Function &function,
                                     List<json> &statements,
                                     const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  std::string condString = getJsonExpr(ifJson["cond"]);
  auto cond = bindExpression(function, condString);
  auto *afterIfBlock = constructCFG(function, statements, continuations);
  auto thenStatements = getJsonList(ifJson["then_body"]);

  auto newContinuations =
      Continuations(afterIfBlock, continuations.fallthrough,
                    continuations.loopExit, continuations.functionExit);
  auto *thenBlock = constructCFG(function, thenStatements, newContinuations);

  List<json> elseIfStatements;
  if (ifJson.contains("elsif_list")) {
    auto elseIfJsons = ifJson["elsif_list"];
    elseIfStatements = getJsonList(elseIfJsons);
  }

  if (ifJson.contains("else_body")) {
    auto str = "{\"PLpgSQL_if_else\":" + ifJson["else_body"].dump() + "}";
    json j = json::parse(str);
    elseIfStatements.push_back(j);
  }

  newBlock->addInstruction(Make<BranchInst>(
      thenBlock, constructCFG(function, elseIfStatements, newContinuations),
      std::move(cond)));

  return newBlock;
}

BasicBlock *Compiler::constructIfElseCFG(const json &ifElseJson,
                                         Function &function,
                                         List<json> &statements,
                                         const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  auto thenStatements = getJsonList(ifElseJson);
  auto *thenBlock = constructCFG(function, thenStatements, continuations);
  newBlock->addInstruction(Make<BranchInst>(thenBlock));
  return newBlock;
}

BasicBlock *Compiler::constructIfElseIfCFG(const json &ifElseIfJson,
                                           Function &function,
                                           List<json> &statements,
                                           const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  std::string condString = getJsonExpr(ifElseIfJson["cond"]);
  auto cond = bindExpression(function, condString);
  auto thenStatements = getJsonList(ifElseIfJson["stmts"]);
  auto *thenBlock = constructCFG(function, thenStatements, continuations);
  newBlock->addInstruction(Make<BranchInst>(
      thenBlock, constructCFG(function, statements, continuations),
      std::move(cond)));
  return newBlock;
}

BasicBlock *Compiler::constructWhileCFG(const json &whileJson,
                                        Function &function,
                                        List<json> &statements,
                                        const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  std::string condString = getJsonExpr(whileJson["cond"]);
  auto cond = bindExpression(function, condString);

  auto *afterLoopBlock = constructCFG(function, statements, continuations);
  auto bodyStatements = getJsonList(whileJson["body"]);

  auto newContinuations = Continuations(newBlock, newBlock, afterLoopBlock,
                                        continuations.functionExit);
  auto *bodyBlock = constructCFG(function, bodyStatements, newContinuations);
  newBlock->addInstruction(
      Make<BranchInst>(bodyBlock, afterLoopBlock, std::move(cond)));
  return newBlock;
}

BasicBlock *Compiler::constructLoopCFG(const json &loopJson, Function &function,
                                       List<json> &statements,
                                       const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  auto *afterLoopBlock = constructCFG(function, statements, continuations);
  auto bodyStatements = getJsonList(loopJson["body"]);
  auto newContinuations = Continuations(newBlock, newBlock, afterLoopBlock,
                                        continuations.functionExit);
  auto *bodyBlock = constructCFG(function, bodyStatements, newContinuations);
  newBlock->addInstruction(Make<BranchInst>(bodyBlock));
  return newBlock;
}

BasicBlock *Compiler::constructForLoopCFG(const json &forJson,
                                          Function &function,
                                          List<json> &statements,
                                          const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  auto &bodyJson = forJson["body"];

  bool reverse = forJson.contains("reverse");
  auto &lower = forJson["lower"];
  auto &upper = forJson["upper"];

  std::string lowerString = getJsonExpr(lower);
  std::string upperString = getJsonExpr(upper);

  // Default step of 1
  std::string stepString = "1";
  if (forJson.contains("step")) {
    auto &step = forJson["step"];
    stepString = getJsonExpr(step);
  }

  std::string varString = forJson["var"]["PLpgSQL_var"]["refname"];

  // Create assignment <var> =  <lower>
  const auto &[left, right] = std::make_pair(varString, lowerString);
  auto *var = function.getBinding(left);
  auto expr = bindExpression(function, right);
  newBlock->addInstruction(Make<Assignment>(var, std::move(expr)));

  // Create step (assignment) <var> = <var> (reverse ? - : +) <step>
  auto latchBlock = function.makeBasicBlock();
  std::string rhs =
      fmt::format("{} {} {}", varString, (reverse ? " - " : " + "), stepString);
  auto stepExpr = bindExpression(function, rhs);
  latchBlock->addInstruction(Make<Assignment>(var, std::move(stepExpr)));

  // Create condition as terminator into while loop
  auto headerBlock = function.makeBasicBlock();
  std::string condString =
      fmt::format("{} {} {}", left, (reverse ? " > " : " < "), upperString);
  auto cond = bindExpression(function, condString);
  auto *afterLoopBlock = constructCFG(function, statements, continuations);
  auto bodyStatements = getJsonList(bodyJson);

  auto newContinuations = Continuations(latchBlock, latchBlock, afterLoopBlock,
                                        continuations.functionExit);
  auto *bodyBlock = constructCFG(function, bodyStatements, newContinuations);

  headerBlock->addInstruction(
      Make<BranchInst>(bodyBlock, afterLoopBlock, std::move(cond)));

  // Unconditional jump from latch block to header block
  latchBlock->addInstruction(Make<BranchInst>(headerBlock));

  // Unconditional from newBlock to headerBlock
  newBlock->addInstruction(Make<BranchInst>(headerBlock));
  return newBlock;
}

BasicBlock *Compiler::constructExitCFG(const json &exitJson, Function &function,
                                       List<json> &statements,
                                       const Continuations &continuations) {
  auto newBlock = function.makeBasicBlock();
  // continue
  if (!exitJson.contains("is_exit")) {
    newBlock->addInstruction(Make<BranchInst>(continuations.loopHeader));
    return newBlock;
  } else {
    newBlock->addInstruction(Make<BranchInst>(continuations.loopExit));
    return newBlock;
  }
}

BasicBlock *Compiler::constructCFG(Function &function, List<json> &statements,
                                   const Continuations &continuations) {
  if (statements.empty()) {
    return continuations.fallthrough;
  }

  auto statement = statements.front();
  statements.pop_front();

  if (statement.contains("PLpgSQL_stmt_assign")) {
    return constructAssignmentCFG(statement["PLpgSQL_stmt_assign"], function,
                                  statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_return")) {
    return constructReturnCFG(statement["PLpgSQL_stmt_return"], function,
                              statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_if")) {
    return constructIfCFG(statement["PLpgSQL_stmt_if"], function, statements,
                          continuations);
  }
  if (statement.contains("PLpgSQL_if_else")) {
    return constructIfElseCFG(statement["PLpgSQL_if_else"], function,
                              statements, continuations);
  }
  if (statement.contains("PLpgSQL_if_elsif")) {
    return constructIfElseIfCFG(statement["PLpgSQL_if_elsif"], function,
                                statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_while")) {
    return constructWhileCFG(statement["PLpgSQL_stmt_while"], function,
                             statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_loop")) {
    return constructLoopCFG(statement["PLpgSQL_stmt_loop"], function,
                            statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_fori")) {
    return constructForLoopCFG(statement["PLpgSQL_stmt_fori"], function,
                               statements, continuations);
  }

  if (statement.contains("PLpgSQL_stmt_exit")) {
    return constructExitCFG(statement["PLpgSQL_stmt_exit"], function,
                            statements, continuations);
  }

  // We don't have an assignment so we are starting a new basic block
  ERROR(fmt::format("Unknown statement type: {}", statement));
  return nullptr;
}

std::string Compiler::getJsonExpr(const json &json) {
  return json["PLpgSQL_expr"]["query"];
};

List<json> Compiler::getJsonList(const json &body) {
  List<json> res;
  for (auto &statement : body) {
    res.push_back(statement);
  }
  return res;
};

void Compiler::buildCFG(Function &function, const json &ast) {
  const auto &body =
      ast["PLpgSQL_function"]["action"]["PLpgSQL_stmt_block"]["body"];

  auto *entryBlock = function.makeBasicBlock("entry");
  auto *functionExitBlock = function.makeBasicBlock("exit");

  // Create a "declare" BasicBlock with all declarations
  auto declareBlock = function.makeBasicBlock();
  auto declarations = function.takeDeclarations();
  for (auto &declaration : declarations) {
    declareBlock->addInstruction(std::move(declaration));
  }

  // Get the statements from the body
  auto statements = getJsonList(body);

  // Connect "entry" block to "declare" block
  entryBlock->addInstruction(Make<BranchInst>(declareBlock));

  // Finally, jump to the "declare" block from the "entry" BasicBlock
  auto initialContinuations =
      Continuations(functionExitBlock, nullptr, nullptr, functionExitBlock);
  declareBlock->addInstruction(Make<BranchInst>(
      constructCFG(function, statements, initialContinuations)));
}

void Function::mergeBasicBlocks() {
  Map<BasicBlock *, Vec<BasicBlock *>> pred;

  // compute predecessors
  visitBFS([&pred, this](BasicBlock *block) {
    if (block == getExitBlock()) {
      return;
    }

    for (auto &succ : block->getTerminator()->get()->getSuccessors()) {
      pred[succ].push_back(block);
    }
  });

  // merge basic blocks
  visitBFS([&pred, this](BasicBlock *block) {
    if (block == getExitBlock()) {
      return;
    }
    while (true) {
      auto successors = block->getTerminator()->get()->getSuccessors();
      // if we are not the entry block
      if (block == getEntryBlock()) {
        return;
      }
      // if we have an unconditional jump to a block that
      if (successors.size() != 1) {
        return;
      }
      auto *succ = successors.back();
      // and that block has only a single predecessor
      if (succ == getExitBlock()) {
        return;
      }
      if (pred[succ].size() != 1) {
        return;
      }

      // remove terminator from block
      block->popInstruction();
      // copy instructions from successor block to this block
      auto instructions = succ->takeInstructions();
      for (auto &inst : instructions) {
        block->addInstruction(std::move(inst));
      }
      // finally remove the basic block from the function
      removeBasicBlock(succ);
      pred.erase(succ);
    }
  });
}

void Compiler::run(std::string &code, std::string &registration) {

  auto asts = parseJson();
  auto functions = getFunctions();

  auto header = "PLpgSQL_function";
  for (const auto &ast : asts) {
    ASSERT(ast.contains(header),
           std::string("UDF is missing the magic header string: ") + header);
  }

  for (std::size_t i = 0; i < functions.size(); ++i) {

    auto ast = asts[i];
    auto datums = ast["PLpgSQL_function"]["datums"];
    ASSERT(datums.is_array(), "Datums is not an array.");
    auto &function = functions[i];

    bool readingArguments = true;

    for (const auto &datum : datums) {

      ASSERT(datum.contains("PLpgSQL_var"),
             "Datum does not contain PLpgSQL_var.");
      auto variable = datum["PLpgSQL_var"];
      std::string variableName = variable["refname"];
      if (variableName == "found") {
        readingArguments = false;
        continue;
      }

      std::string variableType =
          variable["datatype"]["PLpgSQL_type"]["typname"];

      if (variableType == "UNKNOWN") {
        if (function.hasBinding(variableName)) {
          continue;
        }
        ERROR(fmt::format(
            "Error: Variable {} in cursor loop must be declared first",
            variableName));
        continue;
      }

      if (readingArguments) {
        function.addArgument(variableName,
                             getTypeFromPostgresName(variableType));
      } else {
        // If the declared variable has a default value (i.e. DECLARE x = 0;)
        // then get it (otherwise assign to NULL)
        std::string defaultVal =
            variable.contains("default_val")
                ? variable["default_val"]["PLpgSQL_expr"]["query"]
                      .get<std::string>()
                : "NULL";
        function.addVariable(variableName,
                             getTypeFromPostgresName(variableType),
                             bindExpression(function, defaultVal));
      }
    }

    buildCFG(function, ast);
    function.mergeBasicBlocks();

    // TODO:
    // 1. Lower to C++
    // 2. Delete Yuchen's code
    // 3. Aggify

    for (const auto &function : functions) {
      std::cout << function << std::endl;
      auto res = generateCode(function);
      code += res[0] + "\n";
      registration += res[1] + "\n";
    }
  }
}

Own<Expression> Compiler::bindExpression(const Function &function,
                                         const std::string &expression) {

  std::stringstream createTableString;
  createTableString << "CREATE TABLE tmp(";
  std::stringstream insertTableString;
  insertTableString << "INSERT INTO tmp VALUES(";

  bool first = true;
  for (const auto &[name, variable] : function.getAllBindings()) {
    auto type = variable->getType();
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
  connectionContext->config.enable_optimizer = false;

  // SELECT <expr> FROM tmp
  duckdb::unique_ptr<duckdb::LogicalOperator> boundExpression;
  try
  {
    boundExpression =
      connectionContext->ExtractPlan(selectExpressionCommand);
  }
  catch(const std::exception& e)
  {
    connection->Query(dropTableCommand);
    throw e;
    return nullptr;
  }
  // DROP tmp
  connection->Query(dropTableCommand);
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

  else if (upper.starts_with("VARCHAR")) {
    return nameToTag.at("VARCHAR");
  }

  if (nameToTag.find(upper) == nameToTag.end()) {
    EXCEPTION(type + " is not a valid Postgres Type.");
  }
  return nameToTag.at(upper);
}

Compiler::StringPair Compiler::unpackAssignment(const std::string &assignment) {
  std::regex pattern(ASSIGNMENT_PATTERN, std::regex_constants::icase);
  std::smatch matches;
  std::regex_search(assignment, matches, pattern);
  return std::make_pair(matches.prefix(), matches.suffix());
}

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

/**
 * Generate code for a function
 * Initialize a CFGCodeGenerator and run it through the function
*/
std::vector<std::string> Compiler::generateCode(const Function &func){
  CFGCodeGenerator codeGenerator(connection);
  return codeGenerator.run(func);
}
