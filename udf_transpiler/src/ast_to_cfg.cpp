#include "ast_to_cfg.hpp"
#include "utils.hpp"
#include <regex>

Own<Function> AstToCFG::createFunction(const json &ast, const String &name,
                                       const String &returnType) {
  ASSERT(ast.contains(MAGIC_HEADER),
         String("UDF is missing the magic header String: ") + MAGIC_HEADER);
  auto datums = ast["PLpgSQL_function"]["datums"];
  ASSERT(datums.is_array(), "Datums is not an array.");

  auto resolvedReturnType = getTypeFromPostgresName(returnType);
  auto f = Make<Function>(conn, name, resolvedReturnType);

  bool readingArguments = true;
  Vec<std::pair<String, String>> pendingInitialization;

  for (const auto &datum : datums) {
    if (!datum.contains("PLpgSQL_var"))
      continue;
    auto variable = datum["PLpgSQL_var"];
    String variableName = variable["refname"];
    if (variableName == "found") {
      readingArguments = false;
      continue;
    }

    String variableType = variable["datatype"]["PLpgSQL_type"]["typname"];

    if (variableType == "UNKNOWN") {
      if (f->hasBinding(variableName)) {
        continue;
      }
      EXCEPTION(fmt::format(
          "Error: Variable {} in cursor loop must be declared first",
          variableName));
      continue;
    }

    if (readingArguments) {
      f->addArgument(variableName, getTypeFromPostgresName(variableType));
    } else {
      // If the declared variable has a default value (i.e. DECLARE x = 0;)
      // then get it (otherwise assign to the default value of the type)
      auto varType = getTypeFromPostgresName(variableType);
      String defaultVal =
          variable.contains("default_val")
              ? variable["default_val"]["PLpgSQL_expr"]["query"].get<String>()
              : varType.getDefaultValue(true);
      f->addVariable(variableName, std::move(varType),
                     !variable.contains("default_val"));
      pendingInitialization.emplace_back(variableName, defaultVal);
    }
  }

  f->makeDuckDBContext();

  for (auto &init : pendingInitialization) {
    auto var = f->getBinding(init.first);
    auto expr = f->bindExpression(init.second);
    f->addVarInitialization(var, std::move(expr));
  }

  buildCFG(*f, ast);
  return f;
}

void AstToCFG::buildCFG(Function &f, const json &ast) {
  const auto &body =
      ast["PLpgSQL_function"]["action"]["PLpgSQL_stmt_block"]["body"];

  auto *entryBlock = f.makeBasicBlock("entry");

  // Create a "declare" BasicBlock with all declarations
  auto declareBlock = f.makeBasicBlock();
  auto declarations = f.takeDeclarations();
  for (auto &declaration : declarations) {
    declareBlock->addInstruction(std::move(declaration));
  }

  // Get the statements from the body
  auto statements = getJsonList(body);

  // Connect "entry" block to "declare" block
  entryBlock->addInstruction(Make<BranchInst>(declareBlock));

  // Finally, jump to the "declare" block from the "entry" BasicBlock
  auto initialContinuations = Continuations(nullptr, nullptr, nullptr);
  declareBlock->addInstruction(
      Make<BranchInst>(constructCFG(f, statements, initialContinuations)));
}

BasicBlock *AstToCFG::constructCFG(Function &f, List<json> &statements,
                                   const Continuations &continuations) {
  if (statements.empty()) {
    return continuations.fallthrough;
  }

  auto statement = statements.front();
  statements.pop_front();

  if (statement.contains("PLpgSQL_stmt_assign")) {
    return constructAssignmentCFG(statement["PLpgSQL_stmt_assign"], f,
                                  statements, continuations);
  }
  if (statement.contains("PLpgSQL_stmt_return")) {
    return constructReturnCFG(statement["PLpgSQL_stmt_return"], f, statements,
                              continuations);
  }
  if (statement.contains("PLpgSQL_stmt_if")) {
    return constructIfCFG(statement["PLpgSQL_stmt_if"], f, statements,
                          continuations);
  }
  if (statement.contains("PLpgSQL_if_else")) {
    return constructIfElseCFG(statement["PLpgSQL_if_else"], f, statements,
                              continuations);
  }
  if (statement.contains("PLpgSQL_if_elsif")) {
    return constructIfElseIfCFG(statement["PLpgSQL_if_elsif"], f, statements,
                                continuations);
  }
  if (statement.contains("PLpgSQL_stmt_while")) {
    return constructWhileCFG(statement["PLpgSQL_stmt_while"], f, statements,
                             continuations);
  }
  if (statement.contains("PLpgSQL_stmt_loop")) {
    return constructLoopCFG(statement["PLpgSQL_stmt_loop"], f, statements,
                            continuations);
  }
  if (statement.contains("PLpgSQL_stmt_fori")) {
    return constructForLoopCFG(statement["PLpgSQL_stmt_fori"], f, statements,
                               continuations);
  }

  if (statement.contains("PLpgSQL_stmt_exit")) {
    return constructExitCFG(statement["PLpgSQL_stmt_exit"], f, statements,
                            continuations);
  }

  if (statement.contains("PLpgSQL_stmt_fors")) {
    return constructCursorLoopCFG(statement["PLpgSQL_stmt_fors"], f, statements,
                                  continuations);
  }

  // We don't have an assignment so we are starting a new basic block
  ERROR(fmt::format("Unknown statement type: {}", statement.dump()));
  return nullptr;
}

BasicBlock *
AstToCFG::constructAssignmentCFG(const json &assignmentJson, Function &f,
                                 List<json> &statements,
                                 const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  String assignmentText = getJsonExpr(assignmentJson["expr"]);
  const auto &[left, right] = unpackAssignment(assignmentText);
  auto *var = f.getBinding(left);
  auto expr = f.bindExpression(right);

  bool isSQLExpression = expr->isSQLExpression();
  newBlock->addInstruction(Make<Assignment>(var, std::move(expr)));
  newBlock->addInstruction(
      Make<BranchInst>(constructCFG(f, statements, continuations)));
  if (isSQLExpression) {
    // add a pre-header
    auto preHeader = f.makeBasicBlock();
    preHeader->addInstruction(Make<BranchInst>(newBlock));
    return preHeader;
  } else {
    return newBlock;
  }
}

BasicBlock *AstToCFG::constructReturnCFG(const json &returnJson, Function &f,
                                         List<json> &statements,
                                         const Continuations &continuations) {
  if (!returnJson.contains("expr")) {
    f.destroyDuckDBContext();
    EXCEPTION("There is a path without return value.");
    return nullptr;
  }
  auto newBlock = f.makeBasicBlock();
  String ret = getJsonExpr(returnJson["expr"]);
  newBlock->addInstruction(Make<ReturnInst>(f.bindExpression(ret)));
  return newBlock;
}

BasicBlock *AstToCFG::constructIfCFG(const json &ifJson, Function &f,
                                     List<json> &statements,
                                     const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  String condString = getJsonExpr(ifJson["cond"]);
  auto cond = f.bindExpression(condString);
  auto *afterIfBlock = constructCFG(f, statements, continuations);
  auto thenStatements = getJsonList(ifJson["then_body"]);

  auto newContinuations = Continuations(afterIfBlock, continuations.fallthrough,
                                        continuations.loopExit);
  auto *thenBlock = constructCFG(f, thenStatements, newContinuations);

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
      thenBlock, constructCFG(f, elseIfStatements, newContinuations),
      std::move(cond)));

  auto preHeader = f.makeBasicBlock();
  preHeader->addInstruction(Make<BranchInst>(newBlock));
  return preHeader;
}

BasicBlock *AstToCFG::constructIfElseCFG(const json &ifElseJson, Function &f,
                                         List<json> &statements,
                                         const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  auto thenStatements = getJsonList(ifElseJson);
  auto *thenBlock = constructCFG(f, thenStatements, continuations);
  newBlock->addInstruction(Make<BranchInst>(thenBlock));
  auto preHeader = f.makeBasicBlock();
  preHeader->addInstruction(Make<BranchInst>(newBlock));
  return preHeader;
}

BasicBlock *AstToCFG::constructIfElseIfCFG(const json &ifElseIfJson,
                                           Function &f, List<json> &statements,
                                           const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  String condString = getJsonExpr(ifElseIfJson["cond"]);
  auto cond = f.bindExpression(condString);
  auto thenStatements = getJsonList(ifElseIfJson["stmts"]);
  auto *thenBlock = constructCFG(f, thenStatements, continuations);
  newBlock->addInstruction(Make<BranchInst>(
      thenBlock, constructCFG(f, statements, continuations), std::move(cond)));
  auto preHeader = f.makeBasicBlock();
  preHeader->addInstruction(Make<BranchInst>(newBlock));
  return preHeader;
}

BasicBlock *AstToCFG::constructWhileCFG(const json &whileJson, Function &f,
                                        List<json> &statements,
                                        const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  String condString = getJsonExpr(whileJson["cond"]);
  auto cond = f.bindExpression(condString);

  auto *afterLoopBlock = constructCFG(f, statements, continuations);
  auto bodyStatements = getJsonList(whileJson["body"]);

  auto newContinuations = Continuations(newBlock, newBlock, afterLoopBlock);
  auto *bodyBlock = constructCFG(f, bodyStatements, newContinuations);

  // create a block for the condition
  auto condBlock = f.makeBasicBlock();
  condBlock->addInstruction(
      Make<BranchInst>(bodyBlock, afterLoopBlock, std::move(cond)));
  // the block jumps immediately to the cond block
  newBlock->addInstruction(Make<BranchInst>(condBlock));

  auto preHeader = f.makeBasicBlock();
  preHeader->addInstruction(Make<BranchInst>(newBlock));
  return preHeader;
}

BasicBlock *AstToCFG::constructLoopCFG(const json &loopJson, Function &f,
                                       List<json> &statements,
                                       const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  auto *afterLoopBlock = constructCFG(f, statements, continuations);
  auto bodyStatements = getJsonList(loopJson["body"]);
  auto newContinuations = Continuations(newBlock, newBlock, afterLoopBlock);
  auto *bodyBlock = constructCFG(f, bodyStatements, newContinuations);
  newBlock->addInstruction(Make<BranchInst>(bodyBlock));
  auto preHeader = f.makeBasicBlock();
  preHeader->addInstruction(Make<BranchInst>(newBlock));
  return preHeader;
}

BasicBlock *AstToCFG::constructForLoopCFG(const json &forJson, Function &f,
                                          List<json> &statements,
                                          const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  auto &bodyJson = forJson["body"];

  bool reverse = forJson.contains("reverse");
  auto &lower = forJson["lower"];
  auto &upper = forJson["upper"];

  String lowerString = getJsonExpr(lower);
  String upperString = getJsonExpr(upper);

  // Default step of 1
  String stepString = "1";
  if (forJson.contains("step")) {
    auto &step = forJson["step"];
    stepString = getJsonExpr(step);
  }

  String varString = forJson["var"]["PLpgSQL_var"]["refname"];

  // Create assignment <var> =  <lower>
  const auto &[left, right] = std::make_pair(varString, lowerString);
  auto *var = f.getBinding(left);
  auto expr = f.bindExpression(right);
  newBlock->addInstruction(Make<Assignment>(var, std::move(expr)));

  // Create step (assignment) <var> = <var> (reverse ? - : +) <step>
  auto latchBlock = f.makeBasicBlock();
  String rhs =
      fmt::format("{} {} {}", varString, (reverse ? " - " : " + "), stepString);
  auto stepExpr = f.bindExpression(rhs);
  latchBlock->addInstruction(Make<Assignment>(var, std::move(stepExpr)));

  // Create condition as terminator into while loop
  auto headerBlock = f.makeBasicBlock();
  String condString =
      fmt::format("{} {} {}", left, (reverse ? " > " : " < "), upperString);
  auto cond = f.bindExpression(condString);
  auto *afterLoopBlock = constructCFG(f, statements, continuations);
  auto bodyStatements = getJsonList(bodyJson);

  auto newContinuations = Continuations(latchBlock, latchBlock, afterLoopBlock);
  auto *bodyBlock = constructCFG(f, bodyStatements, newContinuations);

  // create a block for the condition
  auto condBlock = f.makeBasicBlock();
  condBlock->addInstruction(
      Make<BranchInst>(bodyBlock, afterLoopBlock, std::move(cond)));
  // the block jumps immediately to the cond block
  headerBlock->addInstruction(Make<BranchInst>(condBlock));

  // Unconditional jump from latch block to header block
  latchBlock->addInstruction(Make<BranchInst>(headerBlock));

  // Unconditional from newBlock to header block
  newBlock->addInstruction(Make<BranchInst>(headerBlock));
  return newBlock;
}

BasicBlock *AstToCFG::constructExitCFG(const json &exitJson, Function &f,
                                       List<json> &statements,
                                       const Continuations &continuations) {
  auto newBlock = f.makeBasicBlock();
  // continue
  if (!exitJson.contains("is_exit")) {
    newBlock->addInstruction(Make<BranchInst>(continuations.loopHeader));
    return newBlock;
  } else {
    newBlock->addInstruction(Make<BranchInst>(continuations.loopExit));
    return newBlock;
  }
}

BasicBlock *
AstToCFG::constructCursorLoopCFG(const json &cursorLoopJson, Function &f,
                                 List<json> &statements,
                                 const Continuations &continuations) {
  ERROR("Cursor loops are not currently supported.");
  auto newBlock = f.makeBasicBlock();
  return newBlock;
}

void AstToCFG::buildCursorLoopCFG(Function &f, const json &ast) {
  const auto &body = ast["body"];
  auto *entryBlock = f.makeBasicBlock("entry");

  // Get the statements from the body
  auto statements = getJsonList(body);

  // Connect "entry" block to initial block
  auto initialContinuations = Continuations(nullptr, nullptr, nullptr);
  entryBlock->addInstruction(
      Make<BranchInst>(constructCFG(f, statements, initialContinuations)));
}

StringPair AstToCFG::unpackAssignment(const String &assignment) {
  std::regex pattern(ASSIGNMENT_PATTERN, std::regex_constants::icase);
  std::smatch matches;
  std::regex_search(assignment, matches, pattern);
  return std::make_pair(matches.prefix(), matches.suffix());
}

String AstToCFG::getJsonExpr(const json &json) {
  return json["PLpgSQL_expr"]["query"];
}

List<json> AstToCFG::getJsonList(const json &body) {
  List<json> res;
  for (auto &statement : body) {
    res.push_back(statement);
  }
  return res;
}

PostgresTypeTag AstToCFG::getPostgresTag(const String &type) {
  // remove spaces and capitalize the name
  String upper = toUpper(removeSpaces(type));

  Map<String, PostgresTypeTag> nameToTag = {
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

Opt<WidthScale> AstToCFG::getDecimalWidthScale(const String &type) {
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

Type AstToCFG::getTypeFromPostgresName(const String &name) const {
  auto resolvedName = resolveTypeName(name);
  auto tag = getPostgresTag(resolvedName);
  if (tag == PostgresTypeTag::DECIMAL) {
    // provide width, scale info if available
    auto widthScale = getDecimalWidthScale(resolvedName);
    if (widthScale) {
      auto [width, scale] = *widthScale;
      return Type(true, width, scale, tag);
    } else {
      return Type(true, std::nullopt, std::nullopt, tag);
    }
  } else {
    return Type(false, std::nullopt, std::nullopt, tag);
  }
}

String AstToCFG::resolveTypeName(const String &type) const {
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
