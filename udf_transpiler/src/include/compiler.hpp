
#pragma once

#include "compiler_fmt/core.h"
#include "dominator_dataflow.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "instructions.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <functional>
#include <json.hpp>
#include <memory>
#include <optional>
#include <queue>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cfg_code_generator.hpp"

/* Compiler */

using json = nlohmann::json;

class DominanceFrontier;
class DominatorTree;

struct Continuations {
  Continuations(BasicBlock *fallthrough, BasicBlock *loopHeader,
                BasicBlock *loopExit, BasicBlock *functionExit)
      : fallthrough(fallthrough), loopHeader(loopHeader), loopExit(loopExit),
        functionExit(functionExit) {}

  BasicBlock *fallthrough;
  BasicBlock *loopHeader;
  BasicBlock *loopExit;
  BasicBlock *functionExit;
};

struct CompilationResult : CFGCodeGeneratorResult {
  bool success;
};

class Compiler {
public:
  using WidthScale = std::pair<int, int>;
  using StringPair = std::pair<String, String>;

  Compiler(duckdb::Connection *connection, const String &programText,
           size_t &udfCount)
      : connection(connection), programText(programText), udfCount(udfCount) {}

  void buildCursorLoopCFG(Function &function, const json &ast);
  void buildCFG(Function &function, const json &ast);

  CFGCodeGeneratorResult generateCode(const Function &function);

  BasicBlock *constructAssignmentCFG(const json &assignment, Function &function,
                                     List<json> &statements,
                                     const Continuations &continuations);

  BasicBlock *constructReturnCFG(const json &returnJson, Function &function,
                                 List<json> &statements,
                                 const Continuations &continuations);

  BasicBlock *constructIfCFG(const json &ifJson, Function &function,
                             List<json> &statements,
                             const Continuations &continuations);

  BasicBlock *constructIfElseCFG(const json &ifElseJson, Function &function,
                                 List<json> &statements,
                                 const Continuations &continuations);

  BasicBlock *constructIfElseIfCFG(const json &ifElseIfJson, Function &function,
                                   List<json> &statements,
                                   const Continuations &continuations);

  BasicBlock *constructWhileCFG(const json &whileJson, Function &function,
                                List<json> &statements,
                                const Continuations &continuations);

  BasicBlock *constructLoopCFG(const json &loopJson, Function &function,
                               List<json> &statements,
                               const Continuations &continuations);

  BasicBlock *constructForLoopCFG(const json &forJson, Function &function,
                                  List<json> &statements,
                                  const Continuations &continuations);

  BasicBlock *constructExitCFG(const json &exitJson, Function &function,
                               List<json> &statements,
                               const Continuations &continuations);

  BasicBlock *constructCursorLoopCFG(const json &cursorLoopJson,
                                     Function &function, List<json> &statements,
                                     const Continuations &continuations);

  BasicBlock *constructCFG(Function &function, List<json> &statements,
                           const Continuations &continuations);
  CompilationResult run();

  void optimize(Function &f);

  static constexpr std::size_t VECTOR_SIZE = 2048;
  static constexpr std::size_t DECIMAL_WIDTH = 18;
  static constexpr std::size_t DECIMAL_SCALE = 3;
  static constexpr char RETURN_TYPE_PATTERN[] =
      "RETURNS\\s+(\\w+ *(\\((\\d+, *)?\\d+\\))?)";
  static constexpr char FUNCTION_NAME_PATTERN[] =
      "CREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+(\\w+)";
  static constexpr char ASSIGNMENT_PATTERN[] = "\\:?\\=";

private:
  json parseJson() const;
  String getJsonExpr(const json &json);
  List<json> getJsonList(const json &body);
  VecOwn<Function> getFunctions() const;
  static StringPair unpackAssignment(const String &assignment);
  static Opt<WidthScale> getDecimalWidthScale(const String &type);
  static PostgresTypeTag getPostgresTag(const String &name);
  Type getTypeFromPostgresName(const String &name) const;
  String resolveTypeName(const String &type) const;

  duckdb::Connection *connection;
  String programText;
  YAMLConfig config;
  size_t &udfCount;
};
