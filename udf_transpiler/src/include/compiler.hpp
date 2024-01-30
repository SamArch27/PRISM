
#pragma once

#include "cfg.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <functional>
#include <include/fmt/core.h>
#include <json.hpp>
#include <memory>
#include <optional>
#include <queue>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

/* Compiler */

using json = nlohmann::json;

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

struct CompilationResult {
  bool success;
  std::string code;
  std::string registration;
};

class Compiler {
public:
  using WidthScale = std::pair<int, int>;
  using StringPair = std::pair<std::string, std::string>;

  Compiler(duckdb::Connection *connection, const std::string &programText,
           size_t &udfCount)
      : connection(connection), programText(programText), udfCount(udfCount) {}

  void buildCursorLoopCFG(Function &function, const json &ast);
  void buildCFG(Function &function, const json &ast);

  std::vector<std::string> generateCode(const Function &function);

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

  static constexpr std::size_t VECTOR_SIZE = 2048;
  static constexpr std::size_t DECIMAL_WIDTH = 18;
  static constexpr std::size_t DECIMAL_SCALE = 3;
  static constexpr char RETURN_TYPE_PATTERN[] =
      "RETURNS\\s+(\\w+ *(\\((\\d+, *)?\\d+\\))?)";
  static constexpr char FUNCTION_NAME_PATTERN[] =
      "CREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+(\\w+)";
  static constexpr char ASSIGNMENT_PATTERN[] = "\\:?\\=";

private:
  void makeDuckDBContext(const Function &function);
  void destroyDuckDBContext();
  json parseJson() const;
  std::string getJsonExpr(const json &json);
  List<json> getJsonList(const json &body);
  Vec<Function> getFunctions() const;

  SelectExpression bindExpression(const Function &function,
                                  const std::string &expression);
  static StringPair unpackAssignment(const string &assignment);
  static Opt<WidthScale> getDecimalWidthScale(const std::string &type);
  static PostgresTypeTag getPostgresTag(const std::string &name);
  Own<Type> getTypeFromPostgresName(const std::string &name) const;
  std::string resolveTypeName(const std::string &type) const;

  duckdb::Connection *connection;
  std::string programText;
  YAMLConfig config;
  size_t &udfCount;
};
