#pragma once

#include "compiler_fmt/core.h"
#include "function_pass.hpp"
#include "utils.hpp"

using json = nlohmann::json;

struct Continuations {
  Continuations(BasicBlock *fallthrough, BasicBlock *loopHeader,
                BasicBlock *loopExit)
      : fallthrough(fallthrough), loopHeader(loopHeader), loopExit(loopExit) {}

  BasicBlock *fallthrough;
  BasicBlock *loopHeader;
  BasicBlock *loopExit;
};

using StringPair = Pair<String, String>;
using WidthScale = Pair<int, int>;

class Region;

class AstToCFG {
public:
  AstToCFG(duckdb::Connection *conn, const String &programText)
      : conn(conn), programText(programText) {}
  Own<Function> createFunction(const json &ast, const String &name,
                               const String &returnType);

private:
  static constexpr char MAGIC_HEADER[] = "PLpgSQL_function";
  static constexpr char ASSIGNMENT_PATTERN[] = "\\:?\\=";

  void buildCursorLoopCFG(Function &function, const json &ast);
  void buildCFG(Function &function, const json &ast);

  Own<Region> getFallthroughRegion(bool attachFallthrough,
                                   Own<Region> fallthrough);

  Own<Region> constructAssignmentCFG(const json &assignment, Function &function,
                                     List<json> &statements,
                                     const Continuations &continuations,
                                     bool attachFallthrough);

  Own<Region> constructReturnCFG(const json &returnJson, Function &function,
                                 List<json> &statements,
                                 const Continuations &continuations,
                                 bool attachFallthrough);

  Own<Region> constructIfCFG(const json &ifJson, Function &function,
                             List<json> &statements,
                             const Continuations &continuations,
                             bool attachFallthrough);

  Own<Region> constructElseCFG(const json &ifElseJson, Function &function,
                               List<json> &statements,
                               const Continuations &continuations,
                               bool attachFallthrough);

  Own<Region> constructIfElseIfCFG(const json &ifElseIfJson, Function &function,
                                   List<json> &statements,
                                   const Continuations &continuations,
                                   bool attachFallthrough);

  Own<Region> constructWhileCFG(const json &whileJson, Function &function,
                                List<json> &statements,
                                const Continuations &continuations,
                                bool attachFallthrough);

  Own<Region> constructLoopCFG(const json &loopJson, Function &function,
                               List<json> &statements,
                               const Continuations &continuations,
                               bool attachFallthrough);

  Own<Region> constructForLoopCFG(const json &forJson, Function &function,
                                  List<json> &statements,
                                  const Continuations &continuations,
                                  bool attachFallthrough);

  Own<Region> constructExitCFG(const json &exitJson, Function &function,
                               List<json> &statements,
                               const Continuations &continuations,
                               bool attachFallthrough);

  Own<Region> constructCursorLoopCFG(const json &cursorLoopJson,
                                     Function &function, List<json> &statements,
                                     const Continuations &continuations,
                                     bool attachFallthrough);

  Own<Region> constructCFG(Function &function, List<json> &statements,
                           const Continuations &continuations,
                           bool attachFallthrough);

  json parseJson() const;
  String getJsonExpr(const json &json);
  List<json> getJsonList(const json &body);
  static StringPair unpackAssignment(const String &assignment);
  static Opt<WidthScale> getDecimalWidthScale(const String &type);
  static PostgresTypeTag getPostgresTag(const String &name);
  Type getTypeFromPostgresName(const String &name) const;
  String resolveTypeName(const String &type) const;

  duckdb::Connection *conn;
  String programText;
};