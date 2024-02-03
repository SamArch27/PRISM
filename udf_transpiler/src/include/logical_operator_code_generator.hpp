#pragma once
#include "cfg.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utils.hpp"

struct CodeGenInfo {
public:
  CodeGenInfo(const Function &_func) : function(_func) {}
  const Function &function;
  Vec<String> lines;
  /**
   * reference the string_function_count in FunctionInfo
   */
  int vectorCount = 0;

  int tmpVarCount = 0;

  String newVector() {
    // make sure it is does not already exist
    String name = "tmp_vec" + std::to_string(vectorCount++);
    if (function.hasBinding(name))
      return newVector();
    return name;
  }

  String toString() {
    if (lines.empty())
      return "";
    return vectorJoin(lines, "\n") + "\n";
  }

  String newTmpVar() {
    String name = "tmp_var" + std::to_string(tmpVarCount++);
    if (function.hasBinding(name))
      return newTmpVar();
    return name;
  }
};

namespace duckdb {

struct BoundExpressionCodeGenerator {
public:
  template <typename T>
  static String Transpile(const T &exp, CodeGenInfo &insert) {
    return "Not implemented yet!";
  }

private:
  static void SpecialCaseHandler(const ScalarFunctionInfo &function_info,
                                 String &function_name,
                                 Vec<String> &template_args,
                                 const Vec<Expression *> &children,
                                 CodeGenInfo &insert,
                                 List<String> &args);
  static String
  CodeGenScalarFunction(const ScalarFunctionInfo &function_info,
                        const Vec<Expression *> &children,
                        CodeGenInfo &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor {
private:
  // header is the code that should be inserted before the query
  String header;
  String res;

public:
  void VisitOperator(duckdb::LogicalOperator &op) override;
  void VisitOperator(const duckdb::LogicalOperator &op, CodeGenInfo &insert);
  std::pair<String, String> getResult() { return {header, res}; }
};

template <>
String BoundExpressionCodeGenerator::Transpile(const Expression &exp,
                                                    CodeGenInfo &insert);
} // namespace duckdb
