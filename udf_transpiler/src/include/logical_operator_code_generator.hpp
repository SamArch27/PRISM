#pragma once
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "function.hpp"
#include "instructions.hpp"
#include "utils.hpp"

struct CodeGenInfo {
public:
  Vec<String> lines;
  /**
   * reference the string_function_count in FunctionInfo
   */
  int vectorCount = 0;

  int tmpVarCount = 0;

  String newVector() {
    // make sure it is does not already exist
    return "tmp_vec" + std::to_string(vectorCount++);
  }

  String toString() {
    if (lines.empty())
      return "";
    return joinVector(lines, "\n") + "\n";
  }

  String newTmpVar() { return "tmp_var" + std::to_string(tmpVarCount++); }
  String getNewestTmpVar() { return "tmp_var" + std::to_string(tmpVarCount - 1); }
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
                                 CodeGenInfo &insert, std::list<String> &args);
  static String CodeGenScalarFunction(const ScalarFunctionInfo &function_info,
                                      const Vec<Expression *> &children,
                                      CodeGenInfo &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor {
public:
  LogicalOperatorCodeGenerator() : LogicalOperatorVisitor() {}

  void VisitOperator(duckdb::LogicalOperator &op) override;
  void VisitOperator(const duckdb::LogicalOperator &op, CodeGenInfo &insert);
  std::pair<String, String> getResult() { return {header, res}; }

private:
  // header is the code that should be inserted before the query
  String header;
  String res;
};

template <>
String BoundExpressionCodeGenerator::Transpile(const Expression &exp,
                                               CodeGenInfo &insert);
} // namespace duckdb
