#pragma once
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utils.hpp"

struct CodeGenInfo {
  vector<string> lines;
  /**
   * reference the string_function_count in FunctionInfo
   */
  int vectorCount = 0;
//   int stringFunctionCount;

//   CodeGenInfo(int &vectorCount) : vectorCount(vectorCount) {}
  std::string newVector() {
    return "tmp_vec" + std::to_string(vectorCount++);
  }

  std::string toString() {
    if (lines.empty())
      return "";
    return vec_join(lines, "\n") + "\n";
  }
};

namespace duckdb {

struct BoundExpressionCodeGenerator {
public:
  template <typename T>
  static std::string Transpile(const T &exp, CodeGenInfo &insert) {
    return "Not implemented yet!";
  }

private:
  static void SpecialCaseHandler(const ScalarFunctionInfo &function_info,
                                 string &function_name,
                                 std::vector<std::string> &template_args,
                                 const std::vector<Expression *> &children,
                                 CodeGenInfo &insert,
                                 std::list<std::string> &args);
  static std::string
  CodeGenScalarFunctionInfo(const ScalarFunctionInfo &function_info,
                            const std::vector<Expression *> &children,
                            CodeGenInfo &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor {
private:
  // header is the code that should be inserted before the query
  std::string header;
  std::string res;

public:
  void VisitOperator(duckdb::LogicalOperator &op) override;
  void VisitOperator(duckdb::LogicalOperator &op, CodeGenInfo &insert);
  std::pair<std::string, std::string> getResult() {
    return {header, res};
  }
  std::string
  run(Connection &con, const std::string &query,
      const std::vector<pair<const std::string &, const VarInfo &>> &vars,
      CodeGenInfo &insert);
};

template <>
std::string BoundExpressionCodeGenerator::Transpile(const Expression &exp,
                                                    CodeGenInfo &insert);
} // namespace duckdb
