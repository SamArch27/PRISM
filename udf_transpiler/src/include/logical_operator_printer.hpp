#pragma once
#include "duckdb/common/types/value.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#define FMT_HEADER_ONLY
#include "duckdb/common/enums/expression_type.hpp"
#include "include/fmt/core.h"
#include <iostream>

namespace udf {
class ExpressionPrinter : public duckdb::LogicalOperatorVisitor {
private:
  // duckdb::LogicalOperator *op;
public:
  ExpressionPrinter() {
    // do nothing
  }

  std::string bound_function_to_string(duckdb::BoundFunctionExpression &exp) {
    std::string result =
        fmt::format(fmt::runtime("{}[{}]"), exp.ToString(),
                    duckdb::ExpressionClassToString(exp.GetExpressionClass()));
    if (exp.function.has_scalar_funcition_info) {
      result +=
          fmt::format(fmt::runtime(" -> {}"), exp.function.function_info.str());
    }
    return result;
  }

  std::string
  bound_comparison_to_string(duckdb::BoundComparisonExpression &exp) {
    std::string result =
        fmt::format(fmt::runtime("{}[{}] -> {}"), exp.ToString(),
                    duckdb::ExpressionClassToString(exp.GetExpressionClass()),
                    duckdb::ExpressionTypeToString(exp.GetExpressionType()));
    return result;
  }

  void VistorOperatorHelper(duckdb::BoundFunctionExpression &exp, int indent) {
    std::cout << fmt::format(fmt::runtime("{:<{}}{}"), "", indent,
                             bound_function_to_string(exp))
              << std::endl;
    for (auto &child : exp.children) {
      VisitOperatorHelper(*child, indent + 4);
    }
  }

  void VistorOperatorHelper(duckdb::BoundComparisonExpression &exp,
                            int indent) {
    std::cout << fmt::format(fmt::runtime("{:<{}}{}"), "", indent,
                             bound_comparison_to_string(exp))
              << std::endl;
    VisitOperatorHelper(*exp.left, indent + 4);
    VisitOperatorHelper(*exp.right, indent + 4);
  }

  void VisitOperatorHelper(duckdb::Expression &exp, int indent) {
    switch (exp.GetExpressionClass()) {
    case duckdb::ExpressionClass::BOUND_FUNCTION:
      VistorOperatorHelper(exp.Cast<duckdb::BoundFunctionExpression>(), indent);
      break;
    case duckdb::ExpressionClass::BOUND_COMPARISON:
      VistorOperatorHelper(exp.Cast<duckdb::BoundComparisonExpression>(),
                           indent);
      break;
    default:
      std::cout << fmt::format(fmt::runtime("{:<{}}{}[{}]"), "", indent,
                               exp.ToString(),
                               duckdb::ExpressionClassToString(
                                   exp.GetExpressionClass()))
                << std::endl;
      break;
    }
  }

  void VisitOperatorHelper(duckdb::LogicalOperator &op, int indent) {
    std::cout << fmt::format(fmt::runtime("{:<{}}{}[logical]"), "", indent,
                             op.GetName())
              << std::endl;
    for (auto &exp : op.expressions) {
      VisitOperatorHelper(*exp, indent);
    }
    for (auto &child : op.children) {
      // child->expressions[0]->Print()
      VisitOperatorHelper(*child, indent + 4);
    }
  }

  void VisitOperator(duckdb::LogicalOperator &op) override {
    return VisitOperatorHelper(op, 0);
  }
  // void VisitExpression(duckdb::unique_ptr<duckdb::Expression> *expression)
  // override;
};
} // namespace udf