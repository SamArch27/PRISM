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
#include "compiler_fmt/core.h"
#include <iostream>

// Simple class to print simple expressions
class ExpressionPrinter : public duckdb::LogicalOperatorVisitor {
private:
  std::ostream &os;

public:
  ExpressionPrinter(std::ostream &os) : os(os) {}

  void VisitOperatorHelper(duckdb::Expression &exp) { os << exp.ToString(); }

  void VisitOperatorHelper(const duckdb::LogicalOperator &op) {
    if (op.GetName() == "PROJECTION") {
      for (auto &exp : op.expressions) {
        VisitOperatorHelper(*exp);
      }
    }
  }

  void VisitOperator(duckdb::LogicalOperator &op) override {
    return VisitOperatorHelper(op);
  }

  void VisitOperator(const duckdb::LogicalOperator &op) {
    return VisitOperatorHelper(op);
  }
};