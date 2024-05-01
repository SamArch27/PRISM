#include "used_variable_finder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
void UsedVariableFinder::VisitOperator(LogicalOperator &op) {
  VisitOperatorChildren(op);
  VisitOperatorExpressions(op);
}

void UsedVariableFinder::VisitExpression(unique_ptr<Expression> *expression) {
  auto &expr = *expression;
  if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

    auto &bound_column_ref = expr->Cast<BoundColumnRefExpression>();

    auto res = plannerBinder->bind_context.GetMatchingBindings(
        bound_column_ref.GetName());

    if (res.size() > 1 and res.find(targetTableName) != res.end()) {
      ERROR("Unexpected multiple bindings for column " +
            bound_column_ref.GetName());
    }
    if (res.find(targetTableName) != res.end()) {
      usedVariables.push_back(bound_column_ref.GetName());
    }
  }
  else if (expr->expression_class == ExpressionClass::BOUND_REF) {
    auto &column_ref = expr->Cast<BoundReferenceExpression>();

    auto res = plannerBinder->bind_context.GetMatchingBindings(
        column_ref.GetName());

    if (res.size() > 1 and res.find(targetTableName) != res.end()) {
      ERROR("Unexpected multiple bindings for column " +
            column_ref.GetName());
    }
    if (res.find(targetTableName) != res.end()) {
      usedVariables.push_back(column_ref.GetName());
    }
  }

  VisitExpressionChildren(**expression);
}
} // namespace duckdb