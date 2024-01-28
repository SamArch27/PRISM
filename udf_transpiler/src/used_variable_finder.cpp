#include "used_variable_finder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
// #include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {
void UsedVariableFinder::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void UsedVariableFinder::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = *expression;
    if(expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
        // cout << "Found bound column ref" << endl;
        auto &bound_column_ref = expr->Cast<BoundColumnRefExpression>();
        // if (bound_column_ref.GetName() == targetTableName) {
        //     usedVariables.push_back(bound_column_ref.GetName());
        // }
        // bound_column_ref.GetName();
        auto res = plannerBinder->bind_context.GetMatchingBindings(bound_column_ref.GetName());
        // cout << res.size() << endl;
        // for (auto &binding : res) {
        //     cout << binding << endl;
        // }
        if(res.size() > 1 and res.find(targetTableName) != res.end()){
            ERROR("Unexpected multiple bindings for column "+bound_column_ref.GetName());
        }
        if(res.find(targetTableName) != res.end()){
            usedVariables.push_back(bound_column_ref.GetName());
        }
        // cout<<bound_column_ref.binding.table_index<<" "<<bound_column_ref.binding.column_index<<endl;
    }
    // else{
    //     cout<<expr->GetName()<<(int)expr->expression_class<<endl;
    // }

	VisitExpressionChildren(**expression);
}
}