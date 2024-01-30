#include "utils.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {
class UsedVariableFinder : LogicalOperatorVisitor {
public:
	UsedVariableFinder(String _targetTableName, std::shared_ptr<Binder> _plannerBinder)
		: targetTableName(_targetTableName), plannerBinder(_plannerBinder) {
	}

public:
	//! Update each operator of the plan
	void VisitOperator(LogicalOperator &op) override;
	//! Visit an expression and update its column bindings
	void VisitExpression(unique_ptr<Expression> *expression) override;

public:
	String targetTableName;
	Vec<String> usedVariables;
	std::shared_ptr<Binder> plannerBinder;
};
}