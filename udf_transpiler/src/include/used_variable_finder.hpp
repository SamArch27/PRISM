#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utils.hpp"

namespace duckdb {
class UsedVariableFinder : LogicalOperatorVisitor {
public:
  UsedVariableFinder(String _targetTableName,
                     std::shared_ptr<Binder> _plannerBinder)
      : targetTableName(_targetTableName), plannerBinder(_plannerBinder) {}
      
  //! Update each operator of the plan
  void VisitOperator(LogicalOperator &op) override;
  //! Visit an expression and update its column bindings
  void VisitExpression(unique_ptr<Expression> *expression) override;
  Vec<String> getUsedVariables() { return usedVariables; }

private:
  String targetTableName;
  Vec<String> usedVariables;
  std::shared_ptr<Binder> plannerBinder;
};
} // namespace duckdb