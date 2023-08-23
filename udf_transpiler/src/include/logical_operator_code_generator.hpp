#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "utils.hpp"

namespace duckdb
{
struct BoundExpressionCodeGenerator{
public:
    template <typename T>
    static std::string Transpile(const T &exp, std::string &insert) {
        return "Not implemented yet!";
    }
private:
    static std::string CodeGenScalarFunctionInfo(const ScalarFunctionInfo &function_info, const vector<Expression *> &children, std::string &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor
{
public:
    void VisitOperator(duckdb::LogicalOperator &op) override;
};

template <>
std::string BoundExpressionCodeGenerator::Transpile(const Expression &exp, std::string &insert);
} // namespace duckdb

