#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "utils.hpp"

namespace duckdb
{
struct BoundExpressionCodeGenerator{
    template <typename T>
    static std::string Transpile(T &exp, std::string &insert) {
        return "Not implemented yet!";
    }

};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor
{
public:
    void VisitOperator(duckdb::LogicalOperator &op) override;
};

template <>
std::string BoundExpressionCodeGenerator::Transpile(Expression &exp, std::string &insert);
} // namespace duckdb

