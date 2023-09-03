#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
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
    static void SpecialCaseHandler(const ScalarFunctionInfo &function_info, const vector<Expression *> &children, std::string &insert, std::vector<std::string> &args);
    static std::string CodeGenScalarFunctionInfo(const ScalarFunctionInfo &function_info, const vector<Expression *> &children, std::string &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor
{
private:
    std::string res;
public:
    void VisitOperator(duckdb::LogicalOperator &op) override;
    std::string run(Connection &con, const std::string &query, const std::vector<pair<const std::string &, const VarInfo &>> &vars);
};

template <>
std::string BoundExpressionCodeGenerator::Transpile(const Expression &exp, std::string &insert);
} // namespace duckdb

