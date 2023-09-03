#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "utils.hpp"

namespace duckdb
{
struct CodeInsertionPoint{
    vector<string> lines;
    /**
     * reference the string_function_count in FunctionInfo
    */
    int &vector_count;

    CodeInsertionPoint(int &vector_count) : vector_count(vector_count){}
    std::string new_vector(){
        return "tmp_vec"+std::to_string(vector_count++);
    }

    std::string to_string(){
        if(lines.empty())
            return "";
        return vec_join(lines, "\n")+"\n";
    }
};

struct BoundExpressionCodeGenerator{
public:
    template <typename T>
    static std::string Transpile(const T &exp, CodeInsertionPoint &insert) {
        return "Not implemented yet!";
    }
private:
    static void SpecialCaseHandler(const ScalarFunctionInfo &function_info, const std::vector<Expression *> &children, CodeInsertionPoint &insert, std::list<std::string> &args);
    static std::string CodeGenScalarFunctionInfo(const ScalarFunctionInfo &function_info, const std::vector<Expression *> &children, CodeInsertionPoint &insert);
};

class LogicalOperatorCodeGenerator : public LogicalOperatorVisitor
{
private:
    std::string res;
public:
    void VisitOperator(duckdb::LogicalOperator &op) override;
    void VisitOperator(duckdb::LogicalOperator &op, CodeInsertionPoint &insert);
    std::string run(Connection &con, const std::string &query, const std::vector<pair<const std::string &, const VarInfo &>> &vars, CodeInsertionPoint &insert);
};

template <>
std::string BoundExpressionCodeGenerator::Transpile(const Expression &exp, CodeInsertionPoint &insert);
} // namespace duckdb

