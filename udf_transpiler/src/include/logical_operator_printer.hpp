#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#define FMT_HEADER_ONLY
#include "include/fmt/core.h"
#include "duckdb/common/enums/expression_type.hpp"
#include <iostream>

namespace udf{
class LogicalOperatorPrinter : public duckdb::LogicalOperatorVisitor {
private:
    // duckdb::LogicalOperator *op;
public:
    LogicalOperatorPrinter(){
        // do nothing
    }

    std::string bound_function_to_string(duckdb::BoundFunctionExpression &exp){
        std::string result = fmt::format(fmt::runtime("{}(exp: {})"), exp.ToString(), duckdb::ExpressionClassToString(exp.GetExpressionClass()));
        if(exp.function.has_scalar_funcition_info){
            result += fmt::format(fmt::runtime(" -> {}"), exp.function.function_info.str());
        }
        return result;
    }

    void VisitOperatorHelper(duckdb::Expression &exp, int indent){
        switch (exp.GetExpressionClass())
        {
        case duckdb::ExpressionClass::BOUND_FUNCTION:
            std::cout<<fmt::format(fmt::runtime("{:<{}}{}(exp: {}) -> {}"), "", indent, exp.ToString(), duckdb::ExpressionClassToString(exp.GetExpressionClass()), 
                                    bound_function_to_string(exp.Cast<duckdb::BoundFunctionExpression>()))<<std::endl;
            for(auto &child : exp.Cast<duckdb::BoundFunctionExpression>().children){
                VisitOperatorHelper(*child, indent + 4);
            }
            break;
        default:
            std::cout<<fmt::format(fmt::runtime("{:<{}}{}(exp: {})"), "", indent, exp.ToString(), duckdb::ExpressionClassToString(exp.GetExpressionClass()))<<std::endl;
            break;
        }
    }

    void VisitOperatorHelper(duckdb::LogicalOperator &op, int indent) {
        std::cout<<fmt::format(fmt::runtime("{:<{}}{}(logical)"), "", indent, op.GetName())<<std::endl;
        for(auto &exp : op.expressions){
            VisitOperatorHelper(*exp, indent);
        }
        for(auto &child : op.children){
            // child->expressions[0]->Print()
            VisitOperatorHelper(*child, indent + 4);
        }
    }

    void VisitOperator(duckdb::LogicalOperator &op) override {
        return VisitOperatorHelper(op, 0);
    }
    // void VisitExpression(duckdb::unique_ptr<duckdb::Expression> *expression) override;
};
}