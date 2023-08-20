#include "logical_operator_code_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#define FMT_HEADER_ONLY
#include "include/fmt/core.h"
#include "duckdb/common/enums/expression_type.hpp"
#include <iostream>
#include "utils.hpp"

namespace duckdb {
template <>
std::string BoundExpressionCodeGenerator::Transpile(BoundFunctionExpression &exp, string &insert){
    if(exp.function.has_scalar_funcition_info){
        ScalarFunction &function = exp.function;
        ScalarFunctionInfo &function_info = function.function_info;
        std::string ret = function_info.cpp_name;
		if(function_info.template_args.size() > 0){
			ret += "<";
			for(auto &arg : function_info.template_args){
				ret += arg + ", ";
			}
			ret = ret.substr(0, ret.size()-2);
			ret += ">";
		}
        ret += "(";
        std::vector<std::string> args;
        for(auto &child : exp.children){
            args.push_back(BoundExpressionCodeGenerator::Transpile(*child, insert));
        }
        ret += vec_join(args, ", ");
        ret += ")";
        return ret;
    }
    else{
        throw Exception(fmt::format("Function {} does not support transpilation yet.", exp.function.name));
    }
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(BoundComparisonExpression &exp, std::string &insert){
    switch (exp.GetExpressionType())
    {
    case ExpressionType::COMPARE_EQUAL:
        return fmt::format("Equals::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    case ExpressionType::COMPARE_NOTEQUAL:
        return fmt::format("NotEquals::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    case ExpressionType::COMPARE_LESSTHAN:
        return fmt::format("LessThan::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    case ExpressionType::COMPARE_GREATERTHAN:
        return fmt::format("GreaterThan::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return fmt::format("LessThanEquals::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return fmt::format("GreaterThanEquals::Operation({}, {})", Transpile(*exp.left, insert), Transpile(*exp.right, insert));
        break;
    default:
        return exp.ToString();
        break;
    }
}

template<>
std::string BoundExpressionCodeGenerator::Transpile(BoundConjunctionExpression &exp, std::string &insert){
    ASSERT(exp.children.size() == 2, "Conjunction expression should have 2 children.");
    switch (exp.GetExpressionType())
    {
    case ExpressionType::CONJUNCTION_AND:
        return fmt::format("({} && {})", Transpile(*exp.children[0], insert), Transpile(*exp.children[1], insert));
        break;
    case ExpressionType::CONJUNCTION_OR:
        return fmt::format("({} || {})", Transpile(*exp.children[0], insert), Transpile(*exp.children[1], insert));
        break;
    default:
        ASSERT(false, "Conjunction expression should be AND or OR.");
        break;
    }
}

// udf_todo
template <>
std::string BoundExpressionCodeGenerator::Transpile(BoundCastExpression &exp, std::string &insert){
    return fmt::format("[CAST {} AS {}]", Transpile(*exp.child, insert), exp.return_type.ToString());
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(Expression &exp, std::string &insert){
    switch (exp.GetExpressionClass())
    {
    case ExpressionClass::BOUND_FUNCTION:
        return Transpile(exp.Cast<BoundFunctionExpression>(), insert);
        break;
    case ExpressionClass::BOUND_COMPARISON:
        return Transpile(exp.Cast<BoundComparisonExpression>(), insert);
        break;
    case ExpressionClass::BOUND_CONJUNCTION:
        return Transpile(exp.Cast<BoundConjunctionExpression>(), insert);
        break;
    case ExpressionClass::BOUND_CAST:
        return Transpile(exp.Cast<BoundCastExpression>(), insert);
        break;
    default:
        return fmt::format("[{}: {}]", exp.ToString(), ExpressionClassToString(exp.GetExpressionClass())) ;
        break;
    }
}

void LogicalOperatorCodeGenerator::VisitOperator(duckdb::LogicalOperator &op) {
    ASSERT(op.expressions.size() == 1, "Expression of the root operator should be 1.");
    std::string insert;
    std::string ret = BoundExpressionCodeGenerator::Transpile(*(op.expressions[0]), insert);
    std::cout<<ret<<std::endl;
}
} // namespace duckdb