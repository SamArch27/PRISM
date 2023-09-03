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
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#define FMT_HEADER_ONLY
#include "include/fmt/core.h"
#include "duckdb/common/enums/expression_type.hpp"
#include <iostream>
#include "utils.hpp"

namespace duckdb {
void BoundExpressionCodeGenerator::SpecialCaseHandler(const ScalarFunctionInfo &function_info, const vector<Expression *> &children, std::string &insert, std::vector<std::string> &args){
    for(auto special_case : function_info.special_handling){
        switch (special_case)
        {
        case ScalarFunctionInfo::BinaryNumericDivideWrapper:
            // udf_todo
            break;
        case ScalarFunctionInfo::BinaryZeroIsNullWrapper:
            // udf_todo
            break;
        case ScalarFunctionInfo::BinaryZeroIsNullHugeintWrapper:
            // udf_todo
            break;
        default:
            break;
        }
    }
    return;
}

std::string BoundExpressionCodeGenerator::CodeGenScalarFunctionInfo(const ScalarFunctionInfo &function_info, const vector<Expression *> &children, std::string &insert){
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
    for(auto &child : children){
        args.push_back(Transpile(*child, insert));
    }
    SpecialCaseHandler(function_info, children, insert, args);
    ret += vec_join(args, ", ");
    ret += ")";
    return ret;
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(const BoundFunctionExpression &exp, string &insert){
    if(exp.function.has_scalar_funcition_info){
        // ScalarFunction &function = exp.function;
        const ScalarFunctionInfo &function_info = exp.function.function_info;
        vector<Expression *> children(exp.children.size());
        for(size_t i = 0; i < exp.children.size(); i++){
            children[i] = exp.children[i].get();
        }
        return CodeGenScalarFunctionInfo(function_info, children, insert);
    }
    else{
        throw Exception(fmt::format("Function {} does not support transpilation yet.", exp.function.name));
    }
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(const BoundComparisonExpression &exp, std::string &insert){
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
std::string BoundExpressionCodeGenerator::Transpile(const BoundConjunctionExpression &exp, std::string &insert){
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
std::string BoundExpressionCodeGenerator::Transpile(const BoundCastExpression &exp, std::string &insert){
    if(exp.bound_cast.has_function_info){
        return CodeGenScalarFunctionInfo(exp.bound_cast.function_info, {exp.child.get()}, insert);
    }
    return fmt::format("[CAST {} AS {}]", Transpile(*exp.child, insert), exp.return_type.ToString());
}

// udf_todo
template <>
std::string BoundExpressionCodeGenerator::Transpile(const BoundOperatorExpression &exp, std::string &insert){
    switch(exp.GetExpressionType()){
    case ExpressionType::OPERATOR_NOT:
        ASSERT(exp.children.size() == 1, "NOT operator should have 1 child.");
        return fmt::format("(!{})", Transpile(*exp.children[0], insert));
    // case ExpressionType::OPERATOR_IS_NULL:
    //     have a {}_isnull for all function variables
    default:
        return fmt::format("[{}: {}]", exp.ToString(), ExpressionTypeToString(exp.GetExpressionType()));
    }
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(const BoundConstantExpression &exp, std::string &insert){
    if(exp.value.type().IsNumeric() or exp.value.type() == LogicalType::BOOLEAN){
        return exp.value.ToString();
    }
    return fmt::format("\"{}\"", exp.value.ToString());
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(const BoundReferenceExpression &exp, std::string &insert){
    return exp.GetName();
}

template <>
std::string BoundExpressionCodeGenerator::Transpile(const Expression &exp, std::string &insert){
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
    case ExpressionClass::BOUND_OPERATOR:
        return Transpile(exp.Cast<BoundOperatorExpression>(), insert);
        break;
    case ExpressionClass::BOUND_CONSTANT:
        return Transpile(exp.Cast<BoundConstantExpression>(), insert);
        break;
    case ExpressionClass::BOUND_REF:
        return Transpile(exp.Cast<BoundReferenceExpression>(), insert);
        break;
    default:
        return fmt::format("[{}: {}]", exp.ToString(), ExpressionClassToString(exp.GetExpressionClass())) ;
        break;
    }
}

/**
 * traverse the logical operator tree and generate the code into member res
 *
*/
void LogicalOperatorCodeGenerator::VisitOperator(duckdb::LogicalOperator &op) {
    ASSERT(op.expressions.size() == 1, "Expression of the root operator should be 1.");
    std::string insert;
    res = BoundExpressionCodeGenerator::Transpile(*(op.expressions[0]), insert);
    // std::cout<<ret<<std::endl;
    return;
}

std::string LogicalOperatorCodeGenerator::run(Connection &con, const std::string &query, const std::vector<pair<const std::string &, const VarInfo &>> &vars){
    std::string create_stmt = "create table tmp1 (";
    for(auto &var : vars){
        create_stmt += var.first + " " + var.second.type.duckdb_type + ", ";
    }
    create_stmt = create_stmt.substr(0, create_stmt.size()-2);
    create_stmt += ")";
    // cout<<create_stmt<<endl;
    auto create_res = con.Query(create_stmt);
    if(create_res->HasError()){
        EXCEPTION(create_res->GetError());
    }
    // auto query_res = con.Query()
    auto context = con.context.get();
    context->config.enable_optimizer = false;
    // cout<<query<<endl;
    auto plan = context->ExtractPlan(query+" from tmp1");
    VisitOperator(*plan);
    con.Query("drop table tmp1");
    // cout<<"query end"<<endl;
    return res;
}
} // namespace duckdb