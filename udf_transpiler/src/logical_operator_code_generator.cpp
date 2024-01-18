#include "logical_operator_code_generator.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#define FMT_HEADER_ONLY
#include "duckdb/common/enums/expression_type.hpp"
#include "include/fmt/core.h"
#include "utils.hpp"
#include <iostream>

namespace duckdb {

string get_struct_name(const string &struct_operation) {
  return struct_operation.substr(0, struct_operation.find("::"));
}

std::string pow10String(int scale_difference){
  std::string ret = "1";
  for(int i = 0; i < scale_difference; i++){
    ret += "0";
  }
  return ret;
}

void decimalDecimalCastHandler(
    const ScalarFunctionInfo &function_info, string &function_name,
    std::vector<std::string> &template_args,
    const std::vector<Expression *> &children, CodeGenInfo &insert,
    std::list<std::string> &args){
  ASSERT(function_info.width_scale != std::make_pair((uint8_t)0, (uint8_t)0) && function_info.width_scale2 != std::make_pair((uint8_t)0, (uint8_t)0),
             "DecimalCastWrapper should have two width_scale sets.");
  // recreate the source and target type
  int source_scale = function_info.width_scale.second;
  int source_width = function_info.width_scale.first;
  int target_scale = function_info.width_scale2.second;
  int target_width = function_info.width_scale2.first;
  LogicalType source_type = LogicalType::DECIMAL(source_width, source_scale);
  LogicalType target_type = LogicalType::DECIMAL(target_width, target_scale);
  cout<<source_width<<", "<<source_scale<<endl;
  cout<<target_width<<", "<<target_scale<<endl;
  string source_physical = ScalarFunctionInfo::DecimalTypeToCppType(source_width, source_scale);
  string target_physical = ScalarFunctionInfo::DecimalTypeToCppType(target_width, target_scale);
  if(target_scale >= source_scale){
    // scale up
    idx_t scale_difference = target_scale - source_scale;
    auto multiply_factor = pow10String(scale_difference);
    idx_t res_width = target_width - scale_difference;
    if(source_width < res_width){
      function_name = fmt::format("{} * Cast::Operation", multiply_factor);
      template_args.push_back(source_physical);
      template_args.push_back(target_physical);
    }
    else{
      // DecimalScaleUpCheckOperator
      // evaluate the child first
      string newVar = insert.newTmpVar();
      insert.lines.push_back(fmt::format("auto {} = {};", newVar, args.front()));
      args.pop_front();
      args.push_front(newVar);
      string limit = pow10String(res_width);
      insert.lines.push_back(fmt::format("\
      if ({input} >= {limit} || {input} <= -{limit}){{\n\
        throw std::runtime_error(\"Numeric value out of range\");\n\
      }}\
      ", fmt::arg("input", newVar), fmt::arg("limit", limit)));
      function_name = fmt::format("{} * Cast::Operation", multiply_factor);
      template_args.push_back(source_physical);
      template_args.push_back(target_physical);
    }
  }
  else{ 
    // scale down
    idx_t scale_difference = source_scale - target_scale;
    auto multiply_factor = pow10String(scale_difference);
    idx_t res_width = target_width + scale_difference;
    if(source_width < res_width){
      // udf_todo: possibly a source of result difference
      function_name = fmt::format("1/{} * Cast::Operation", multiply_factor);
      template_args.push_back(source_physical);
      template_args.push_back(target_physical);
    }
    else{
      // DecimalScaleUpCheckOperator
      // evaluate the child first
      string newVar = insert.newTmpVar();
      insert.lines.push_back(fmt::format("auto {} = {};", newVar, args.front()));
      args.pop_front();
      args.push_front(newVar);
      string limit = pow10String(res_width);
      insert.lines.push_back(fmt::format("\
      if ({input} >= {limit} || {input} <= -{limit}){{\n\
        throw std::runtime_error(\"Numeric value out of range\");\n\
      }}\
      ", fmt::arg("input", newVar), fmt::arg("limit", limit)));
      function_name = fmt::format("1/{} * Cast::Operation", multiply_factor);
      template_args.push_back(source_physical);
      template_args.push_back(target_physical);
    }
  }
}

/**
 * currently only support vector front and back
 */
void BoundExpressionCodeGenerator::SpecialCaseHandler(
    const ScalarFunctionInfo &function_info, string &function_name,
    std::vector<std::string> &template_args,
    const std::vector<Expression *> &children, CodeGenInfo &insert,
    std::list<std::string> &args) {
  for (auto special_case : function_info.special_handling) {
    switch (special_case) {
    case ScalarFunctionInfo::BinaryNumericDivideWrapper:
      // udf_todo
      EXCEPTION("BinaryNumericDivideWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::BinaryZeroIsNullWrapper:
      // udf_todo
      EXCEPTION("BinaryZeroIsNullWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::BinaryZeroIsNullHugeintWrapper:
      // udf_todo
      EXCEPTION("BinaryZeroIsNullHugeintWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::VectorBackWrapper:
      args.push_back(insert.newVector());
      break;
    case ScalarFunctionInfo::VectorFrontWrapper:
      args.push_front(insert.newVector());
      break;
    case ScalarFunctionInfo::SubStringAutoLengthWrapper:
      // udf_todo
      EXCEPTION("SubStringAutoLengthWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::NumericCastWrapper:
      function_name = "NumericCastHelper";
      template_args.push_back(get_struct_name(function_info.cpp_name));
      break;
    case ScalarFunctionInfo::DecimalCastWrapper:
      ASSERT(function_info.width_scale != std::make_pair((uint8_t)0, (uint8_t)0),
             "DecimalCastWrapper should have width_scale set.");
      function_name = "DecimalCastHelper";
      template_args.push_back(get_struct_name(function_info.cpp_name));
      args.push_back(std::to_string(function_info.width_scale.first));
      args.push_back(std::to_string(function_info.width_scale.second));
      break;
    case ScalarFunctionInfo::ErrorCastWrapper:
      // udf_todo
      EXCEPTION("ErrorCastWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::DecimalVectorBackWrapper:
      // udf_todo
      EXCEPTION("DecimalVectorBackWrapper not implemented yet.");
      break;
    case ScalarFunctionInfo::DecimalDeciamlCastWrapper:
      // udf_todo
      decimalDecimalCastHandler(function_info, function_name, template_args, children, insert, args);
      break;
    default: break;
    }
  }

  return;
}

std::string BoundExpressionCodeGenerator::CodeGenScalarFunction(
    const ScalarFunctionInfo &function_info,
    const std::vector<Expression *> &children, CodeGenInfo &insert) {
  std::list<std::string> args;
  for (auto &child : children) {
    args.push_back(Transpile(*child, insert));
  }
  auto template_args = function_info.template_args;
  std::string ret = function_info.cpp_name;
  // change the meta info for special cases
  SpecialCaseHandler(function_info, ret, template_args, children, insert, args);

  // should only use the argument of the SpecialCaseHandler function, no more
  if (template_args.size() > 0) {
    ret += "<";
    for (auto &arg : template_args) {
      ret += arg + ", ";
    }
    ret = ret.substr(0, ret.size() - 2);
    ret += ">";
  }
  ret += "(";
  ret += list_join(args, ", ");
  ret += ")";
  return ret;
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundFunctionExpression &exp,
                                        CodeGenInfo &insert) {
  if (exp.function.has_scalar_funcition_info) {
    // ScalarFunction &function = exp.function;
    const ScalarFunctionInfo &function_info = exp.function.function_info;
    std::vector<Expression *> children(exp.children.size());
    for (size_t i = 0; i < exp.children.size(); i++) {
      children[i] = exp.children[i].get();
    }
    return CodeGenScalarFunction(function_info, children, insert);
  } else {
    std::list<std::string> args;
    for (auto &child : exp.children) {
      args.push_back(Transpile(*child, insert));
    }
    // ERROR(fmt::format("Function {} does not support transpilation yet.",
    // exp.function.name));
    return fmt::format("[{}({})->{}]", exp.GetName(), list_join(args, ", "),
                       exp.return_type.ToString());
    // throw Exception();
  }
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundComparisonExpression &exp,
                                        CodeGenInfo &insert) {
  switch (exp.GetExpressionType()) {
  case ExpressionType::COMPARE_EQUAL:
    return fmt::format("Equals::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  case ExpressionType::COMPARE_NOTEQUAL:
    return fmt::format("NotEquals::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  case ExpressionType::COMPARE_LESSTHAN:
    return fmt::format("LessThan::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  case ExpressionType::COMPARE_GREATERTHAN:
    return fmt::format("GreaterThan::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    return fmt::format("LessThanEquals::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    return fmt::format("GreaterThanEquals::Operation({}, {})",
                       Transpile(*exp.left, insert),
                       Transpile(*exp.right, insert));
    break;
  default: return exp.ToString(); break;
  }
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundConjunctionExpression &exp,
                                        CodeGenInfo &insert) {
  ASSERT(exp.children.size() == 2,
         "Conjunction expression should have 2 children.");
  switch (exp.GetExpressionType()) {
  case ExpressionType::CONJUNCTION_AND:
    return fmt::format("({} && {})", Transpile(*exp.children[0], insert),
                       Transpile(*exp.children[1], insert));
    break;
  case ExpressionType::CONJUNCTION_OR:
    return fmt::format("({} || {})", Transpile(*exp.children[0], insert),
                       Transpile(*exp.children[1], insert));
    break;
  default: ASSERT(false, "Conjunction expression should be AND or OR."); break;
  }
}

// udf_todo
template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundCastExpression &exp,
                                        CodeGenInfo &insert) {
  if (exp.bound_cast.has_function_info) {
    return CodeGenScalarFunction(exp.bound_cast.function_info,
                                     {exp.child.get()}, insert);
  }
  return fmt::format("[CAST {} AS {}]", Transpile(*exp.child, insert),
                     exp.return_type.ToString());
}

// udf_todo
template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundOperatorExpression &exp,
                                        CodeGenInfo &insert) {
  switch (exp.GetExpressionType()) {
  case ExpressionType::OPERATOR_NOT:
    ASSERT(exp.children.size() == 1, "NOT operator should have 1 child.");
    return fmt::format("(!{})", Transpile(*exp.children[0], insert));
  // case ExpressionType::OPERATOR_IS_NULL:
  //     have a {}_isnull for all function variables
  default:
    return fmt::format("[{}: {}]", exp.ToString(),
                       ExpressionTypeToString(exp.GetExpressionType()));
  }
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundConstantExpression &exp,
                                        CodeGenInfo &insert) {
  if (exp.value.type().IsNumeric() or
      exp.value.type() == LogicalType::BOOLEAN) {
    return exp.value.ToString();
  }
  return fmt::format("\"{}\"", exp.value.ToString());
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const BoundReferenceExpression &exp,
                                        CodeGenInfo &insert) {
  return get_var_name(exp.GetName());
}

template <>
std::string
BoundExpressionCodeGenerator::Transpile(const Expression &exp,
                                        CodeGenInfo &insert) {
  switch (exp.GetExpressionClass()) {
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
    return fmt::format("[{}: {}]", exp.ToString(),
                       ExpressionClassToString(exp.GetExpressionClass()));
    break;
  }
}

/**
 * 
 */
void LogicalOperatorCodeGenerator::VisitOperator(duckdb::LogicalOperator &op) {
  auto func = newFunction("tmp");
  CodeGenInfo insert(func);
  VisitOperator(op, insert);
  return;
}

/**
 * traverse the logical operator tree and generate the code into member res
 *
 */
void LogicalOperatorCodeGenerator::VisitOperator(duckdb::LogicalOperator &op,
                                                 CodeGenInfo &insert) {
  ASSERT(op.expressions.size() == 1,
         "Expression of the root operator should be 1.");
  insert.lines.clear();
  res = BoundExpressionCodeGenerator::Transpile(*(op.expressions[0]), insert);
  // std::cout<<ret<<std::endl;
  header = insert.toString();
  return;
}

std::string LogicalOperatorCodeGenerator::run(
    Connection &con, const std::string &query,
    const std::vector<pair<const std::string &, const VarInfo &>> &vars,
    CodeGenInfo &insert) {
  std::string create_stmt = "create table tmp1 (";
  for (auto &var : vars) {
    create_stmt += var.first + " " + var.second.type.get_duckdb_type() + ", ";
  }
  create_stmt = create_stmt.substr(0, create_stmt.size() - 2);
  create_stmt += ")";
  // cout<<create_stmt<<endl;
  auto create_res = con.Query(create_stmt);
  if (create_res->HasError()) {
    EXCEPTION(create_res->GetError());
  }
  // auto query_res = con.Query()
  auto context = con.context.get();
  context->config.enable_optimizer = false;
  // cout<<query<<endl;
  auto plan = context->ExtractPlan(query + " from tmp1");
  VisitOperator(*plan, insert);
  con.Query("drop table tmp1");
  // cout<<"query end"<<endl;
  return res;
}
} // namespace duckdb