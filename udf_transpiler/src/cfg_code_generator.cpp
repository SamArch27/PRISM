/**
 * @file cfg_code_generator.cpp
 * @brief Generate C++ code from CFG of PL/pgSQL
 */

#include "cfg_code_generator.hpp"
#include "logical_operator_code_generator.hpp"
#include "types.hpp"

String CFGCodeGenerator::createReturnValue(const String &retName,
                                           const Type &retType,
                                           const String &retValue) {

  if (retType.isDecimal()) {
    auto width = retType.getWidth();
    auto scale = retType.getScale();
    return fmt::format("{} = Value::DECIMAL({}, {}, {})", retName, retValue,
                       *width, *scale);
  } else if (retType.isNumeric()) {
    return fmt::format("{} = Value::{}({})", retName, retType.getDuckDBType(),
                       retValue);
  } else if (retType.isBlob()) {
    return fmt::format(
        "{0} = Value({1});\n{0}.GetTypeMutable() = LogicalType::{2}", retName,
        retValue, retType.getDuckDBType());
  } else {
    ERROR(fmt::format("Cannot create duckdb value from type {}",
                      retType.getDuckDBType()));
  }
}

/**
 * for each instruction in the basic block, generate the corresponding C++ code
 */
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb,
                                               const Function &f,
                                               CodeGenInfo &function_info) {
  String code;
  code += fmt::format("/* ==== Basic block {} start ==== */\n", bb->getLabel());
  code += fmt::format("{}:\n", bb->getLabel());

  for (auto &inst : *bb) {
    try {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        if (assign->getRHS()->isSQLExpression()) {
          ERROR("FROM clause should not be compiled.");
        }
        duckdb::LogicalOperatorCodeGenerator locg;
        auto *plan = assign->getRHS()->getLogicalPlan();
        locg.VisitOperator(*plan, function_info);
        auto [header, res] = locg.getResult();
        code += header;
        code += fmt::format("{} = {};\n", assign->getLHS()->getName(), res);
      } else if (auto *ret = dynamic_cast<const ReturnInst *>(&inst)) {
        duckdb::LogicalOperatorCodeGenerator locg;
        auto *plan = ret->getExpr()->getLogicalPlan();
        locg.VisitOperator(*plan, function_info);
        auto [header, res] = locg.getResult();
        code += header;
        code += fmt::format(
            "{};\nreturn;\n",
            createReturnValue(config.function["return_name"].Scalar(),
                              f.getReturnType(), res));
      } else if (auto *br = dynamic_cast<const BranchInst *>(&inst)) {
        if (br->isConditional()) {
          duckdb::LogicalOperatorCodeGenerator locg;
          auto *plan = br->getCond()->getLogicalPlan();
          locg.VisitOperator(*plan, function_info);
          auto [header, res] = locg.getResult();
          code += header;
          code += fmt::format("if({}) goto {};\n", res,
                              br->getIfTrue()->getLabel());
          code += fmt::format("goto {};\n", br->getIfFalse()->getLabel());
        } else {
          code += fmt::format("goto {};\n", br->getIfTrue()->getLabel());
        }
      } else if (dynamic_cast<const PhiNode *>(&inst)) {
        ERROR("Encountered a phi instruction which should have been removed "
              "before code-generation to C++!");

      } else {
        ERROR("Instruction does not fall into a specific type.");
      }
    } catch (const duckdb::Exception &e) {
      std::stringstream ss;
      ss << e.GetStackTrace(10) << "\n"
         << e.what() << "\n"
         << "When compiling instruction: " << "\n"
         << inst;
      throw duckdb::ParserException(ss.str());
    } catch (const std::exception &e) {
      std::stringstream ss;
      ss << e.what() << "When compiling instruction: " << "\n" << inst;
      throw duckdb::ParserException(ss.str());
    }
  }
  container.basicBlockCodes.push_back(code);
  return;
}

/**
 *
 */
String CFGCodeGenerator::extractVarFromChunk(const Function &func) {
  String code;

  std::size_t i = 0;
  for (const auto &arg : func.getArguments()) {
    String extract_data_from_value =
        fmt::format("v{}.GetValueUnsafe<{}>()", i, arg->getType().getCppType());
    code += fmt::format("{} {} = {};\n", arg->getType().getCppType(),
                        arg->getName(), extract_data_from_value);

    ++i;
  }

  // just declare the local variables, they will be initialized in the basic
  // blocks also create the null indicator for each variable
  i = 0;
  for (const auto &var : func.getVariables()) {
    code +=
        fmt::format("{} {};\n", var->getType().getCppType(), var->getName());
    code += fmt::format("bool {}_null = {};\n", var->getName(),
                        var->isNull() ? "true" : "false");
    ++i;
  }

  return code;
}

CFGCodeGeneratorResult CFGCodeGenerator::run(const Function &f) {
  CodeGenInfo function_info;

  for (auto &bbUniq : f) {
    basicBlockCodeGenerator(&bbUniq, f, function_info);
  }
  String function_args, arg_indexes, subfunc_args, subfunc_args_all_0,
      fbody_args;
  Vec<String> check_null;

  int count = 0;
  for (const auto &arg : f.getArguments()) {
    String name = arg->getName();
    function_args +=
        fmt::format(fmt::runtime(config.function["fargs2"].Scalar()),
                    fmt::arg("var_name", name), fmt::arg("i", count));
    function_args += "\n";

    arg_indexes +=
        fmt::format(fmt::runtime(config.function["argindex"].Scalar()),
                    fmt::arg("var_name", name));
    arg_indexes += "\n";

    subfunc_args +=
        fmt::format(fmt::runtime(config.function["subfunc_arg"].Scalar()),
                    fmt::arg("var_name", name));
    subfunc_args += ", ";

    subfunc_args_all_0 +=
        fmt::format(fmt::runtime(config.function["subfunc_arg_0"].Scalar()),
                    fmt::arg("var_name", name));
    subfunc_args_all_0 += ", ";

    fbody_args +=
        fmt::format(fmt::runtime(config.function["fbody_arg"].Scalar()),
                    fmt::arg("i", count), fmt::arg("var_name", name));
    fbody_args += ", ";

    check_null.push_back(name + "_null");

    // todo: add {}_null, result to the argument list
    count++;
  }

  String vector_create = "";
  if (function_info.vectorCount > 0) {
    vector_create =
        fmt::format(fmt::runtime(config.function["vector_create"].Scalar()),
                    fmt::arg("vector_count", function_info.vectorCount));
    for (int i = 0; i < function_info.vectorCount; i++) {
      subfunc_args += fmt::format("tmp_chunk.data[{}], ", i);
      subfunc_args_all_0 += fmt::format("tmp_chunk.data[{}], ", i);
      fbody_args += fmt::format("Vector &tmp_vec{}, ", i);
    }
  }

  String vars_init = extractVarFromChunk(f);

  container.body = fmt::format(
      fmt::runtime(config.function["fbodyshell"].Scalar()),
      fmt::arg("function_name", f.getFunctionName()),
      fmt::arg("fbody_args", fbody_args),
      fmt::arg("check_null",
               check_null.empty() ? "false" : joinVector(check_null, " or ")),
      fmt::arg("vars_init", vars_init),
      fmt::arg("action", joinVector(container.basicBlockCodes, "\n")));

  container.main =
      fmt::format(fmt::runtime(config.function["fshell2"].Scalar()),
                  fmt::arg("function_name", f.getFunctionName()),
                  fmt::arg("function_args", function_args),
                  fmt::arg("arg_indexes", arg_indexes),
                  fmt::arg("vector_create", vector_create),
                  fmt::arg("subfunc_args", subfunc_args),
                  fmt::arg("subfunc_args_all_0", subfunc_args_all_0));

  Vec<String> args_logical_types;
  for (auto &arg : f.getArguments()) {
    args_logical_types.push_back(arg->getType().getDuckDBLogicalType());
  }
  container.registration = fmt::format(
      fmt::runtime(config.function["fcreate"].Scalar()),
      fmt::arg("function_name", f.getFunctionName()),
      fmt::arg("return_logical_type", f.getReturnType().getDuckDBLogicalType()),
      fmt::arg("args_logical_types", joinVector(args_logical_types, ", ")));

  return {container.body + "\n" + container.main, container.registration};
}