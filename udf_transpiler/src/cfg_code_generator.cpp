/**
 * @file cfg_code_generator.cpp
 * @brief Generate C++ code from CFG of PL/pgSQL
 */

#include "cfg_code_generator.hpp"
#include "logical_operator_code_generator.hpp"
#include "types.hpp"

String CFGCodeGenerator::createReturnValue(const String &retName,
                                           const Type *retType,
                                           const String &retValue) {

  if (dynamic_cast<const DecimalType *>(retType)) {
    auto width = dynamic_cast<const DecimalType *>(retType)->getWidth();
    auto scale = dynamic_cast<const DecimalType *>(retType)->getScale();
    return fmt::format("{} = Value::DECIMAL({}, {}, {})", retName, retValue,
                       width, scale);
  } else if (retType->isNumeric()) {
    return fmt::format("{} = Value::{}({})", retName, retType->getDuckDBType(),
                       retValue);
  } else if (retType->isBLOB()) {
    return fmt::format(
        "{0} = Value({1});\n{0}.GetTypeMutable() = LogicalType::{2}", retName,
        retValue, retType->getDuckDBType());
  } else {
    ERROR(fmt::format("Cannot create duckdb value from type {}",
                      retType->getDuckDBType()));
  }
}

/**
 * for each instruction in the basic block, generate the corresponding C++ code
 */
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb,
                                               const Function &func,
                                               CodeGenInfo &function_info) {
  String code;
  code += fmt::format("/* ==== Basic block {} start ==== */\n", bb->getLabel());
  code += fmt::format("{}:\n", bb->getLabel());

  if (bb->getLabel() == "exit") {
    code += fmt::format("return;\n");
    goto end;
  }
  for (auto &inst : bb->getInstructions()) {
    try {
      if (auto *assign = dynamic_cast<const Assignment *>(inst.get())) {
        if (toUpper(assign->getRHS()->getRawSQL()).find(" FROM ") !=
            String::npos) {
          ERROR("FROM clause should not be compiled.");
        }
        duckdb::LogicalOperatorCodeGenerator locg;
        auto *plan = assign->getRHS()->getLogicalPlan();
        locg.VisitOperator(*plan, function_info);
        auto [header, res] = locg.getResult();
        code += header;
        code += fmt::format("{} = {};\n", assign->getLHS()->getName(), res);
      } else if (auto *ret = dynamic_cast<const ReturnInst *>(inst.get())) {
        duckdb::LogicalOperatorCodeGenerator locg;
        auto *plan = ret->getExpr()->getLogicalPlan();
        locg.VisitOperator(*plan, function_info);
        auto [header, res] = locg.getResult();
        code += header;
        code += fmt::format(
            "{};\ngoto exit;\n",
            createReturnValue(config.function["return_name"].Scalar(),
                              func.getReturnType(), res));
      } else if (auto *br = dynamic_cast<const BranchInst *>(inst.get())) {
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
      } else if (dynamic_cast<const ExitInst *>(inst.get())) {
        // do nothing for ExitInst
      } else if (dynamic_cast<const PhiNode *>(inst.get())) {
        ERROR("Encountered a phi instruction which should have been removed "
              "before code-generation to C++!");

      } else {
        ERROR("Instruction does not fall into a specific type.");
      }
    } catch (const std::exception &e) {
      std::stringstream ss;
      ss << e.what() << "When compiling instruction: " << *inst;
      throw duckdb::ParserException(ss.str());
    }
  }
end:
  container.basicBlockCodes.push_back(code);
  return;
}

/**
 *
 */
String CFGCodeGenerator::extractVarFromChunk(const Function &func) {
  String code;
  const auto &args = func.getArguments();
  for (size_t i = 0; i < args.size(); i++) {
    auto &arg = args[i];
    String extract_data_from_value = fmt::format("v{}.GetValueUnsafe<{}>()", i,
                                                 arg->getType()->getCppType());
    code += fmt::format("{} {} = {};\n", arg->getType()->getCppType(),
                        arg->getName(), extract_data_from_value);
  }

  // just declare the local variables, they will be initialized in the basic
  // blocks also create the null indicator for each variable
  const auto &vars = func.getVariables();
  for (size_t i = 0; i < vars.size(); i++) {
    auto &var = vars[i];
    code +=
        fmt::format("{} {};\n", var->getType()->getCppType(), var->getName());
    code += fmt::format("bool {}_null = {};\n", var->getName(),
                        var->isNull() ? "true" : "false");
  }

  return code;
}

Vec<String> CFGCodeGenerator::run(const Function &func) {
  std::cout << fmt::format("Generating code for function {}",
                           func.getFunctionName())
            << std::endl;
  CodeGenInfo function_info(func);

  for (auto &bbUniq : func.getBasicBlocks()) {
    basicBlockCodeGenerator(bbUniq.get(), func, function_info);
  }
  String function_args = "";
  String arg_indexes = "";
  String subfunc_args = "";
  String fbody_args = "";
  Vec<String> check_null;

  int count = 0;
  for (const auto &arg : func.getArguments()) {
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
      fbody_args += fmt::format("Vector &tmp_vec{}, ", i);
    }
  }

  String vars_init = extractVarFromChunk(func);

  container.body = fmt::format(
      fmt::runtime(config.function["fbodyshell"].Scalar()),
      fmt::arg("function_name", func.getFunctionName()),
      fmt::arg("fbody_args", fbody_args),
      fmt::arg("check_null", joinVector(check_null, " or ")),
      fmt::arg("vars_init", vars_init),
      fmt::arg("action", joinVector(container.basicBlockCodes, "\n")));

  container.main =
      fmt::format(fmt::runtime(config.function["fshell2"].Scalar()),
                  fmt::arg("function_name", func.getFunctionName()),
                  fmt::arg("function_args", function_args),
                  fmt::arg("arg_indexes", arg_indexes),
                  fmt::arg("vector_create", vector_create),
                  fmt::arg("subfunc_args", subfunc_args));

  Vec<String> args_logical_types;
  for (auto &arg : func.getArguments()) {
    args_logical_types.push_back(arg->getType()->getDuckDBLogicalType());
  }
  container.registration = fmt::format(
      fmt::runtime(config.function["fcreate"].Scalar()),
      fmt::arg("function_name", func.getFunctionName()),
      fmt::arg("return_logical_type",
               func.getReturnType()->getDuckDBLogicalType()),
      fmt::arg("args_logical_types", joinVector(args_logical_types, ", ")));

  std::cout << container.body << std::endl;
  std::cout << container.main << std::endl;
  std::cout << container.registration << std::endl;

  return {container.body + "\n" + container.main, container.registration};
}