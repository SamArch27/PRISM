/**
 * @file aggify_code_generator.cpp
 */

#include "aggify_code_generator.hpp"
#include "compiler_fmt/args.h"
#include <set>

/**
 * find the column names in between the first select and from
 * split by comma
 */
Vec<String> AggifyCodeGenerator::getOrginalCursorLoopCol(const json &ast) {
  Vec<String> res;
  String query = ast["query"]["PLpgSQL_expr"]["query"].get<String>();
  query = toUpper(query);
  size_t fromPos = query.find(" FROM ");
  size_t selectPos = query.find("SELECT ");
  ASSERT(fromPos != String::npos && selectPos != String::npos,
         "Cannot find FROM or SELECT in query: " + query);
  String selectClause = query.substr(selectPos + 7, fromPos - selectPos - 7);
  // split by comma
  size_t start = 0;
  size_t end = selectClause.find(",");
  do {
    String col = selectClause.substr(start, end - start);
    // remove leading and trailing spaces
    col = col.substr(col.find_first_not_of(" "), col.find_last_not_of(" ") + 1);
    res.push_back(col);
    start = end + 1;
    end = selectClause.find(",", start);
  } while (end != String::npos);
  return res;
}

AggifyCodeGeneratorResult AggifyCodeGenerator::run(const Function &func,
                                                   const json &ast,
                                                   const AggifyDFA &dfaResult,
                                                   size_t id) {
  std::set<String> cursorVars;
  for (const json &vars : ast["var"]["PLpgSQL_row"]["fields"]) {
    cursorVars.insert(vars["name"]);
  }
  // except for cursor variables, assume now that all others variables are used
  // in cursor loop

  String code;
  String varyingFuncTemplate = config.aggify["varyingFuncTemplate"].Scalar();
  Vec<String> inputDependentComps(20);

  String stateDefition, operationArgs, operationNullArgs, varInit;
  String inputTypes, inputLogicalTypes;
  String funcArgs;
  size_t count = 0;
  size_t stateVarCount = 0;
  const auto &allBindings = func.getAllBindings();

  auto originalCursorLoopCols = getOrginalCursorLoopCol(ast);
  ASSERT(originalCursorLoopCols.size() == cursorVars.size(),
         "Cursor loop columns size does not match cursor variables size");

  for (const auto &p : allBindings) {
    // all the c(s) in the template file
    for (int i = 0; i < inputDependentComps.size(); i++) {
      inputDependentComps[i] += fmt::format(
          fmt::runtime(config.aggify["c" + std::to_string(i + 1)].Scalar()),
          fmt::arg("i", count));
    }

    operationArgs +=
        fmt::format(fmt::runtime(config.aggify["operationArg"].Scalar()),
                    fmt::arg("i", count), fmt::arg("name", p.first));

    operationNullArgs +=
        fmt::format(fmt::runtime(config.aggify["operationNullArg"].Scalar()),
                    fmt::arg("i", count), fmt::arg("name", p.first));

    inputTypes += fmt::format(
        fmt::runtime(config.aggify["inputType"].Scalar()), fmt::arg("i", count),
        fmt::arg("type", p.second->getType()->getCppType()));

    inputLogicalTypes += fmt::format(
        fmt::runtime(config.aggify["inputLogicalType"].Scalar()),
        fmt::arg("i", count),
        fmt::arg("type", p.second->getType()->getDuckDBLogicalType()));

    if (cursorVars.count(p.first) == 0) {
      // is state variable
      stateDefition +=
          fmt::format(fmt::runtime(config.aggify["stateDefition"].Scalar()),
                      fmt::arg("type", p.second->getType()->getCppType()),
                      fmt::arg("name", p.first));

      varInit += fmt::format(fmt::runtime(config.aggify["varInit"].Scalar()),
                             fmt::arg("name", p.first));

      funcArgs += p.first + ", ";

      stateVarCount++;
    } else {
      // is custom aggregate argument
      funcArgs += originalCursorLoopCols[count - stateVarCount] + ", ";
    }

    count++;
  }
  for (int i = 0; i < inputDependentComps.size(); i++) {
    inputDependentComps[i] =
        inputDependentComps[i].substr(0, inputDependentComps[i].size() - 2);
  }

  operationArgs = operationArgs.substr(0, operationArgs.size() - 2);
  operationNullArgs = operationNullArgs.substr(0, operationNullArgs.size() - 2);

  inputTypes = inputTypes.substr(0, inputTypes.size() - 2);
  inputLogicalTypes = inputLogicalTypes.substr(0, inputLogicalTypes.size() - 2);

  funcArgs = funcArgs.substr(0, funcArgs.size() - 2);

  // code gen the body
  CodeGenInfo function_info(func);
  for (auto &bbUniq : func.getBasicBlocks()) {
    basicBlockCodeGenerator(bbUniq.get(), func, function_info);
  }

  String body = joinVector(container.basicBlockCodes, "\n");

  fmt::dynamic_format_arg_store<fmt::format_context> store;
  store.push_back(fmt::arg("id", id));
  for (int i = 0; i < inputDependentComps.size(); i++) {
    String c = "c" + std::to_string(i + 1);
    store.push_back(fmt::arg(c.c_str(), inputDependentComps[i]));
  }

  code = fmt::vformat(fmt::runtime(varyingFuncTemplate).str, store);

  code += fmt::format(
      fmt::runtime(config.aggify["customAggregateTemplate"].Scalar()),
      fmt::arg("id", id), fmt::arg("c1", inputDependentComps[0]),
      fmt::arg("stateDefition", stateDefition),
      fmt::arg("operationArgs", operationArgs),
      fmt::arg("operationNullArgs", operationNullArgs),
      fmt::arg("varInit", varInit), fmt::arg("body", body));

  String registration = fmt::format(
      fmt::runtime(config.aggify["registration"].Scalar()), fmt::arg("id", id),
      fmt::arg("inputTypes", inputTypes),
      fmt::arg("outputType", dfaResult.getReturnVar()->getType()->getCppType()),
      fmt::arg("inputLogicalTypes", inputLogicalTypes),
      fmt::arg("outputLogicalType",
               dfaResult.getReturnVar()->getType()->getDuckDBLogicalType()));

  String customAggCaller = fmt::format(
      fmt::runtime(config.aggify["caller"].Scalar()), fmt::arg("id", id),
      fmt::arg("funcArgs", funcArgs),
      fmt::arg("returnVarName", dfaResult.getReturnVarName()),
      fmt::arg("cursorQuery",
               ast["query"]["PLpgSQL_expr"]["query"].get<String>()));
  // COUT << code << ENDL;
  return {
      {code, registration}, "custom_agg" + std::to_string(id), customAggCaller};
}
