/**
 * @file aggify_code_generator.cpp
 */

#include "aggify_code_generator.hpp"
#include "compiler_fmt/args.h"
#include "compiler_fmt/core.h"
#include "compiler_fmt/format.h"
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

AggifyCodeGeneratorResult AggifyCodeGenerator::run(
    Function &f, const json &ast, Vec<const Variable *> cursorVars,
    Vec<const Variable *> usedVars, const Variable *retVariable, size_t id) {

  String name = f.getFunctionName();
  String code;
  String varyingFuncTemplate = config.aggify["varyingFuncTemplate"].Scalar();
  Vec<String> inputDependentComps(20);

  String stateDefinition, argInit, argStore, operationArgs, operationNullArgs,
      varInit, createValue, destroyValue;
  String inputTypes, inputLogicalTypes;
  size_t count = 0;

  for (auto *usedVar : usedVars) {
    // all the c(s) in the template file

    if (std::find(cursorVars.begin(), cursorVars.end(), usedVar) ==
        cursorVars.end()) {
      // not cursor variable, is state variable
      // cursor variable: variable that is assigned a value in every iteration
      // state variable: variable that reused across iterations
      stateDefinition +=
          fmt::format(fmt::runtime(config.aggify["stateDefinition"].Scalar()),
                      fmt::arg("type", usedVar->getType().getCppType()),
                      fmt::arg("name", usedVar->getName()));

      varInit += fmt::format(fmt::runtime(config.aggify["varInit"].Scalar()),
                             fmt::arg("name", usedVar->getName()),
                             fmt::arg("i", count));

      argInit += fmt::format(fmt::runtime(config.aggify["argInit"].Scalar()),
                             fmt::arg("name", usedVar->getName()));

      argStore += fmt::format(fmt::runtime(config.aggify["argStore"].Scalar()),
                              fmt::arg("name", usedVar->getName()),
                              fmt::arg("i", count));

      createValue +=
          fmt::format(fmt::runtime(config.aggify["createValue"].Scalar()),
                      fmt::arg("name", usedVar->getName()));

      destroyValue +=
          fmt::format(fmt::runtime(config.aggify["destroyValue"].Scalar()),
                      fmt::arg("name", usedVar->getName()));

      operationArgs += fmt::format(
          fmt::runtime(config.aggify["operationArg"].Scalar()),
          fmt::arg("i", count), fmt::arg("name", usedVar->getName() + "_arg"));

    } else {
      // is custom aggregate argument
      operationArgs += fmt::format(
          fmt::runtime(config.aggify["operationArg"].Scalar()),
          fmt::arg("i", count), fmt::arg("name", usedVar->getName()));
    }

    for (std::size_t i = 0; i < inputDependentComps.size(); i++) {
      inputDependentComps[i] += fmt::format(
          fmt::runtime(config.aggify["c" + std::to_string(i + 1)].Scalar()),
          fmt::arg("i", count));
    }

    operationNullArgs +=
        fmt::format(fmt::runtime(config.aggify["operationNullArg"].Scalar()),
                    fmt::arg("i", count), fmt::arg("name", usedVar->getName()));

    inputTypes += fmt::format(
        fmt::runtime(config.aggify["inputType"].Scalar()), fmt::arg("i", count),
        fmt::arg("type", usedVar->getType().getCppType()));

    inputLogicalTypes += fmt::format(
        fmt::runtime(config.aggify["inputLogicalType"].Scalar()),
        fmt::arg("i", count),
        fmt::arg("type", usedVar->getType().getDuckDBLogicalTypeStr()));

    count++;
  }
  for (std::size_t i = 0; i < inputDependentComps.size(); i++) {
    inputDependentComps[i] =
        inputDependentComps[i].substr(0, inputDependentComps[i].size() - 2);
  }

  operationArgs = operationArgs.substr(0, operationArgs.size() - 2);
  operationNullArgs = operationNullArgs.substr(0, operationNullArgs.size() - 2);

  inputTypes = inputTypes.substr(0, inputTypes.size() - 2);
  inputLogicalTypes = inputLogicalTypes.substr(0, inputLogicalTypes.size() - 2);

  // code gen the body
  CodeGenInfo function_info;
  for (auto &bbUniq : f) {
    if (bbUniq.getLabel() == "accumulateReturnBlock") {
      String blockCode =
          fmt::format("accumulateReturnBlock:\n{{{}\nreturn;}}", argStore);
      container.basicBlockCodes.push_back(blockCode);
      continue;
    }
    basicBlockCodeGenerator(&bbUniq, f, function_info);
  }

  String optionalDataChunk, dataChunkInit, tmpVecInit, optionalChunkReset;
  if (function_info.vectorCount > 0) {
    optionalDataChunk =
        fmt::format(fmt::runtime(config.aggify["optionalDataChunk"].Scalar()));
    dataChunkInit =
        fmt::format(fmt::runtime(config.aggify["dataChunkInit"].Scalar()),
                    fmt::arg("vectorCount", function_info.vectorCount));
    for (int i = 0; i < function_info.vectorCount; i++) {
      tmpVecInit +=
          fmt::format(fmt::runtime(config.aggify["tmpVecInit"].Scalar()),
                      fmt::arg("vecId", i));
    }
    optionalChunkReset =
        fmt::format(fmt::runtime(config.aggify["optionalChunkReset"].Scalar()));
  }

  String body = joinVector(container.basicBlockCodes, "\n");

  fmt::dynamic_format_arg_store<fmt::format_context> store;
  store.push_back(fmt::arg("id", id));
  for (std::size_t i = 0; i < inputDependentComps.size(); i++) {
    String c = "c" + std::to_string(i + 1);
    store.push_back(fmt::arg(c.c_str(), inputDependentComps[i]));
  }
  store.push_back(fmt::arg("optionalChunkReset", optionalChunkReset));

  code = fmt::vformat(fmt::runtime(varyingFuncTemplate).str, store);

  code += fmt::format(
      fmt::runtime(config.aggify["customAggregateTemplate"].Scalar()),
      fmt::arg("id", id), fmt::arg("c1", inputDependentComps[0]),
      fmt::arg("stateDefinition", stateDefinition),
      fmt::arg("createValue", createValue),
      fmt::arg("optionalDataChunk", optionalDataChunk),
      fmt::arg("dataChunkInit", dataChunkInit),
      fmt::arg("destroyValue", destroyValue), fmt::arg("argInit", argInit),
      fmt::arg("tmpVecInit", tmpVecInit),
      fmt::arg("operationArgs", operationArgs),
      fmt::arg("operationNullArgs", operationNullArgs),
      fmt::arg("varInit", varInit), fmt::arg("body", body),
      fmt::arg("returnVariable", retVariable->getName()));

  String registration = fmt::format(
      fmt::runtime(config.aggify["registration"].Scalar()), fmt::arg("id", id),
      fmt::arg("name", name), fmt::arg("inputTypes", inputTypes),
      fmt::arg("outputType", f.getReturnType().getCppType()),
      fmt::arg("inputLogicalTypes", inputLogicalTypes),
      fmt::arg("outputLogicalType",
               f.getReturnType().getDuckDBLogicalTypeStr()));

  return {{code, registration}, name};
}
