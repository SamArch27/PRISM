/**
 * @file aggify_code_generator.cpp
 */

#include "aggify_code_generator.hpp"
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

Vec<String> AggifyCodeGenerator::run(const Function &func, const json &ast,
                                     const AggifyDFA &dfaResult, size_t id) {
  std::set<String> cursorVars;
  for (const json &vars : ast["var"]["PLpgSQL_row"]["fields"]) {
    cursorVars.insert(vars["name"]);
  }
  // except for cursor variables, assume now that all others variables are used
  // in cursor loop

  String code;
  String varyingFuncTemplate = config.aggify["varyingFuncTemplate"].Scalar();
  String c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16,
      c17, c18, c19, c20;
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
    c1 += fmt::format(fmt::runtime(config.aggify["c1"].Scalar()),
                      fmt::arg("i", count));
    c2 += fmt::format(fmt::runtime(config.aggify["c2"].Scalar()),
                      fmt::arg("i", count));
    c3 += fmt::format(fmt::runtime(config.aggify["c3"].Scalar()),
                      fmt::arg("i", count));
    c4 += fmt::format(fmt::runtime(config.aggify["c4"].Scalar()),
                      fmt::arg("i", count));
    c5 += fmt::format(fmt::runtime(config.aggify["c5"].Scalar()),
                      fmt::arg("i", count));
    c6 += fmt::format(fmt::runtime(config.aggify["c6"].Scalar()),
                      fmt::arg("i", count));
    c7 += fmt::format(fmt::runtime(config.aggify["c7"].Scalar()),
                      fmt::arg("i", count));
    c8 += fmt::format(fmt::runtime(config.aggify["c8"].Scalar()),
                      fmt::arg("i", count));
    c9 += fmt::format(fmt::runtime(config.aggify["c9"].Scalar()),
                      fmt::arg("i", count));
    c10 += fmt::format(fmt::runtime(config.aggify["c10"].Scalar()),
                       fmt::arg("i", count));
    c11 += fmt::format(fmt::runtime(config.aggify["c11"].Scalar()),
                       fmt::arg("i", count));
    c12 += fmt::format(fmt::runtime(config.aggify["c12"].Scalar()),
                       fmt::arg("i", count));
    c13 += fmt::format(fmt::runtime(config.aggify["c13"].Scalar()),
                       fmt::arg("i", count));
    c14 += fmt::format(fmt::runtime(config.aggify["c14"].Scalar()),
                       fmt::arg("i", count));
    c15 += fmt::format(fmt::runtime(config.aggify["c15"].Scalar()),
                       fmt::arg("i", count));
    c16 += fmt::format(fmt::runtime(config.aggify["c16"].Scalar()),
                       fmt::arg("i", count));
    c17 += fmt::format(fmt::runtime(config.aggify["c17"].Scalar()),
                       fmt::arg("i", count));

    c18 += fmt::format(fmt::runtime(config.aggify["c18"].Scalar()),
                       fmt::arg("i", count));

    c19 += fmt::format(fmt::runtime(config.aggify["c19"].Scalar()),
                       fmt::arg("i", count));

    c20 += fmt::format(fmt::runtime(config.aggify["c20"].Scalar()),
                       fmt::arg("i", count));

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
  c1 = c1.substr(0, c1.size() - 2);
  c2 = c2.substr(0, c2.size() - 2);
  c3 = c3.substr(0, c3.size() - 2);
  c4 = c4.substr(0, c4.size() - 2);
  c5 = c5.substr(0, c5.size() - 4);
  c7 = c7.substr(0, c7.size() - 4);
  c8 = c8.substr(0, c8.size() - 2);
  c9 = c9.substr(0, c9.size() - 2);
  c10 = c10.substr(0, c10.size() - 2);
  c11 = c11.substr(0, c11.size() - 2);
  c12 = c12.substr(0, c12.size() - 2);
  c14 = c14.substr(0, c14.size() - 2);
  c15 = c15.substr(0, c15.size() - 2);
  c16 = c16.substr(0, c16.size() - 2);
  c17 = c17.substr(0, c17.size() - 2);
  c18 = c18.substr(0, c18.size() - 2);
  c19 = c19.substr(0, c19.size() - 2);
  c20 = c20.substr(0, c20.size() - 2);

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

  String body = vector_join(container.basicBlockCodes, "\n");

  code = fmt::format(
      fmt::runtime(varyingFuncTemplate), fmt::arg("id", id), fmt::arg("c1", c1),
      fmt::arg("c2", c2), fmt::arg("c3", c3), fmt::arg("c4", c4),
      fmt::arg("c5", c5), fmt::arg("c6", c6), fmt::arg("c7", c7),
      fmt::arg("c8", c8), fmt::arg("c9", c9), fmt::arg("c10", c10),
      fmt::arg("c11", c11), fmt::arg("c12", c12), fmt::arg("c13", c13),
      fmt::arg("c14", c14), fmt::arg("c15", c15), fmt::arg("c16", c16),
      fmt::arg("c17", c17), fmt::arg("c18", c18), fmt::arg("c19", c19),
      fmt::arg("c20", c20));

  code += fmt::format(
      fmt::runtime(config.aggify["customAggregateTemplate"].Scalar()),
      fmt::arg("id", id), fmt::arg("c1", c1),
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
  std::cout << code << std::endl;
  return {code, registration, customAggCaller};
}
