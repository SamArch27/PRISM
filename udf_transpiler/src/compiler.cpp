#include "compiler.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include "utils.hpp"
#include "aggify_code_generator.hpp"
#include "aggify_dfa.hpp"
#include "ast_to_cfg.hpp"
#include "break_phi_interference.hpp"
#include "cfg_code_generator.hpp"
#include "copy_propagation.hpp"
#include "dead_code_elimination.hpp"
#include "dominator_dataflow.hpp"
#include "duckdb/main/connection.hpp"
#include "expression_printer.hpp"
#include "expression_propagation.hpp"
#include "file.hpp"
#include "fixpoint_pass.hpp"
#include "function.hpp"
#include "liveness_dataflow.hpp"
#include "merge_basic_blocks.hpp"
#include "pg_query.h"
#include "pipeline_pass.hpp"
#include "ssa_construction.hpp"
#include "ssa_destruction.hpp"
#include "cfg_to_ast.hpp"

std::ostream &operator<<(std::ostream &os, const LogicalPlan &expr) {
  ExpressionPrinter printer(os);
  printer.VisitOperator(expr);
  return os;
}

CompilationResult Compiler::run() {

  CompilationResult codeRes;
  auto functionNames = extractMatches(programText, FUNCTION_NAME_PATTERN, 1);
  auto returnTypes = extractMatches(programText, RETURN_TYPE_PATTERN, 1);
  ASSERT(returnTypes.size() >= functionNames.size(),
         "Return type not specified for all functions");
  ASSERT(functionNames.size() >= returnTypes.size(),
         "Function name not specified for all functions");

  auto asts = parseJson();
  VecOwn<Function> functions;

  AstToCFG astToCFG(conn, programText);
  for (std::size_t i = 0; i < functionNames.size(); ++i) {
    auto ast = asts[i];
    auto function =
        astToCFG.createFunction(ast, functionNames[i], returnTypes[i]);
    functions.emplace_back(std::move(function));
  }

  for (auto &f : functions) {
    optimize(*f);
    std::cout << *f << std::endl;
    auto res = generateCode(*f);
    codeRes.code += res.code;
    codeRes.registration += res.registration;
  }
  codeRes.success = true;
  return codeRes;
}

json Compiler::parseJson() const {
  auto result = pg_query_parse_plpgsql(programText.c_str());
  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    ERROR(fmt::format("Error when parsing the plpgsql: {}",
                      result.error->message));
  }
  auto json = json::parse(result.plpgsql_funcs);
  pg_query_free_plpgsql_parse_result(result);
  return json;
}

void Compiler::optimize(Function &f) {
  auto corePasses = Make<FixpointPass>(Make<PipelinePass>(
      Make<MergeBasicBlocksPass>(), Make<CopyPropagationPass>(),
      Make<ExpressionPropagationPass>(), Make<DeadCodeEliminationPass>()));

  // TODO: Add cleanup to the BreakPhiInterferencePass to eliminate copies

  auto pipeline = Make<PipelinePass>(
      Make<MergeBasicBlocksPass>(), Make<SSAConstructionPass>(),
      std::move(corePasses), Make<BreakPhiInterferencePass>(),
      Make<SSADestructionPass>()
      );

  // std::cout << f << std::endl;
  // drawGraph(f.getCFGString(), "begin");
  // drawGraph(f.getRegionString(), "region");

  // pipeline->runOnFunction(f);

  
  PLpgSQLGenerator plpgsqlGenerator(config);
  auto plpgsqlRes = plpgsqlGenerator.run(f);
  COUT<<"----------- PLpgSQL code start -----------\n";
  COUT<<plpgsqlRes.code<<ENDL;
  COUT<<"----------- PLpgSQL code end-----------\n";
  // std::cout << f << std::endl;
  // drawGraph(f.getCFGString(), "end");
}

/**
 * Generate code for a function
 * Initialize a CFGCodeGenerator and run it through the function
 */
CFGCodeGeneratorResult Compiler::generateCode(const Function &func) {

  CFGCodeGenerator codeGenerator(config);
  auto res = codeGenerator.run(func);
  return res;
}
