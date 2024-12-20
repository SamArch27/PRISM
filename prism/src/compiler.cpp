#include "compiler.hpp"
#include "aggify_code_generator.hpp"
#include "aggify_pass.hpp"
#include "ast_to_cfg.hpp"
#include "cfg_code_generator.hpp"
#include "cfg_to_ast.hpp"
#include "dead_code_elimination.hpp"
#include "dominator_analysis.hpp"
#include "duckdb/main/connection.hpp"
#include "expression_printer.hpp"
#include "file.hpp"
#include "function.hpp"
#include "instruction_elimination.hpp"
#include "liveness_analysis.hpp"
#include "merge_regions.hpp"
#include "outlining.hpp"
#include "pg_query.h"
#include "predicate_analysis.hpp"
#include "query_motion.hpp"
#include "remove_unused_variable.hpp"
#include "ssa_construction.hpp"
#include "ssa_destruction.hpp"
#include "utils.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

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
  }
  codeRes.success = true;
  return codeRes;
}

CompilationResult Compiler::runOnFunction(Function &f) {
  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<SSADestructionPass>(), Make<AggressiveMergeRegionsPass>());
  ssaDestructionPipeline->runOnFunction(f);

  CompilationResult codeRes;
  auto res = generateCode(f);
  codeRes.code += res.code;
  codeRes.registration += res.registration;
  codeRes.success = true;
  return codeRes;
}

json Compiler::parseJson() const {
  auto result = pg_query_parse_plpgsql(programText.c_str());
  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    EXCEPTION(fmt::format("Error when parsing the plpgsql: {}",
                          result.error->message));
  }
  auto json = json::parse(result.plpgsql_funcs);
  pg_query_free_plpgsql_parse_result(result);
  return json;
}

void Compiler::optimize(Function &f) {

  auto ssaConstruction =
      Make<PipelinePass>(Make<MergeRegionsPass>(), Make<SSAConstructionPass>());

  auto coreOptimizations = Make<FixpointPass>(Make<PipelinePass>(
      Make<InstructionEliminationPass>(), Make<DeadCodeEliminationPass>()));

  auto aggifyPipeline = Make<PipelinePass>(Make<AggifyPass>(*this),
                                           Make<DeadCodeEliminationPass>());

  auto beforeOutliningPipeline = Make<FixpointPass>(Make<PipelinePass>(
      Make<QueryMotionPass>(), Make<InstructionEliminationPass>(),
      Make<DeadCodeEliminationPass>()));
  auto rightBeforeOutliningPipeline =
      Make<FixpointPass>(Make<DeadCodeEliminationPass>());

  auto outliningPipeline = Make<PipelinePass>(
      Make<OutliningPass>(*this), Make<AggressiveInstructionEliminationPass>(),
      Make<DeadCodeEliminationPass>());

  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<SSADestructionPass>(), Make<AggressiveMergeRegionsPass>());

  auto finalCleanUpPipeline =
      Make<PipelinePass>(Make<RemoveUnusedVariablePass>());

  // Convert to SSA
  ssaConstruction->runOnFunction(f);

  // Run the core optimizations
  coreOptimizations->runOnFunction(f);

  // Extract the predicates
  auto predicateAnalysis = Make<PredicateAnalysis>(f);
  predicateAnalysis->runAnalysis();
  auto hoistedPredicates = predicateAnalysis->getPredicates();
  for (auto &pred : hoistedPredicates) {
    std::cout << pred << std::endl;
  }

  // Now perform outlining
  aggifyPipeline->runOnFunction(f);
  beforeOutliningPipeline->runOnFunction(f);
  rightBeforeOutliningPipeline->runOnFunction(f);
  outliningPipeline->runOnFunction(f);

  // Finally get out of SSA
  ssaDestructionPipeline->runOnFunction(f);

  // clean up the variable list
  finalCleanUpPipeline->runOnFunction(f);

  // Compile the UDF to PL/SQL
  PLpgSQLGenerator plpgsqlGenerator(config);
  auto plpgsqlRes = plpgsqlGenerator.run(f);
  std::cout << "----------- PLpgSQL code start -----------\n";
  std::cout << plpgsqlRes.code << std::endl;
  std::cout << "----------- PLpgSQL code end-----------\n";
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
