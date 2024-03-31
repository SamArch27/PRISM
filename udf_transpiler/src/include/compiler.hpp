
#pragma once

#include "compiler_fmt/core.h"
#include "duckdb/planner/logical_operator.hpp"
#include "instructions.hpp"
#include "types.hpp"
#include "utils.hpp"

#include "cfg_code_generator.hpp"
#include "fixpoint_pass.hpp"
#include "function_pass.hpp"
#include "pipeline_pass.hpp"

using json = nlohmann::json;

struct CompilationResult : CFGCodeGeneratorResult {
  bool success;
};

class Compiler {
public:
  Compiler(Map<String, bool> optimizerPassOnMap, duckdb::Connection *conn,
           const String &programText, const YAMLConfig &config,
           size_t &udfCount)
      : optimizerPassOnMap(optimizerPassOnMap), conn(conn),
        programText(programText), config(config), udfCount(udfCount) {}

  CompilationResult run();

  bool passOn(const String &passName) const {
    return optimizerPassOnMap.count(passName) > 0 &&
           optimizerPassOnMap.at(passName);
  }

  void runPass(FunctionPass &pass, Function &f) {
    if (auto *pipelinePass = dynamic_cast<PipelinePass *>(&pass)) {
      auto &pipeline = pipelinePass->getPipeline();
      auto iter = pipeline.begin();
      while (iter != pipeline.end()) {
        auto &cur = *iter;
        if (!passOn(cur->getPassName())) {
          iter = pipeline.erase(iter);
        } else {
          ++iter;
        }
      }
      pass.runOnFunction(f);
    } else if (auto *fixpointPass = dynamic_cast<FixpointPass *>(&pass)) {
      runPass(fixpointPass->getPass(), f);
    } else {
      if (passOn(pass.getPassName())) {
        pass.runOnFunction(f);
      }
    }
  }

  CompilationResult runOnFunction(Function &f);

  CFGCodeGeneratorResult generateCode(const Function &function);

  void optimize(Function &f);

  inline size_t &getUdfCount() { return udfCount; }
  inline duckdb::Connection *getConnection() { return conn; }
  inline const YAMLConfig &getConfig() { return config; }

  static constexpr std::size_t VECTOR_SIZE = 2048;
  static constexpr std::size_t DECIMAL_WIDTH = 18;
  static constexpr std::size_t DECIMAL_SCALE = 3;
  static constexpr char RETURN_TYPE_PATTERN[] =
      "RETURNS\\s+(\\w+ *(\\((\\d+, *)?\\d+\\))?)";
  static constexpr char FUNCTION_NAME_PATTERN[] =
      "CREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+(\\w+)";

private:
  Map<String, bool> optimizerPassOnMap;
  json parseJson() const;

  duckdb::Connection *conn;
  String programText;
  const YAMLConfig &config;
  size_t &udfCount;
};
