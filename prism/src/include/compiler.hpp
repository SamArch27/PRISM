
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
  Compiler(duckdb::Connection *conn, const String &programText,
           const YAMLConfig &config, size_t &udfCount)
      : conn(conn), programText(programText), config(config),
        udfCount(udfCount) {}

  CompilationResult run();

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
  json parseJson() const;

  duckdb::Connection *conn;
  String programText;
  const YAMLConfig &config;
  size_t &udfCount;
};
