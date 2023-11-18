#pragma once

#include "types.hpp"
#include "utils.hpp"
#include <include/fmt/core.h>
#include <json.hpp>
#include <memory>
#include <regex>
#include <unordered_map>
#include <vector>

struct FunctionMetadata {

  FunctionMetadata(const std::string &functionName,
                   std::unique_ptr<Type> returnType)
      : functionName(functionName), returnType(std::move(returnType)) {}

  std::string functionName;
  std::unique_ptr<Type> returnType;
};

class Value;
class BasicBlock;
struct Variable;

/* Variable */

/*
struct Variable
{
    Variable(bool initialized, const std::string& type, const std::string& name)
: initialize(initialized), type(type), name(name) {};

    bool initialized;
    UDFType type;
    std::string name;
}
*/

/* Instructions */

class Instruction {};

class Assignment : public Instruction {
  Variable *lhs;
  Value *rhs;
};

class BranchInst : public Instruction {
  bool is_conditional;
  Value *cond;
  BasicBlock *true_target;
  BasicBlock *false_target;
};

class ReturnInst : public Instruction {
  Value *value;
};

/* BasicBlock */

class BasicBlock {
  std::vector<Instruction> instructions;
};

/* ControlFlowGraph */

class ControlFlowGraph {
  std::vector<BasicBlock> blocks;
  std::unordered_map<BasicBlock *, std::vector<BasicBlock *>> preds;
  std::unordered_map<BasicBlock *, std::vector<BasicBlock *>> succs;
};

/* Compiler */

using json = nlohmann::json;

class Compiler {
public:
  Compiler(const std::string &udfs, bool optimize = true);

  static constexpr std::size_t VECTOR_SIZE = 2048;
  static constexpr std::size_t DECIMAL_WIDTH = 18;
  static constexpr std::size_t DECIMAL_SCALE = 3;
  static constexpr char RETURN_TYPE_PATTERN[] =
      "RETURNS\\s+(\\w+ *(\\((\\d+, *)?\\d+\\))?)";
  static constexpr char FUNCTION_NAME_PATTERN[] =
      "CREATE\\s+FUNCTION\\s+(\\w+)";

  std::vector<std::string> getGeneratedCppFunctions();
  std::string getGeneratedPLpgSQLFunctions();

private:
  json parseJson(const std::string &udfs) const;
  std::vector<FunctionMetadata>
  getFunctionMetadata(const std::string &udfs) const;

  /*
  void addVariable(const std::string &t, const std::string &name)
  {
      auto v = Variable(t, name);
      variables.push_back(v);
      bindings.insert({name,&v});
  }
  */

  std::size_t variable_id = 0;
  std::unordered_map<std::string, Variable *> bindings;
  std::vector<Variable *> variables;
  ControlFlowGraph cfg;
};
