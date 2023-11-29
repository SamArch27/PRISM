
#pragma once

#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <include/fmt/core.h>
#include <json.hpp>
#include <memory>
#include <optional>
#include <regex>
#include <unordered_map>
#include <utility>
#include <vector>

using Expression = duckdb::LogicalOperator;

class Variable {
public:
  Variable(const std::string &name, Own<Type> type)
      : name(name), type(std::move(type)) {}

  std::string getName() const { return name; }
  const Type *getType() const { return type.get(); }

  friend std::ostream &operator<<(std::ostream &os, const Variable &var) {
    var.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const { os << name << "::" << *type; }

private:
  std::string name;
  Own<Type> type;
};

class Instruction {
public:
  friend std::ostream &operator<<(std::ostream &os, const Instruction &inst) {
    inst.print(os);
    return os;
  }

protected:
  virtual void print(std::ostream &os) const = 0;
};

class Assignment : public Instruction {
public:
  Assignment(const Variable *var, Own<Expression> expr)
      : Instruction(), var(var), expr(std::move(expr)) {}

  const Variable *getVar() const { return var; }
  const Expression *getExpr() const { return expr.get(); }
  friend std::ostream &operator<<(std::ostream &os,
                                  const Assignment &assignment) {
    assignment.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const override {
    os << var << " = \n" << expr->ToString();
  }

private:
  const Variable *var;
  Own<Expression> expr;
};

class BasicBlock {
public:
  BasicBlock(const std::string &label) : label(label) {}

  void addInstruction(Instruction *inst) { instructions.push_back(inst); }

private:
  std::string label;
  Vec<Instruction *> instructions;
};

class Function {
public:
  Function(const std::string &functionName, Own<Type> returnType)
      : functionName(functionName), returnType(std::move(returnType)) {}

  void addArgument(const std::string &name, Own<Type> type) {
    bindings.emplace(name, type.get());
    arguments.emplace_back(Make<Variable>(name, std::move(type)));
  }

  void addVariable(const std::string &name, Own<Type> type,
                   Own<Expression> expr) {
    bindings.emplace(name, type.get());
    variables.emplace_back(Make<Variable>(name, std::move(type)));
    declarations.emplace_back(
        Make<Assignment>(variables.back().get(), std::move(expr)));
  }

  const VecOwn<Variable> &getArguments() const { return arguments; }
  const VecOwn<Variable> &getVariables() const { return variables; }
  const VecOwn<Assignment> &getDeclarations() const { return declarations; }

  std::string getFunctionName() const { return functionName; }
  const Type *getReturnType() const { return returnType.get(); }

  const std::unordered_map<std::string, Type *> &getBindings() const {
    return bindings;
  }

private:
  std::string functionName;
  Own<Type> returnType;
  VecOwn<Variable> arguments;
  VecOwn<Variable> variables;
  VecOwn<Assignment> declarations;
  Map<std::string, Type *> bindings;
};

/* Compiler */

using json = nlohmann::json;

class Compiler {
public:
  using WidthScale = std::pair<int, int>;

  Compiler(duckdb::Connection *connection, const std::string &programText)
      : connection(connection), programText(programText) {}

  void run();

  static constexpr std::size_t VECTOR_SIZE = 2048;
  static constexpr std::size_t DECIMAL_WIDTH = 18;
  static constexpr std::size_t DECIMAL_SCALE = 3;
  static constexpr char RETURN_TYPE_PATTERN[] =
      "RETURNS\\s+(\\w+ *(\\((\\d+, *)?\\d+\\))?)";
  static constexpr char FUNCTION_NAME_PATTERN[] =
      "CREATE\\s+FUNCTION\\s+(\\w+)";

private:
  json parseJson() const;
  Vec<Function> getFunctions() const;

  Own<Expression> bindExpression(const Function &function,
                                 const std::string &expression);
  static Opt<WidthScale> getDecimalWidthScale(const std::string &type);
  static PostgresTypeTag getPostgresTag(const std::string &name);
  Own<Type> getTypeFromPostgresName(const std::string &name) const;
  std::string resolveTypeName(const std::string &type) const;

  duckdb::Connection *connection;
  std::string programText;
};
