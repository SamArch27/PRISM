
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
  Variable(const std::string &name, std::unique_ptr<Type> type)
      : name(name), type(std::move(type)) {}

  std::string getName() const { return name; }
  const Type *getType() const { return type.get(); }

private:
  std::string name;
  std::unique_ptr<Type> type;
};

class Instruction {};

class Assignment : public Instruction {
public:
  Assignment(const Variable *var, std::unique_ptr<Expression> expr)
      : Instruction(), var(var), expr(std::move(expr)) {}

  const Variable *getVar() const { return var; }
  const Expression *getExpr() const { return expr.get(); }

private:
  const Variable *var;
  std::unique_ptr<Expression> expr;
};

class FunctionMetadata {
public:
  FunctionMetadata(const std::string &functionName,
                   std::unique_ptr<Type> returnType)
      : functionName(functionName), returnType(std::move(returnType)) {}

  void addArgument(const std::string &name, std::unique_ptr<Type> type) {
    bindings.emplace(name, type.get());
    arguments.emplace_back(std::make_unique<Variable>(name, std::move(type)));
  }

  void addVariable(const std::string &name, std::unique_ptr<Type> type,
                   std::unique_ptr<Expression> expr) {
    bindings.emplace(name, type.get());
    variables.emplace_back(std::make_unique<Variable>(name, std::move(type)));
    declarations.emplace_back(
        std::make_unique<Assignment>(variables.back().get(), std::move(expr)));
  }

  const std::vector<std::unique_ptr<Variable>> &getArguments() const {
    return arguments;
  }
  const std::vector<std::unique_ptr<Variable>> &getVariables() const {
    return variables;
  }
  const std::vector<std::unique_ptr<Assignment>> &getDeclarations() const {
    return declarations;
  }

  std::string getFunctionName() const { return functionName; }
  const Type *getReturnType() const { return returnType.get(); }

  const std::unordered_map<std::string, Type *> &getBindings() const {
    return bindings;
  }

private:
  std::string functionName;
  std::unique_ptr<Type> returnType;
  std::vector<std::unique_ptr<Variable>> arguments;
  std::vector<std::unique_ptr<Variable>> variables;
  std::vector<std::unique_ptr<Assignment>> declarations;
  std::unordered_map<std::string, Type *> bindings;
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
  std::vector<FunctionMetadata> getFunctionMetadata() const;

  std::unique_ptr<Expression> bindExpression(const FunctionMetadata &function,
                                             const std::string &expression);
  static std::optional<WidthScale>
  getDecimalWidthScale(const std::string &type);
  static PostgresTypeTag getPostgresTag(const std::string &name);
  std::unique_ptr<Type> getTypeFromPostgresName(const std::string &name) const;
  std::string resolveTypeName(const std::string &type) const;

  duckdb::Connection *connection;
  std::string programText;
};
