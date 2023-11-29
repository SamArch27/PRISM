
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
  virtual bool isTerminator() const = 0;

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
  bool isTerminator() const override { return false; }

protected:
  void print(std::ostream &os) const override {
    os << *var << " = \n" << expr->ToString();
  }

private:
  const Variable *var;
  Own<Expression> expr;
};

class BasicBlock {
public:
  BasicBlock(const std::string &label) : label(label) {}

  void addInstruction(Own<Instruction> inst) {
    instructions.emplace_back(std::move(inst));
  }

  std::string getLabel() const { return label; }

private:
  std::string label;
  VecOwn<Instruction> instructions;
};

class ReturnInst : public Instruction {
public:
  ReturnInst(Own<Expression> expr, BasicBlock *exitBlock)
      : Instruction(), expr(std::move(expr)), exitBlock(exitBlock) {}
  friend std::ostream &operator<<(std::ostream &os,
                                  const ReturnInst &returnInst) {
    returnInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }

protected:
  void print(std::ostream &os) const override {
    os << "RETURN " << expr->ToString() << ";";
  }

private:
  Own<Expression> expr;
  BasicBlock *exitBlock;
};

class BranchInst : public Instruction {
public:
  BranchInst(BasicBlock *ifTrue, BasicBlock *ifFalse, Own<Expression> cond)
      : Instruction(), ifTrue(ifTrue), ifFalse(ifFalse), cond(std::move(cond)),
        conditional(true) {}
  BranchInst(BasicBlock *ifTrue)
      : Instruction(), ifTrue(ifTrue), ifFalse(nullptr), cond(nullptr),
        conditional(false) {}

  friend std::ostream &operator<<(std::ostream &os,
                                  const BranchInst &branchInst) {
    branchInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }

  bool isConditional() const { return conditional; }
  bool isUnconditional() const { return !conditional; }

protected:
  void print(std::ostream &os) const override {
    os << "jmp " << (conditional ? cond->ToString() : "true") << " ["
       << ifTrue->getLabel() << "," << ifFalse->getLabel() << "]";
  }

private:
  BasicBlock *ifTrue;
  BasicBlock *ifFalse;
  Own<Expression> cond;
  bool conditional;
};

class Function {
public:
  Function(const std::string &functionName, Own<Type> returnType)
      : functionName(functionName), returnType(std::move(returnType)) {}

  friend std::ostream &operator<<(std::ostream &os, const Function &function) {
    function.print(os);
    return os;
  }

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

protected:
  void print(std::ostream &os) const {
    os << "Function Name: " << functionName << std::endl;
    os << "Return Type: " << *returnType << std::endl;
    os << "Arguments: " << std::endl;
    for (const auto &argument : arguments) {
      os << "\t" << *argument << std::endl;
    }
    os << "Variables: " << std::endl;
    for (const auto &variable : variables) {
      os << "\t" << *variable << std::endl;
    }
    os << "Declarations: " << std::endl;
    for (const auto &declaration : declarations) {
      os << "\t" << *declaration << std::endl;
    }
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
