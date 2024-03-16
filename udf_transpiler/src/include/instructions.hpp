#pragma once

#include "compiler_fmt/core.h"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <algorithm>
#include <functional>
#include <json.hpp>
#include <memory>
#include <optional>
#include <queue>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using LogicalPlan = duckdb::LogicalOperator;

std::ostream &operator<<(std::ostream &os, const LogicalPlan &expr);

class Variable {
public:
  Variable(const String &name, Type type, bool null = true)
      : name(name), type(type), null(null) {}

  String getName() const { return name; }
  Type getType() const { return type; }
  bool isNull() const { return null; }

  friend std::ostream &operator<<(std::ostream &os, const Variable &var) {
    var.print(os);
    return os;
  }

  Own<Variable> clone() const { return Make<Variable>(name, type, null); }

protected:
  void print(std::ostream &os) const { os << type << " " << name; }

private:
  String name;
  Type type;
  bool null;
};

class SelectExpression {
public:
  SelectExpression(const String &rawSQL, const Type &returnType,
                   Shared<LogicalPlan> logicalPlan,
                   const Set<const Variable *> &usedVariables)

      : rawSQL(rawSQL), returnType(returnType), logicalPlan(logicalPlan),
        usedVariables(usedVariables) {}

  Own<SelectExpression> clone() const {
    return Make<SelectExpression>(rawSQL, returnType, logicalPlan,
                                  usedVariables);
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const SelectExpression &expr) {
    expr.print(os);
    return os;
  }

  bool usesVariable(const Variable *var) const {
    return std::find(usedVariables.begin(), usedVariables.end(), var) !=
           usedVariables.end();
  }

  bool isSQLExpression() const {
    return toUpper(rawSQL).find(" FROM ") != String::npos;
  }

  String getRawSQL() const {
    // trim leading and trailing whitespace
    auto first = rawSQL.find_first_not_of(' ');
    auto last = rawSQL.find_last_not_of(' ');
    return rawSQL.substr(first, (last - first + 1));
  }

  const LogicalPlan *getLogicalPlan() const { return logicalPlan.get(); }
  const Set<const Variable *> &getUsedVariables() const {
    return usedVariables;
  }

  inline const Type &getReturnType() const { return returnType; }

protected:
  void print(std::ostream &os) const { os << rawSQL; }

private:
  String rawSQL;
  Type returnType;
  Shared<LogicalPlan> logicalPlan;
  duckdb::ClientContext *clientContext;
  Set<const Variable *> usedVariables;
};

class BasicBlock;

class Instruction {
public:
  Instruction() {}

  virtual ~Instruction() = default;

  friend std::ostream &operator<<(std::ostream &os, const Instruction &inst) {
    inst.print(os);
    return os;
  }
  virtual Own<Instruction> clone() const = 0;
  virtual bool isTerminator() const = 0;

  virtual const Variable *getResultOperand() const = 0;
  virtual Set<const Variable *> getOperands() const = 0;
  virtual Vec<BasicBlock *> getSuccessors() const = 0;
  Instruction *replaceWith(Own<Instruction> replacement);
  void eraseFromParent();
  void setParent(BasicBlock *parentBlock) { parent = parentBlock; }
  BasicBlock *getParent() const { return parent; }

protected:
  virtual void print(std::ostream &os) const = 0;

private:
  BasicBlock *parent = nullptr;
};

class PhiNode : public Instruction {
public:
  PhiNode(const Variable *var, VecOwn<SelectExpression> arguments)
      : Instruction(), var(var), arguments(std::move(arguments)) {}

  ~PhiNode() override = default;

  Own<Instruction> clone() const override {
    VecOwn<SelectExpression> newArguments;
    for (auto &arg : arguments) {
      newArguments.emplace_back(arg->clone());
    }
    return Make<PhiNode>(var, std::move(newArguments));
  }

  const Variable *getResultOperand() const override { return var; }

  Set<const Variable *> getOperands() const override {
    Set<const Variable *> operands;
    for (auto &arg : arguments) {
      for (auto *var : arg->getUsedVariables()) {
        operands.insert(var);
      }
    }
    return operands;
  }

  const Variable *getLHS() const { return var; }

  Vec<const SelectExpression *> getRHS() const {
    Vec<const SelectExpression *> result;
    for (auto &arg : arguments) {
      result.push_back(arg.get());
    }
    return result;
  }

  bool hasIdenticalArguments() const {
    auto &firstArgument = *arguments.begin();
    return std::all_of(arguments.begin(), arguments.end(),
                       [&](const Own<SelectExpression> &arg) {
                         return arg->getRawSQL() == firstArgument->getRawSQL();
                       });
  }

  bool isTerminator() const override { return false; }

  Vec<BasicBlock *> getSuccessors() const override { return {}; }

  friend std::ostream &operator<<(std::ostream &os, const PhiNode &phiNode) {
    phiNode.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const override {
    os << *var << " = Φ(";
    bool first = true;
    for (auto &arg : arguments) {
      if (first) {
        first = false;
      } else {
        os << ", ";
      }
      os << *arg;
    }
    os << ")";
  }

private:
  const Variable *var;
  VecOwn<SelectExpression> arguments;
};

class Assignment : public Instruction {
public:
  Assignment(const Variable *var, Own<SelectExpression> expr)
      : Instruction(), var(var), expr(std::move(expr)) {}

  ~Assignment() override = default;

  Own<Instruction> clone() const override {
    return Make<Assignment>(var, expr->clone());
  }

  const Variable *getResultOperand() const override { return var; }

  Set<const Variable *> getOperands() const override {
    return expr->getUsedVariables();
  }

  const Variable *getLHS() const { return var; }
  const SelectExpression *getRHS() const { return expr.get(); }
  friend std::ostream &operator<<(std::ostream &os,
                                  const Assignment &assignment) {
    assignment.print(os);
    return os;
  }
  bool isTerminator() const override { return false; }
  Vec<BasicBlock *> getSuccessors() const override { return {}; }

protected:
  void print(std::ostream &os) const override { os << *var << " = " << *expr; }

private:
  const Variable *var;
  Own<SelectExpression> expr;
};

class ReturnInst : public Instruction {
public:
  ReturnInst(Own<SelectExpression> expr)
      : Instruction(), expr(std::move(expr)) {}

  ~ReturnInst() override = default;

  Own<Instruction> clone() const override {
    return Make<ReturnInst>(expr->clone());
  }

  const Variable *getResultOperand() const override { return nullptr; }

  Set<const Variable *> getOperands() const override {
    return expr->getUsedVariables();
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const ReturnInst &returnInst) {
    returnInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override { return {}; }
  const SelectExpression *getExpr() const { return expr.get(); }

protected:
  void print(std::ostream &os) const override {
    os << "RETURN " << *expr << ";";
  }

private:
  Own<SelectExpression> expr;
};

class BranchInst : public Instruction {
public:
  BranchInst(BasicBlock *ifTrue, BasicBlock *ifFalse,
             Own<SelectExpression> cond)
      : Instruction(), ifTrue(ifTrue), ifFalse(ifFalse), cond(std::move(cond)),
        conditional(true) {}
  BranchInst(BasicBlock *ifTrue)
      : Instruction(), ifTrue(ifTrue), ifFalse(nullptr), cond(),
        conditional(false) {}

  ~BranchInst() override = default;

  Own<Instruction> clone() const override {
    return conditional ? Make<BranchInst>(ifTrue, ifFalse, cond->clone())
                       : Make<BranchInst>(ifTrue);
  }

  const Variable *getResultOperand() const override { return nullptr; }

  Set<const Variable *> getOperands() const override {
    return conditional ? cond->getUsedVariables() : Set<const Variable *>{};
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const BranchInst &branchInst) {
    branchInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override {
    return (conditional ? Vec<BasicBlock *>{ifTrue, ifFalse}
                        : Vec<BasicBlock *>{ifTrue});
  }

  bool isConditional() const { return conditional; }
  bool isUnconditional() const { return !conditional; }

  BasicBlock *getIfTrue() const { return ifTrue; }
  BasicBlock *getIfFalse() const { return ifFalse; }
  const SelectExpression *getCond() const { return cond.get(); }

protected:
  void print(std::ostream &os) const override;

private:
  BasicBlock *ifTrue;
  BasicBlock *ifFalse;
  Own<SelectExpression> cond;
  bool conditional;
};