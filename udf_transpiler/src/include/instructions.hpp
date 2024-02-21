/**
 * @file cfg.hpp
 * Building block classes of the control flow graph
 * All the definitions copied from compiler.hpp
 */

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
  SelectExpression(const String &rawSQL, Shared<LogicalPlan> logicalPlan,
                   const Vec<const Variable *> &usedVariables)

      : rawSQL(rawSQL), logicalPlan(logicalPlan), usedVariables(usedVariables) {
  }

  Own<SelectExpression> clone() const {
    return Make<SelectExpression>(rawSQL, logicalPlan, usedVariables);
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

  String getRawSQL() const { return rawSQL; }
  const LogicalPlan *getLogicalPlan() const { return logicalPlan.get(); }
  const Vec<const Variable *> &getUsedVariables() const {
    return usedVariables;
  }

protected:
  void print(std::ostream &os) const { os << rawSQL; }

private:
  String rawSQL;
  Shared<LogicalPlan> logicalPlan;
  duckdb::ClientContext *clientContext;
  Vec<const Variable *> usedVariables;
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
  virtual Vec<const Variable *> getOperands() const = 0;
  virtual Vec<BasicBlock *> getSuccessors() const = 0;
  void setParent(BasicBlock *parentBlock) { parent = parentBlock; }
  BasicBlock *getParent() const { return parent; }

protected:
  virtual void print(std::ostream &os) const = 0;

private:
  BasicBlock *parent;
};

class PhiNode : public Instruction {
public:
  PhiNode(const Variable *var, const Vec<const Variable *> &arguments)
      : Instruction(), var(var), arguments(arguments) {}

  ~PhiNode() override = default;

  Own<Instruction> clone() const override {
    return Make<PhiNode>(var, arguments);
  }

  const Variable *getResultOperand() const override { return var; }

  Vec<const Variable *> getOperands() const override { return arguments; }

  const Variable *getLHS() const { return var; }

  const Vec<const Variable *> &getRHS() const { return arguments; }

  bool hasIdenticalArguments() const {
    auto *firstArgument = arguments.front();
    return std::all_of(
        arguments.begin(), arguments.end(),
        [&](const Variable *arg) { return arg == firstArgument; });
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
    for (auto *arg : arguments) {
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
  Vec<const Variable *> arguments;
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

  Vec<const Variable *> getOperands() const override {
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
  void print(std::ostream &os) const override {
    os << *var << " = " << *getRHS();
  }

private:
  const Variable *var;
  Own<SelectExpression> expr;
};

class BasicBlock {
public:
  BasicBlock(const String &label)

      : label(label), predecessors(), successors() {}

  using InstIterator = ListOwn<Instruction>::iterator;

  friend std::ostream &operator<<(std::ostream &os, const BasicBlock &block) {
    block.print(os);
    return os;
  }

  const Vec<BasicBlock *> &getSuccessors() const { return successors; }
  const Vec<BasicBlock *> &getPredecessors() const { return predecessors; }

  void addSuccessor(BasicBlock *succ) { successors.push_back(succ); }
  void addPredecessor(BasicBlock *pred) { predecessors.push_back(pred); }
  void removeSuccessor(BasicBlock *succ) {
    successors.erase(std::remove(successors.begin(), successors.end(), succ),
                     successors.end());
  }
  void removePredecessor(BasicBlock *pred) {
    predecessors.erase(
        std::remove(predecessors.begin(), predecessors.end(), pred),
        predecessors.end());
  }

  void addInstruction(Own<Instruction> inst) {
    // if we are inserting a terminator instruction then we update the
    // successor/predecessors appropriately
    inst->setParent(this);
    if (inst->isTerminator()) {
      for (auto *succBlock : inst->getSuccessors()) {
        addSuccessor(succBlock);
        succBlock->addPredecessor(this);
      }
    }
    instructions.emplace_back(std::move(inst));
  }

  List<Own<Instruction>>::iterator insertBefore(const Instruction *targetInst,
                                                Own<Instruction> newInst) {

    auto it = std::find_if(
        instructions.begin(), instructions.end(),
        [&](const Own<Instruction> &inst) { return targetInst == inst.get(); });
    ASSERT((it != instructions.end()),
           "Instruction must exist when performing insertBefore()!");
    newInst->setParent(this);
    return instructions.emplace(it, std::move(newInst));
  }

  List<Own<Instruction>>::iterator insertAfter(const Instruction *targetInst,
                                               Own<Instruction> newInst) {

    auto it = std::find_if(
        instructions.begin(), instructions.end(),
        [&](const Own<Instruction> &inst) { return targetInst == inst.get(); });
    ASSERT((it != instructions.end()),
           "Instruction must exist when performing insertBefore()!");
    newInst->setParent(this);
    ++it; // import that we increment the iterator before to insert after
    return instructions.insert(it, std::move(newInst));
  }

  List<Own<Instruction>>::iterator removeInst(const Instruction *targetInst) {
    auto it = std::find_if(
        instructions.begin(), instructions.end(),
        [&](const Own<Instruction> &inst) { return targetInst == inst.get(); });
    return instructions.erase(it);
  }

  List<Own<Instruction>>::iterator replaceInst(const Instruction *targetInst,
                                               Own<Instruction> newInst) {
    auto it = insertBefore(targetInst, std::move(newInst));
    removeInst(targetInst);
    return it;
  }

  const ListOwn<Instruction> &getInstructions() const { return instructions; }

  void appendBasicBlock(BasicBlock *toAppend) {
    // delete my terminator
    instructions.pop_back();
    // copy all instructions over from other basic block
    for (const auto &inst : toAppend->getInstructions()) {
      addInstruction(inst->clone());
    }
  }

  Instruction *getInitiator() { return instructions.begin()->get(); }

  Instruction *getTerminator() {
    auto last = std::prev(instructions.end());
    ASSERT((*last)->isTerminator(),
           "Last instruction of BasicBlock must be a Terminator instruction.");
    return last->get();
  }

  String getLabel() const { return label; }

  bool isConditional() const { return successors.size() == 2; }

protected:
  void print(std::ostream &os) const {
    os << label << ":" << std::endl;
    for (const auto &inst : instructions) {

      os << *inst << std::endl;
    }
  }

private:
  String label;
  ListOwn<Instruction> instructions;
  Vec<BasicBlock *> predecessors;
  Vec<BasicBlock *> successors;
};

class ExitInst : public Instruction {
public:
  ExitInst() : Instruction() {}

  ~ExitInst() override = default;

  Own<Instruction> clone() const override { return Make<ExitInst>(); }

  const Variable *getResultOperand() const override { return nullptr; }

  Vec<const Variable *> getOperands() const override { return {}; }

  friend std::ostream &operator<<(std::ostream &os, const ExitInst &exitInst) {
    exitInst.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const override { os << "EXIT;"; }
  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override { return {}; }
};

class ReturnInst : public Instruction {
public:
  ReturnInst(Own<SelectExpression> expr, BasicBlock *exitBlock)
      : Instruction(), expr(std::move(expr)), exitBlock(exitBlock) {}

  ~ReturnInst() override = default;

  Own<Instruction> clone() const override {
    return Make<ReturnInst>(expr->clone(), exitBlock);
  }

  const Variable *getResultOperand() const override { return nullptr; }

  Vec<const Variable *> getOperands() const override {
    return expr->getUsedVariables();
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const ReturnInst &returnInst) {
    returnInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override { return {exitBlock}; }
  const SelectExpression *getExpr() const { return expr.get(); }
  BasicBlock *getExitBlock() const { return exitBlock; }

protected:
  void print(std::ostream &os) const override {
    os << "RETURN " << *getExpr() << ";";
  }

private:
  Own<SelectExpression> expr;
  BasicBlock *exitBlock;
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
    if (isConditional()) {
      return Make<BranchInst>(ifTrue, ifFalse, cond->clone());
    } else {
      return Make<BranchInst>(ifTrue);
    }
  }

  const Variable *getResultOperand() const override { return nullptr; }

  Vec<const Variable *> getOperands() const override {
    if (isConditional()) {
      return cond->getUsedVariables();
    } else {
      return {};
    }
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const BranchInst &branchInst) {
    branchInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override {
    Vec<BasicBlock *> res;
    res.push_back(ifTrue);
    if (conditional) {
      res.push_back(ifFalse);
    }
    return res;
  }

  bool isConditional() const { return conditional; }
  bool isUnconditional() const { return !conditional; }

  BasicBlock *getIfTrue() const { return ifTrue; }
  BasicBlock *getIfFalse() const { return ifFalse; }
  const SelectExpression *getCond() const { return cond.get(); }

protected:
  void print(std::ostream &os) const override {
    if (conditional) {
      os << "br ";
    } else {
      os << "jmp ";
    }

    if (conditional) {
      os << *getCond();
      os << " [" << ifTrue->getLabel() << "," << ifFalse->getLabel() << "]";
    } else {
      os << " [" << ifTrue->getLabel() << "] ";
    }
  }

private:
  BasicBlock *ifTrue;
  BasicBlock *ifFalse;
  Own<SelectExpression> cond;
  bool conditional;
};

class DominatorTree;