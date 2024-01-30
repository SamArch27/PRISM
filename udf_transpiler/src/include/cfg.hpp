/**
 * @file cfg.hpp
 * Building block classes of the control flow graph
 * All the definitions copied from compiler.hpp
 */

#pragma once

#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "types.hpp"
#include "utils.hpp"
#include <functional>
#include <include/fmt/core.h>
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

// Expression can be a Phi function

class SelectExpression {
public:
  SelectExpression(const String &rawSQL, Own<LogicalPlan> logicalPlan,
                   const Vec<String> &usedVariables)
      : rawSQL(rawSQL), logicalPlan(std::move(logicalPlan)),
        usedVariables(usedVariables) {}

  friend std::ostream &operator<<(std::ostream &os,
                                  const SelectExpression &expr) {
    expr.print(os);
    return os;
  }

  String getRawSQL() const { return rawSQL; }
  const LogicalPlan *getLogicalPlan() const { return logicalPlan.get(); }
  const Vec<String> &getUsedVariables() const { return usedVariables; }

protected:
  void print(std::ostream &os) const { os << rawSQL; }

private:
  String rawSQL;
  Own<LogicalPlan> logicalPlan;
  Vec<String> usedVariables;
};

class Variable {
public:
  Variable(const String &name, Own<Type> type, bool null = true)
      : name(name), type(std::move(type)), null(null) {}

  String getName() const { return name; }
  const Type *getType() const { return type.get(); }
  bool isNull() const { return null; }

  friend std::ostream &operator<<(std::ostream &os, const Variable &var) {
    var.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const { os << *type << " " << name; }

private:
  String name;
  Own<Type> type;
  bool null;
};

class BasicBlock;

class Instruction {
public:
  friend std::ostream &operator<<(std::ostream &os, const Instruction &inst) {
    inst.print(os);
    return os;
  }
  virtual bool isTerminator() const = 0;
  virtual Vec<BasicBlock *> getSuccessors() const = 0;

protected:
  virtual void print(std::ostream &os) const = 0;
};

class PhiNode : public Instruction {
public:
  PhiNode(const Variable *var, const Vec<const Variable *> &arguments)
      : var(var), arguments(arguments) {}

  friend std::ostream &operator<<(std::ostream &os, const PhiNode &phiNode) {
    phiNode.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const override {
    os << *var << " = φ(";
    bool first = true;
    for (auto *arg : arguments) {
      if (first) {
        first = false;
      } else {
        os << ",";
      }
      os << arg;
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

  const Variable *getVar() const { return var; }
  const SelectExpression *getExpr() const { return expr.get(); }
  friend std::ostream &operator<<(std::ostream &os,
                                  const Assignment &assignment) {
    assignment.print(os);
    return os;
  }
  bool isTerminator() const override { return false; }
  Vec<BasicBlock *> getSuccessors() const override { return {}; }

protected:
  void print(std::ostream &os) const override {
    os << *var << " = " << *getExpr();
  }

private:
  const Variable *var;
  Own<SelectExpression> expr;
};

class BasicBlock {
public:
  BasicBlock(const String &label) : label(label) {}

  using InstIterator = ListOwn<Instruction>::iterator;

  friend std::ostream &operator<<(std::ostream &os, const BasicBlock &block) {
    block.print(os);
    return os;
  }

  void addInstruction(Own<Instruction> inst) {
    instructions.emplace_back(std::move(inst));
  }

  void popInstruction() { instructions.pop_back(); }

  void insertBefore(const InstIterator iter, Own<Instruction> inst) {
    instructions.insert(iter, std::move(inst));
  }

  ListOwn<Instruction> takeInstructions() { return std::move(instructions); }

  InstIterator begin() { return instructions.begin(); }
  InstIterator end() { return instructions.end(); }

  InstIterator getTerminator() {
    auto last = std::prev(instructions.end());
    ASSERT((*last)->isTerminator(),
           "Last instruction of BasicBlock must be a Terminator instruction.");
    return last;
  }

  String getLabel() const { return label; }

protected:
  void print(std::ostream &os) const {
    os << label << ":" << ENDL;
    for (const auto &inst : instructions) {
      os << *inst << ENDL;
    }
  }

private:
  String label;
  ListOwn<Instruction> instructions;
};

class ReturnInst : public Instruction {
public:
  ReturnInst(Own<SelectExpression> expr, BasicBlock *exitBlock)
      : Instruction(), expr(std::move(expr)), exitBlock(exitBlock) {}
  friend std::ostream &operator<<(std::ostream &os,
                                  const ReturnInst &returnInst) {
    returnInst.print(os);
    return os;
  }

  bool isTerminator() const override { return true; }
  Vec<BasicBlock *> getSuccessors() const override { return {exitBlock}; }
  SelectExpression *getExpr() const { return expr.get(); }

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
  SelectExpression *getCond() const { return cond.get(); }

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

class CursorLoop {
public:
  // the nth cursor loop
  int id;
  // cfg created by the compiler
  VecOwn<BasicBlock> basicBlocks;
  // more info
};

class Function {
public:
  Function(const String &functionName, Own<Type> returnType)
      : labelNumber(0), functionName(functionName),
        returnType(std::move(returnType)) {}

  friend std::ostream &operator<<(std::ostream &os, const Function &function) {
    function.print(os);
    return os;
  }

  BasicBlock *makeBasicBlock(const String &label) {
    basicBlocks.emplace_back(Make<BasicBlock>(label));
    return basicBlocks.back().get();
  }

  BasicBlock *makeBasicBlock() {
    auto label = getNextLabel();
    return makeBasicBlock(label);
  }

  void addArgument(const String &name, Own<Type> type) {
    auto var = Make<Variable>(name, std::move(type));
    arguments.emplace_back(std::move(var));
    bindings.emplace(name, arguments.back().get());
  }

  void addVariable(const String &name, Own<Type> &&type, bool isNULL) {
    auto var = Make<Variable>(name, std::move(type), isNULL);
    variables.emplace_back(std::move(var));
    bindings.emplace(name, variables.back().get());
  }

  void addVarInitialization(const Variable *var, Own<SelectExpression> expr) {
    auto assignment = Make<Assignment>(var, std::move(expr));
    declarations.emplace_back(std::move(assignment));
  }

  String getNextLabel() {
    auto label = String("L") + std::to_string(labelNumber);
    ++labelNumber;
    return label;
  }
  const VecOwn<Variable> &getArguments() const { return arguments; }
  const VecOwn<Variable> &getVariables() const { return variables; }
  VecOwn<Assignment> takeDeclarations() { return std::move(declarations); }

  String getFunctionName() const { return functionName; }
  const Type *getReturnType() const { return returnType.get(); }

  String getCleanedVariableName(const String &name) const {
    auto cleanedName = removeSpaces(name);
    cleanedName = toLower(cleanedName);
    return cleanedName;
  }

  bool hasBinding(const String &name) const {
    auto cleanedName = getCleanedVariableName(name);
    return bindings.find(cleanedName) != bindings.end();
  }

  const Variable *getBinding(const String &name) const {
    auto cleanedName = getCleanedVariableName(name);
    ASSERT(
        hasBinding(cleanedName),
        fmt::format("Error: Variable {} is not in bindings map.", cleanedName));
    return bindings.at(cleanedName);
  }

  const Map<String, Variable *> &getAllBindings() const {
    return bindings;
  }

  BasicBlock *getEntryBlock() { return basicBlocks[0].get(); }
  BasicBlock *getExitBlock() { return basicBlocks[1].get(); }

  const VecOwn<BasicBlock> &getAllBasicBlocks() { return basicBlocks; }

  void visitBFS(std::function<void(BasicBlock *)> f) {
    std::queue<BasicBlock *> q;
    std::unordered_set<BasicBlock *> visited;

    q.push(getEntryBlock());
    while (!q.empty()) {
      auto *block = q.front();
      q.pop();

      if (visited.find(block) != visited.end()) {
        continue;
      }

      visited.insert(block);
      f(block);

      if (block == getExitBlock()) {
        continue;
      }

      for (auto &succ : block->getTerminator()->get()->getSuccessors()) {
        q.push(succ);
      }
    }
  }

  void mergeBasicBlocks();

  void removeBasicBlock(BasicBlock *toRemove) {
    auto it = basicBlocks.begin();
    while (it != basicBlocks.end()) {
      if (it->get() == toRemove) {
        it = basicBlocks.erase(it);
      } else {
        ++it;
      }
    }
  }

  const VecOwn<BasicBlock> &getBasicBlocks() const { return basicBlocks; }
  VecOwn<CursorLoop> cursorLoops;

  // protected:
  void print(std::ostream &os) const {
    os << "Function Name: " << functionName << ENDL;
    os << "Return Type: " << *returnType << ENDL;
    os << "Arguments: " << ENDL;
    for (const auto &argument : arguments) {
      os << "\t" << *argument << ENDL;
    }
    os << "Variables: " << ENDL;
    for (const auto &variable : variables) {
      os << "\t" << *variable << ENDL;
    }
    os << "Declarations: " << ENDL;
    for (const auto &declaration : declarations) {
      os << "\t" << *declaration << ENDL;
    }

    os << "Control Flow Graph: \n" << ENDL;

    os << "digraph cfg {" << ENDL;
    for (const auto &block : basicBlocks) {
      os << "\t" << block->getLabel() << " [label=\"" << *block << "\"];";
      if (block->getLabel() != "exit") {
        for (auto *succ : block->getTerminator()->get()->getSuccessors()) {
          os << "\t" << block->getLabel() << " -> " << succ->getLabel() << ";"
             << ENDL;
        }
      }
    }
    os << "}" << ENDL;
  }

  void newState() {
    states.emplace_back(labelNumber, basicBlocks);
    basicBlocks.clear();
    labelNumber = 0;
  }

  /**
   * Clear the current state and restore the previous state in the stack
   */
  void popState() {
    basicBlocks.clear();
    auto &state = states.back();
    labelNumber = state.labelNumber;
    for (auto &bb : state.basicBlocks) {
      COUT << "Pushing: " << *bb << ENDL;
      basicBlocks.push_back(std::move(bb));
    }
    // basicBlocks = state.basicBlocks;
    states.pop_back();
  }

  void addCustomAgg(const Vec<String> &agg) { custom_aggs.push_back(agg); }

  const Vec<Vec<String>> &getCustomAggs() const { return custom_aggs; }

private:
  class CompilationState {
  public:
    std::size_t labelNumber;
    VecOwn<BasicBlock> basicBlocks;
    // more things to memorize later
    CompilationState(std::size_t _labelNumber, VecOwn<BasicBlock> &_basicBlocks)
        : labelNumber(_labelNumber) {
      for (auto &bb : _basicBlocks) {
        COUT << "Pushing: " << *bb << ENDL;
        this->basicBlocks.push_back(std::move(bb));
      }
    }
  };
  std::list<CompilationState> states;
  std::size_t labelNumber;
  String functionName;
  Own<Type> returnType;
  VecOwn<Variable> arguments;
  VecOwn<Variable> variables;
  VecOwn<Assignment> declarations;
  Map<String, Variable *> bindings;
  VecOwn<BasicBlock> basicBlocks;
  Vec<Vec<String>> custom_aggs;
};

/**
 * Workaround for not been able to create a fake Function object
 * in other namespaces
 */
Function newFunction(const String &functionName);