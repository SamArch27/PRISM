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

class SelectExpression {
public:
  SelectExpression(const std::string &rawSQL, Shared<LogicalPlan> logicalPlan,
                   const Vec<std::string> &usedVariables)
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

  std::string getRawSQL() const { return rawSQL; }
  const LogicalPlan *getLogicalPlan() const { return logicalPlan.get(); }
  const Vec<std::string> &getUsedVariables() const { return usedVariables; }

protected:
  void print(std::ostream &os) const { os << rawSQL; }

private:
  std::string rawSQL;
  Shared<LogicalPlan> logicalPlan;
  duckdb::ClientContext *clientContext;
  Vec<std::string> usedVariables;
};

class Variable {
public:
  Variable(const std::string &name, Own<Type> type, bool null = true)
      : name(name), type(std::move(type)), null(null) {}

  std::string getName() const { return name; }
  const Type *getType() const { return type.get(); }
  bool isNull() const { return null; }

  friend std::ostream &operator<<(std::ostream &os, const Variable &var) {
    var.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const { os << *type << " " << name; }

private:
  std::string name;
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
  virtual Own<Instruction> clone() const = 0;
  virtual bool isTerminator() const = 0;
  virtual Vec<BasicBlock *> getSuccessors() const = 0;

protected:
  virtual void print(std::ostream &os) const = 0;
};

class PhiNode : public Instruction {
public:
  PhiNode(const Variable *var, const Vec<const Variable *> &arguments)
      : var(var), arguments(arguments) {}

  Own<Instruction> clone() const override {
    return Make<PhiNode>(var, arguments);
  }

  bool isTerminator() const override { return false; }

  Vec<BasicBlock *> getSuccessors() const override { return {}; }

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

  Own<Instruction> clone() const override {
    return Make<Assignment>(var, expr->clone());
  }

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
  BasicBlock(const std::string &label)
      : label(label), predecessors(), successors() {}

  using InstIterator = ListOwn<Instruction>::iterator;

  friend std::ostream &operator<<(std::ostream &os, const BasicBlock &block) {
    block.print(os);
    return os;
  }

  const Set<BasicBlock *> &getSuccessors() const { return successors; }
  const Set<BasicBlock *> &getPredecessors() const { return predecessors; }

  void addSuccessor(BasicBlock *succ) { successors.insert(succ); }
  void addPredecessor(BasicBlock *pred) { predecessors.insert(pred); }
  void removeSuccessor(BasicBlock *succ) { successors.erase(succ); }
  void removePredecessor(BasicBlock *pred) { predecessors.erase(pred); }

  void addInstruction(Own<Instruction> inst) {
    // if we are inserting a terminator instruction then we update the
    // successor/predecessors appropriately
    if (inst->isTerminator()) {
      for (auto *succBlock : inst->getSuccessors()) {
        addSuccessor(succBlock);
        succBlock->addPredecessor(this);
      }
    }
    instructions.emplace_back(std::move(inst));
  }

  void insertBefore(const InstIterator iter, Own<Instruction> inst) {
    instructions.insert(iter, std::move(inst));
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

  Instruction *getTerminator() {
    auto last = std::prev(instructions.end());
    ASSERT((*last)->isTerminator(),
           "Last instruction of BasicBlock must be a Terminator instruction.");
    return last->get();
  }

  std::string getLabel() const { return label; }

protected:
  void print(std::ostream &os) const {
    os << label << ":" << std::endl;
    for (const auto &inst : instructions) {
      os << *inst << std::endl;
    }
  }

private:
  std::string label;
  ListOwn<Instruction> instructions;
  Set<BasicBlock *> predecessors;
  Set<BasicBlock *> successors;
};

class ReturnInst : public Instruction {
public:
  ReturnInst(Own<SelectExpression> expr, BasicBlock *exitBlock)
      : Instruction(), expr(std::move(expr)), exitBlock(exitBlock) {}

  Own<Instruction> clone() const override {
    return Make<ReturnInst>(expr->clone(), exitBlock);
  }

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

  Own<Instruction> clone() const override {
    if (isConditional()) {
      return Make<BranchInst>(ifTrue, ifFalse, cond->clone());
    } else {
      return Make<BranchInst>(ifTrue);
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
  Function(const std::string &functionName, Own<Type> returnType)
      : labelNumber(0), functionName(functionName),
        returnType(std::move(returnType)) {}

  friend std::ostream &operator<<(std::ostream &os, const Function &function) {
    function.print(os);
    return os;
  }

  BasicBlock *makeBasicBlock(const std::string &label) {
    basicBlocks.emplace_back(Make<BasicBlock>(label));
    return basicBlocks.back().get();
  }

  BasicBlock *makeBasicBlock() {
    auto label = getNextLabel();
    return makeBasicBlock(label);
  }

  void addArgument(const std::string &name, Own<Type> type) {
    auto var = Make<Variable>(name, std::move(type));
    arguments.emplace_back(std::move(var));
    bindings.emplace(name, arguments.back().get());
  }

  void addVariable(const std::string &name, Own<Type> &&type, bool isNULL) {
    auto var = Make<Variable>(name, std::move(type), isNULL);
    variables.emplace_back(std::move(var));
    bindings.emplace(name, variables.back().get());
  }

  void addVarInitialization(const Variable *var, Own<SelectExpression> expr) {
    auto assignment = Make<Assignment>(var, std::move(expr));
    declarations.emplace_back(std::move(assignment));
  }

  std::string getNextLabel() {
    auto label = std::string("L") + std::to_string(labelNumber);
    ++labelNumber;
    return label;
  }
  const VecOwn<Variable> &getArguments() const { return arguments; }
  const VecOwn<Variable> &getVariables() const { return variables; }
  VecOwn<Assignment> takeDeclarations() { return std::move(declarations); }

  std::string getFunctionName() const { return functionName; }
  const Type *getReturnType() const { return returnType.get(); }

  std::string getCleanedVariableName(const std::string &name) const {
    auto cleanedName = removeSpaces(name);
    cleanedName = toLower(cleanedName);
    return cleanedName;
  }

  bool hasBinding(const std::string &name) const {
    auto cleanedName = getCleanedVariableName(name);
    return bindings.find(cleanedName) != bindings.end();
  }

  const Variable *getBinding(const std::string &name) const {
    auto cleanedName = getCleanedVariableName(name);
    ASSERT(
        hasBinding(cleanedName),
        fmt::format("Error: Variable {} is not in bindings map.", cleanedName));
    return bindings.at(cleanedName);
  }

  const Map<std::string, Variable *> &getAllBindings() const {
    return bindings;
  }

  BasicBlock *getEntryBlock() { return basicBlocks[0].get(); }
  BasicBlock *getExitBlock() { return basicBlocks[1].get(); }

  const VecOwn<BasicBlock> &getAllBasicBlocks() { return basicBlocks; }

  void visitBFS(std::function<void(BasicBlock *)> f) {
    Queue<BasicBlock *> q;
    Set<BasicBlock *> visited;

    q.push(getEntryBlock());
    while (!q.empty()) {
      auto *block = q.front();
      q.pop();

      if (block == getExitBlock()) {
        continue;
      }

      if (visited.find(block) != visited.end()) {
        continue;
      }

      visited.insert(block);
      f(block);

      if (block == getExitBlock()) {
        continue;
      }

      for (auto &succ : block->getSuccessors()) {
        q.push(succ);
      }
    }
  }

  void mergeBasicBlocks() {
    visitBFS([this](BasicBlock *block) {
      while (true) {

        // skip if we are entry or exit
        if (block == getEntryBlock() || block == getExitBlock()) {
          return;
        }
        // if we have an unique successor
        auto successors = block->getSuccessors();
        if (successors.size() != 1) {
          return;
        }
        auto *uniqueSucc = *successors.begin();
        // that isn't entry/exit
        if (uniqueSucc == getEntryBlock() || uniqueSucc == getExitBlock()) {
          return;
        }
        // and we are the unique predecessor
        if (uniqueSucc->getPredecessors().size() != 1) {
          return;
        }

        // merge the two blocks
        block->appendBasicBlock(uniqueSucc);

        // finally remove the basic block from the function
        removeBasicBlock(uniqueSucc);
      }
    });
  }

  void removeBasicBlock(BasicBlock *toRemove) {
    auto it = basicBlocks.begin();
    while (it != basicBlocks.end()) {
      if (it->get() == toRemove) {
        // make all predecessors not reference this
        for (auto &pred : toRemove->getPredecessors()) {
          pred->removeSuccessor(toRemove);
        }
        // make all successors not reference this
        for (auto &succ : toRemove->getSuccessors()) {
          succ->removePredecessor(toRemove);
        }
        // finally erase the block
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

    os << "Control Flow Graph: \n" << std::endl;

    os << "digraph cfg {" << std::endl;
    for (const auto &block : basicBlocks) {
      os << "\t" << block->getLabel() << " [label=\"" << *block << "\"];";
      if (block->getLabel() != "exit") {
        for (auto *succ : block->getSuccessors()) {
          os << "\t" << block->getLabel() << " -> " << succ->getLabel() << ";"
             << std::endl;
        }
      }
    }
    os << "}" << std::endl;
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
      cout << "Pushing: " << *bb << endl;
      basicBlocks.push_back(std::move(bb));
    }
    // basicBlocks = state.basicBlocks;
    states.pop_back();
  }

  void addCustomAgg(const vector<string> &agg) { custom_aggs.push_back(agg); }

  const vector<vector<string>> &getCustomAggs() const { return custom_aggs; }

private:
  class CompilationState {
  public:
    std::size_t labelNumber;
    VecOwn<BasicBlock> basicBlocks;
    // more things to memorize later
    CompilationState(std::size_t _labelNumber, VecOwn<BasicBlock> &_basicBlocks)
        : labelNumber(_labelNumber) {
      for (auto &bb : _basicBlocks) {
        cout << "Pushing: " << *bb << endl;
        this->basicBlocks.push_back(std::move(bb));
      }
    }
  };
  std::list<CompilationState> states;
  std::size_t labelNumber;
  std::string functionName;
  Own<Type> returnType;
  VecOwn<Variable> arguments;
  VecOwn<Variable> variables;
  VecOwn<Assignment> declarations;
  Map<std::string, Variable *> bindings;
  VecOwn<BasicBlock> basicBlocks;
  vector<vector<string>> custom_aggs;
};

/**
 * Workaround for not been able to create a fake Function object
 * in other namespaces
 */
Function newFunction(const std::string &functionName);