#pragma once

#include "instructions.hpp"
#include "utils.hpp"

class Function {
public:
  Function(duckdb::Connection *connection, const String &functionName,
           Type returnType)
      : connection(connection), labelNumber(0), functionName(functionName),
        returnType(returnType) {}

  Function(const Function &other) = delete;

  friend std::ostream &operator<<(std::ostream &os, const Function &function) {
    function.print(os);
    return os;
  }

  template <typename Iter>
  static Vec<String> getBasicBlockLabels(Iter it, Iter end) {
    Vec<String> labels;
    labels.reserve(std::distance(it, end));
    for (; it != end; ++it) {
      auto *block = *it;
      labels.push_back(block->getLabel());
    }
    return labels;
  }

  BasicBlock *makeBasicBlock(const String &label) {
    basicBlocks.emplace_back(Make<BasicBlock>(label));
    auto *newBlock = basicBlocks.back().get();
    labelToBasicBlock.insert({label, newBlock});
    return newBlock;
  }

  BasicBlock *makeBasicBlock() {
    auto label = getNextLabel();
    return makeBasicBlock(label);
  }

  String getNextLabel() {
    auto label = String("L") + std::to_string(labelNumber);
    ++labelNumber;
    return label;
  }

  void addArgument(const String &name, Type type) {
    auto cleanedName = getCleanedVariableName(name);
    auto var = Make<Variable>(cleanedName, type);
    arguments.push_back(std::move(var));
    bindings.emplace(cleanedName, arguments.back().get());
  }

  void addVariable(const String &name, Type type, bool isNULL) {
    auto cleanedName = getCleanedVariableName(name);
    auto var = Make<Variable>(cleanedName, type, isNULL);
    auto [it, _] = variables.insert(std::move(var));
    bindings.emplace(cleanedName, it->get());
  }

  void addVarInitialization(const Variable *var, Own<SelectExpression> expr) {
    auto assignment = Make<Assignment>(var, std::move(expr));
    declarations.emplace_back(std::move(assignment));
  }

  String getOriginalName(const String &ssaName) {
    return ssaName.substr(0, ssaName.find_first_of("_"));
  };

  duckdb::Connection *getConnection() const { return connection; }
  const VecOwn<Variable> &getArguments() const { return arguments; }
  const SetOwn<Variable> &getVariables() const { return variables; }
  Set<Variable *> getAllVariables() const {
    Set<Variable *> allVariables;
    for (auto &var : variables) {
      allVariables.insert(var.get());
    }
    for (auto &arg : arguments) {
      allVariables.insert(arg.get());
    }
    return allVariables;
  }
  VecOwn<Assignment> takeDeclarations() { return std::move(declarations); }

  String getFunctionName() const { return functionName; }
  Type getReturnType() const { return returnType; }

  String getCleanedVariableName(const String &name) const {
    auto cleanedName = removeSpaces(name);
    cleanedName = toLower(cleanedName);
    return cleanedName;
  }

  bool hasBinding(const String &name) const {
    auto cleanedName = getCleanedVariableName(name);
    return bindings.find(cleanedName) != bindings.end();
  }

  std::size_t getPredNumber(BasicBlock *child, BasicBlock *parent) {
    const auto &preds = child->getPredecessors();
    auto it = std::find(preds.begin(), preds.end(), parent);
    ASSERT(it != preds.end(),
           "Error! No predecessor found with predNumber function!");
    return std::distance(preds.begin(), it);
  };

  const Variable *getBinding(const String &name) const {
    auto cleanedName = getCleanedVariableName(name);
    ASSERT(hasBinding(cleanedName), "ERROR: No binding for: |" + name +
                                        "| after cleaning the name to: |" +
                                        cleanedName + "|");
    return bindings.at(cleanedName);
  }

  const Map<String, Variable *> &getAllBindings() const { return bindings; }

  // TODO: Exploit SSA to make this faster
  const Instruction *getDefiningInstruction(const Variable *var) {
    for (auto &block : basicBlocks) {
      for (auto &inst : block->getInstructions()) {
        if (inst->getResultOperand() == var) {
          return inst.get();
        }
      }
    }
    ERROR("Should always have defining instruction for a variable!");
    return nullptr;
  }

  Vec<const PhiNode *> getPhisFromBlock(BasicBlock *block) {
    Vec<const PhiNode *> phis;
    for (auto &inst : block->getInstructions()) {
      if (auto *phiNode = dynamic_cast<const PhiNode *>(inst.get())) {
        phis.push_back(phiNode);
      }
    }
    return phis;
  }

  BasicBlock *getEntryBlock() { return basicBlocks[0].get(); }
  BasicBlock *getExitBlock() { return basicBlocks[1].get(); }

  BasicBlock *getBlockFromLabel(const String &label) {
    return labelToBasicBlock.at(label);
  }

  const VecOwn<BasicBlock> &getBasicBlocks() const { return basicBlocks; }

  void removeVariable(const Variable *var) {
    bindings.erase(var->getName());
    auto it = std::find_if(
        variables.begin(), variables.end(),
        [&](const Own<Variable> &variable) { return variable.get() == var; });
    variables.erase(it);
  }

  void removeBasicBlock(BasicBlock *toRemove);

  void makeDuckDBContext();
  void destroyDuckDBContext();
  Own<SelectExpression> buildReplacedExpression(
      const SelectExpression *original,
      const Map<const Variable *, const Variable *> &oldToNew);
  Own<SelectExpression> bindExpression(const String &expr);

  bool replaceUsesWith(const Map<const Variable *, const Variable *> &oldToNew);
  bool replaceDefsWith(const Map<const Variable *, const Variable *> &oldToNew);

protected:
  void print(std::ostream &os) const {
    os << "Function Name: " << functionName << std::endl;
    os << "Return Type: " << returnType << std::endl;
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
      for (auto *succ : block->getSuccessors()) {
        os << "\t" << block->getLabel() << " -> " << succ->getLabel() << ";"
           << std::endl;
      }
    }
    os << "}" << std::endl;
  }

private:
  class CompilationState {
  public:
    std::size_t labelNumber;
    VecOwn<BasicBlock> basicBlocks;
    // more things to memorize later
    CompilationState(std::size_t _labelNumber, VecOwn<BasicBlock> &_basicBlocks)
        : labelNumber(_labelNumber) {
      for (auto &bb : _basicBlocks) {
        std::cout << "Pushing: " << *bb << std::endl;
        this->basicBlocks.push_back(std::move(bb));
      }
    }
  };

  duckdb::Connection *connection;
  std::size_t labelNumber;
  String functionName;
  Type returnType;
  VecOwn<Variable> arguments;
  SetOwn<Variable> variables;
  VecOwn<Assignment> declarations;
  Map<String, Variable *> bindings;
  VecOwn<BasicBlock> basicBlocks;
  Map<String, BasicBlock *> labelToBasicBlock;
};
