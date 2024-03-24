#pragma once

#include "basic_block.hpp"
#include "instructions.hpp"
#include "region.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

class BasicBlockIterator {
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = BasicBlock;
  using pointer = BasicBlock *;
  using reference = BasicBlock &;

public:
  BasicBlockIterator(VecOwn<BasicBlock>::iterator iter) : iter(iter) {}

  reference operator*() const { return *iter->get(); }
  pointer operator->() { return iter->get(); }

  BasicBlockIterator &operator++() {
    ++iter;
    return *this;
  }

  BasicBlockIterator &operator--() {
    --iter;
    return *this;
  }

  BasicBlockIterator operator++(int) {
    auto tmp = *this;
    ++(*this);
    return tmp;
  }

  BasicBlockIterator operator--(int) {
    auto tmp = *this;
    --(*this);
    return tmp;
  }

  friend bool operator==(BasicBlockIterator &a, BasicBlockIterator &b) {
    return a.iter == b.iter;
  };
  friend bool operator!=(BasicBlockIterator &a, BasicBlockIterator &b) {
    return !(a == b);
  };

private:
  VecOwn<BasicBlock>::iterator iter;
};

class ConstBasicBlockIterator {
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = BasicBlock;
  using pointer = BasicBlock *;
  using reference = BasicBlock &;

public:
  ConstBasicBlockIterator(VecOwn<BasicBlock>::const_iterator iter)
      : iter(iter) {}

  const reference operator*() const { return *iter->get(); }
  pointer operator->() { return iter->get(); }

  ConstBasicBlockIterator &operator++() {
    ++iter;
    return *this;
  }

  ConstBasicBlockIterator &operator--() {
    --iter;
    return *this;
  }

  ConstBasicBlockIterator operator++(int) {
    auto tmp = *this;
    ++(*this);
    return tmp;
  }

  ConstBasicBlockIterator operator--(int) {
    auto tmp = *this;
    --(*this);
    return tmp;
  }

  friend bool operator==(const ConstBasicBlockIterator &a,
                         const ConstBasicBlockIterator &b) {
    return a.iter == b.iter;
  };
  friend bool operator!=(const ConstBasicBlockIterator &a,
                         const ConstBasicBlockIterator &b) {
    return !(a == b);
  };

private:
  VecOwn<BasicBlock>::const_iterator iter;
};

class UseDefs;

struct FunctionCloneAndRenameHelper {
  template <typename T> Own<T> cloneAndRename(const T &obj) {
    ERROR("Not implemented yet!");
    return nullptr;
  }

  Map<const Variable *, const Variable *> variableMap;
  Map<BasicBlock *, BasicBlock *> basicBlockMap;
};

class Function {
public:
  Function(duckdb::Connection *conn, const String &name, const Type &returnType)
      : conn(conn), labelNumber(0), tempVariableCounter(0), functionName(name),
        returnType(returnType) {}

  Function(const Function &other) = delete;

  ~Function() { destroyDuckDBContext(); }

  friend std::ostream &operator<<(std::ostream &os, const Function &function) {
    function.print(os);
    return os;
  }

  ConstBasicBlockIterator begin() const {
    return ConstBasicBlockIterator(basicBlocks.cbegin());
  }
  ConstBasicBlockIterator end() const {
    return ConstBasicBlockIterator(basicBlocks.cend());
  }
  BasicBlockIterator begin() { return BasicBlockIterator(basicBlocks.begin()); }
  BasicBlockIterator end() { return BasicBlockIterator(basicBlocks.end()); }

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
    auto label = String("B") + std::to_string(labelNumber);
    ++labelNumber;
    return label;
  }

  void removeNestedRegion(Region *nestedRegion);

  void setRegion(Own<Region> region) { functionRegion = std::move(region); }
  Region *getRegion() const { return functionRegion.get(); }

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

  const Variable *createTempVariable(Type type, bool isNULL) {
    auto newName = "temp_" + std::to_string(tempVariableCounter) + "_";
    addVariable(newName, type, isNULL);
    ++tempVariableCounter;
    return getBinding(newName);
  }

  void addVarInitialization(const Variable *var, Own<SelectExpression> expr) {
    auto assignment = Make<Assignment>(var, std::move(expr));
    declarations.emplace_back(std::move(assignment));
  }

  String getOriginalName(const String &ssaName) const {
    return ssaName.substr(0, ssaName.find_first_of("_"));
  };

  duckdb::Connection *getConnection() const { return conn; }

  bool isArgument(const Variable *var) const {
    for (auto &arg : arguments) {
      if (arg.get() == var) {
        return true;
      }
    }
    return false;
  }

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

  const Variable *getBinding(const String &name) const {
    auto cleanedName = getCleanedVariableName(name);
    ASSERT(hasBinding(cleanedName), "ERROR: No binding for: |" + name +
                                        "| after cleaning the name to: |" +
                                        cleanedName + "|");
    return bindings.at(cleanedName);
  }

  const Map<String, Variable *> &getAllBindings() const { return bindings; }

  Vec<const PhiNode *> getPhisFromBlock(BasicBlock *block) {
    Vec<const PhiNode *> phis;
    for (auto &inst : *block) {
      if (auto *phiNode = dynamic_cast<const PhiNode *>(&inst)) {
        phis.push_back(phiNode);
      }
    }
    return phis;
  }

  BasicBlock *getEntryBlock() { return basicBlocks[0].get(); }

  BasicBlock *getBlockFromLabel(const String &label) {
    return labelToBasicBlock.at(label);
  }

  void removeVariable(const Variable *var) {
    bindings.erase(var->getName());
    auto it = std::find_if(
        variables.begin(), variables.end(),
        [&](const Own<Variable> &variable) { return variable.get() == var; });
    variables.erase(it);
  }

  void mergeBasicBlocks(BasicBlock *top, BasicBlock *bottom);
  void removeBasicBlock(BasicBlock *toRemove);
  void makeDuckDBContext();
  void destroyDuckDBContext();

  Own<SelectExpression> renameVarInExpression(
      const SelectExpression *original,
      const Map<const Variable *, const Variable *> &oldToNew);

  void renameBasicBlocks(const Map<BasicBlock *, BasicBlock *> &oldToNew);

  Own<SelectExpression> replaceVarWithExpression(
      const SelectExpression *original,
      const Map<const Variable *, const SelectExpression *> &oldToNew);

  int typeMatches(const String &rhs, const Type &type, bool needContext = true);

  Own<SelectExpression> bindExpression(const String &expr, const Type &retType,
                                       bool needContext = true);

  Map<Instruction *, Instruction *> replaceUsesWithExpr(
      const Map<const Variable *, const SelectExpression *> &oldToNew,
      UseDefs &useDefs);

  String getCFGString() const {
    std::stringstream ss;
    ss << "digraph cfg {" << std::endl;
    for (const auto &block : basicBlocks) {
      ss << "\t" << block->getLabel() << " [label=\"" << *block << "\"];";
      for (auto *succ : block->getSuccessors()) {
        ss << "\t" << block->getLabel() << " -> " << succ->getLabel() << ";"
           << std::endl;
      }
    }
    ss << "}" << std::endl;
    return ss.str();
  }

  String getRegionString() const {
    std::stringstream ss;
    ss << "digraph region {";
    ss << *functionRegion << std::endl;
    ss << "}" << std::endl;
    return ss.str();
  }

  Own<Function> partialCloneAndRename(
      const String &newName, const Vec<const Variable *> &newArgs,
      const Type &newReturnType, const Vec<BasicBlock *> basicBlocks) const;

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
    os << getCFGString() << std::endl;

    if (functionRegion) {
      os << "Regions: \n" << std::endl;
      os << getRegionString() << std::endl;
    }
  }

private:
  duckdb::Connection *conn;
  std::size_t labelNumber;
  std::size_t tempVariableCounter;
  String functionName;
  Type returnType;
  VecOwn<Variable> arguments;
  SetOwn<Variable> variables;
  VecOwn<Assignment> declarations;
  Map<String, Variable *> bindings;
  VecOwn<BasicBlock> basicBlocks;
  Map<String, BasicBlock *> labelToBasicBlock;
  Own<Region> functionRegion;
};
