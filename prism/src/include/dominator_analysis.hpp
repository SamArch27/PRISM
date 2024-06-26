#pragma once

#include "basic_block.hpp"
#include "compiler_fmt/core.h"
#include "compiler_fmt/ostream.h"
#include "compiler_fmt/ranges.h"
#include "function.hpp"
#include "utils.hpp"

class DominanceFrontier {
public:
  DominanceFrontier(const Vec<BasicBlock *> &basicBlocks) : frontier() {
    for (auto *block : basicBlocks) {
      frontier.insert({block, Set<BasicBlock *>()});
    }
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const DominanceFrontier &frontier) {
    frontier.print(os);
    return os;
  }

  void addToFrontier(BasicBlock *block, BasicBlock *frontierBlock) {
    frontier.at(block).insert(frontierBlock);
  }

  const Set<BasicBlock *> &getFrontier(BasicBlock *block) const {
    return frontier.at(block);
  }

protected:
  void print(std::ostream &os) const {
    for (auto &[block, frontierBlocks] : frontier) {
      fmt::print(os, "DF({}) = {{{}}}\n", block->getLabel(),
                 joinVector(Function::getBasicBlockLabels(
                                frontierBlocks.begin(), frontierBlocks.end()),
                            ", "));
    }
  }

private:
  Map<BasicBlock *, Set<BasicBlock *>> frontier;
};

class DominatorTree {
public:
  DominatorTree(const Vec<String> &blockLabels) : edges() {
    for (const auto &label : blockLabels) {
      edges.insert({label, Set<String>()});
    }
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const DominatorTree &dominatorTree) {
    dominatorTree.print(os);
    return os;
  }

  void addChild(const String &parent, const String &child) {
    edges.at(parent).insert(child);
    parentMap[child] = parent;
  }

  bool dominates(const String &b1, const String &b2) {
    // b1 dominates b2 if b1 is an ancestor of b2
    while (true) {
      auto parent = getParent(b2);
      if (parent.empty()) {
        return false;
      }
      if (parent == b1) {
        return true;
      }
    }
    return false;
  }

  String getParent(const String &child) { return parentMap[child]; }

  const Set<String> &getChildren(const String &node) { return edges.at(node); }

protected:
  void print(std::ostream &os) const {
    os << "Dominator Tree: \n" << std::endl;
    os << "digraph cfg {" << std::endl;
    for (const auto &[blockLabel, childrenLabels] : edges) {
      os << "\t" << blockLabel << " [label=\"" << blockLabel << "\"];\n";
      for (const auto &childLabel : childrenLabels) {
        os << "\t" << blockLabel << " -> " << childLabel << ";" << std::endl;
      }
    }
    os << "}" << std::endl;
  }

private:
  Map<String, Set<String>> edges;
  Map<String, String> parentMap;
};

class DominatorAnalysis : public Analysis {
public:
  DominatorAnalysis(Function &f) : Analysis(f) {}

  void runAnalysis() override;

  const Own<DominanceFrontier> &getDominanceFrontier() const {
    return dominanceFrontier;
  }
  const Own<DominatorTree> &getDominatorTree() const { return dominatorTree; }

private:
  Own<DominanceFrontier> dominanceFrontier;
  Own<DominatorTree> dominatorTree;
};