#pragma once

#include "compiler_fmt/core.h"
#include "compiler_fmt/ostream.h"
#include "compiler_fmt/ranges.h"
#include "dataflow_framework.hpp"

class Dominators {
public:
  Dominators(const Vec<BasicBlock *> &basicBlocks)
      : parentToChild(), childToParent() {
    for (auto *block : basicBlocks) {
      parentToChild.insert({block, Set<BasicBlock *>()});
      childToParent.insert({block, Set<BasicBlock *>()});
    }
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const Dominators &dominators) {
    dominators.print(os);
    return os;
  }

  void addDominanceEdge(BasicBlock *parent, BasicBlock *child) {
    parentToChild.at(parent).insert(child);
    childToParent.at(child).insert(parent);
  }

  bool dominates(BasicBlock *parent, BasicBlock *child) const {
    return parentToChild.at(parent).count(child) != 0;
  }

  bool strictlyDominates(BasicBlock *parent, BasicBlock *child) const {
    return parent != child && dominates(parent, child);
  }

  const Set<BasicBlock *> &getDominatingNodes(BasicBlock *block) const {
    return childToParent.at(block);
  }

protected:
  void print(std::ostream &os) const {
    for (auto &[parent, children] : parentToChild) {
      fmt::print(os, "Dom({}) = {{{}}}\n", parent->getLabel(),
                 joinVector(Function::getBasicBlockLabels(children.begin(),
                                                          children.end()),
                            ", "));
    }
  }

private:
  Map<BasicBlock *, Set<BasicBlock *>> parentToChild;
  Map<BasicBlock *, Set<BasicBlock *>> childToParent;
};

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
  }

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
};

class DominatorDataflow : public DataflowFramework<BitVector, true> {
public:
  DominatorDataflow(Function &f) : DataflowFramework(f) {}

  Own<Dominators> computeDominators() const;
  Own<DominanceFrontier>
  computeDominanceFrontier(const Own<Dominators> &dominators) const;
  Own<DominatorTree>
  computeDominatorTree(const Own<Dominators> &dominators) const;

protected:
  BitVector transfer(BitVector in, Instruction *inst) override;
  BitVector meet(BitVector in1, BitVector in2) override;
  void preprocessInst(Instruction *inst) override;
  void genBoundaryInner() override;

  Map<BasicBlock *, std::size_t> blockToIndex;
  Vec<BasicBlock *> basicBlocks;
};