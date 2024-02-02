#pragma once

#include "dataflow_framework.hpp"

using DominanceFrontier = Map<BasicBlock *, Set<BasicBlock *>>;

class Dominators {
public:
  Dominators(const Vec<BasicBlock *> &basicBlocks)
      : parentToChild(), childToParent() {
    for (auto *block : basicBlocks) {
      parentToChild.insert({block, Set<BasicBlock *>()});
      childToParent.insert({block, Set<BasicBlock *>()});
    }
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

private:
  Map<BasicBlock *, Set<BasicBlock *>> parentToChild;
  Map<BasicBlock *, Set<BasicBlock *>> childToParent;
};

class DominatorTree {
public:
  DominatorTree(const Vec<String> &blockLabels) : edges() {
    for (const auto &label : blockLabels) {
      edges.insert({label, Set<String>()});
    }
  }

  void addChild(const String &parent, const String &child) {
    edges.at(parent).insert(child);
  }

  const Set<String> &getChildren(const String &node) { return edges.at(node); }

private:
  Map<String, Set<String>> edges;
};

class DominatorDataflow : public DataflowFramework<BitVector, true> {
public:
  Map<BasicBlock *, std::size_t> blockToIndex;
  Vec<BasicBlock *> basicBlocks;

  DominatorDataflow(Function &f) : DataflowFramework(f) {}

  Own<Dominators> computeDominators() const;
  Own<DominanceFrontier>
  computeDominanceFrontier(const Own<Dominators> &dominators) const;
  Own<DominatorTree>
  computeDominatorTree(const Own<Dominators> &dominators) const;

protected:
  BitVector transfer(BitVector in, Instruction *inst) override;
  BitVector meet(BitVector in1, BitVector in2) override;
  void preprocessBlock(BasicBlock *block) override;
  void preprocessInst(Instruction *inst) override;
  void genBoundaryInner() override;
};