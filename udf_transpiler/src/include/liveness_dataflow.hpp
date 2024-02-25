#pragma once

#include "compiler_fmt/core.h"
#include "compiler_fmt/ostream.h"
#include "dataflow_framework.hpp"
#include "function.hpp"

class Liveness {
public:
  Liveness(const Vec<BasicBlock *> &basicBlocks)
      : liveIn(), liveOut(), blockLiveIn(), blockLiveOut() {
    for (auto *block : basicBlocks) {
      blockLiveIn.insert({block, Set<const Variable *>()});
      blockLiveOut.insert({block, Set<const Variable *>()});

      for (auto &inst : *block) {
        liveIn.insert({&inst, Set<const Variable *>()});
        liveOut.insert({&inst, Set<const Variable *>()});
      }
    }
  }

  friend std::ostream &operator<<(std::ostream &os, const Liveness &liveness) {
    liveness.print(os);
    return os;
  }

  void addBlockLiveIn(BasicBlock *block, const Variable *liveVariable) {
    blockLiveIn.at(block).insert(liveVariable);
  }

  void removeBlockLiveIn(BasicBlock *block, const Variable *liveVariable) {
    blockLiveIn.at(block).erase(liveVariable);
  }

  void addBlockLiveOut(BasicBlock *block, const Variable *liveVariable) {
    blockLiveOut.at(block).insert(liveVariable);
  }

  void removeBlockLiveOut(BasicBlock *block, const Variable *liveVariable) {
    blockLiveOut.at(block).erase(liveVariable);
  }

  const Set<const Variable *> &getBlockLiveIn(BasicBlock *block) const {
    return blockLiveIn.at(block);
  }

  const Set<const Variable *> &getBlockLiveOut(BasicBlock *block) const {
    return blockLiveOut.at(block);
  }

protected:
  void print(std::ostream &os) const {
    for (auto &[block, _] : blockLiveIn) {
      String liveInString;
      bool first = true;
      for (auto liveVar : blockLiveIn.at(block)) {
        if (first) {
          first = false;
        } else {
          liveInString += ", ";
        }
        liveInString += liveVar->getName();
      }
      fmt::print(os, "LiveIn({}) = {{{}}}\n", block->getLabel(), liveInString);

      String liveOutString;
      first = true;
      for (auto liveVar : blockLiveOut.at(block)) {
        if (first) {
          first = false;
        } else {
          liveOutString += ", ";
        }
        liveOutString += liveVar->getName();
      }
      fmt::print(os, "LiveOut({}) = {{{}}}\n", block->getLabel(),
                 liveOutString);
    }
  }

private:
  Map<Instruction *, Set<const Variable *>> liveIn;
  Map<Instruction *, Set<const Variable *>> liveOut;
  Map<BasicBlock *, Set<const Variable *>> blockLiveIn;
  Map<BasicBlock *, Set<const Variable *>> blockLiveOut;
};

class InterferenceGraph {
public:
  void addInterferenceEdge(const Variable *left, const Variable *right) {
    edge[left].insert(right);
    edge[right].insert(left);
  }

  bool interferes(const Variable *left, const Variable *right) const {
    if (edge.find(left) == edge.end()) {
      return false;
    }
    if (edge.find(right) == edge.end()) {
      return false;
    }
    auto &leftEdges = edge.at(left);
    auto &rightEdges = edge.at(right);
    return leftEdges.find(right) != leftEdges.end() ||
           rightEdges.find(left) != rightEdges.end();
  }

  void removeEdge(const Variable *left, const Variable *right) {
    edge[left].erase(right);
    edge[right].erase(left);
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const InterferenceGraph &interferenceGraph) {
    interferenceGraph.print(os);
    return os;
  }

private:
  void print(std::ostream &os) const {
    os << "digraph cfg {" << std::endl;
    for (const auto &[var, others] : edge) {
      if (others.empty()) {
        continue;
      }
      os << "\t" << var->getName() << " [label=\"" << var->getName() << "\"];";
      for (const auto *other : others) {
        os << "\t" << var->getName() << " -> " << other->getName()
           << " [dir=none];" << std::endl;
      }
    }
    os << "}" << std::endl;
  }

  Map<const Variable *, Set<const Variable *>> edge;
};

class LivenessAnalysis : public DataflowFramework<BitVector, false> {
public:
  LivenessAnalysis(Function &f) : DataflowFramework(f) {}

  const Own<Liveness> &getLiveness() const { return liveness; }
  const Own<InterferenceGraph> &getInterferenceGraph() const {
    return interferenceGraph;
  }

protected:
  BitVector transfer(BitVector out, Instruction *inst) override;
  BitVector meet(BitVector result, BitVector in, BasicBlock *block) override;
  void preprocessInst(Instruction *inst) override;
  void genBoundaryInner() override;
  void finalize() override;

private:
  void computeLiveness();
  void computeInterferenceGraph();

  Map<BasicBlock *, Set<const Instruction *>> allDefs;
  Map<BasicBlock *, Set<const Instruction *>> phiDefs;
  Map<BasicBlock *, Set<const Instruction *>> phiUses;
  Map<BasicBlock *, Set<const Instruction *>> upwardsExposed;
  Vec<const Instruction *> definingInstructions;
  Map<const Instruction *, std::size_t> instToIndex;

  Own<Liveness> liveness;
  Own<InterferenceGraph> interferenceGraph;
};