#pragma once

#include "analysis.hpp"
#include "bitvector.hpp"
#include "compiler_fmt/core.h"
#include "compiler_fmt/ostream.h"
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

template <typename T> struct DataflowResult {
public:
  T in;
  T out;
};

class LivenessAnalysis : public Analysis {
public:
  LivenessAnalysis(Function &f) : Analysis(f) {
    useDefAnalysis = Make<UseDefAnalysis>(f);
  }

  void runAnalysis() override;

  Liveness *getLiveness() const { return liveness.get(); }

private:
  BitVector transfer(BitVector out, BasicBlock *block);
  BitVector meet(BitVector result, BitVector in, BasicBlock *block);
  void preprocess();
  void preprocessInst(Instruction *inst);
  void genBoundaryInner();
  void runBackwards();
  void finalize();

  void computeLiveness();

  Map<BasicBlock *, Set<const Variable *>> allDefs;
  Map<BasicBlock *, Set<const Variable *>> phiDefs;
  Map<BasicBlock *, Set<const Variable *>> phiUses;
  Map<BasicBlock *, Set<const Variable *>> upwardsExposed;
  Vec<const Variable *> variables;
  Map<const Variable *, std::size_t> varToIndex;

  Own<UseDefAnalysis> useDefAnalysis;
  Own<Liveness> liveness;

  BitVector innerStart;
  BitVector boundaryStart;
  Vec<BasicBlock *> exitBlocks;
  Map<BasicBlock *, DataflowResult<BitVector>> results;
};