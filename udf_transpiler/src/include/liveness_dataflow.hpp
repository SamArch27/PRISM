#pragma once

#include "compiler_fmt/core.h"
#include "compiler_fmt/ostream.h"
#include "dataflow_framework.hpp"

class Liveness {
public:
  Liveness(const Vec<BasicBlock *> &basicBlocks) : liveIn(), liveOut() {
    for (auto *block : basicBlocks) {
      liveIn.insert({block, Set<const Variable *>()});
      liveOut.insert({block, Set<const Variable *>()});
    }
  }

  friend std::ostream &operator<<(std::ostream &os, const Liveness &liveness) {
    liveness.print(os);
    return os;
  }

  void addLiveIn(BasicBlock *block, const Variable *liveVariable) {
    liveIn.at(block).insert(liveVariable);
  }

  void addLiveOut(BasicBlock *block, const Variable *liveVariable) {
    liveOut.at(block).insert(liveVariable);
  }

  const Set<const Variable *> &getLiveIn(BasicBlock *block) const {
    return liveIn.at(block);
  }

  const Set<const Variable *> &getLiveOut(BasicBlock *block) const {
    return liveOut.at(block);
  }

protected:
  void print(std::ostream &os) const {
    for (auto &[block, _] : liveIn) {
      String liveInString;
      bool first = true;
      for (auto liveVar : liveIn.at(block)) {
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
      for (auto liveVar : liveOut.at(block)) {
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
  Map<BasicBlock *, Set<const Variable *>> liveIn;
  Map<BasicBlock *, Set<const Variable *>> liveOut;
};

class LivenessDataflow : public DataflowFramework<BitVector, false> {
public:
  LivenessDataflow(Function &f) : DataflowFramework(f) {}
  Own<Liveness> computeLiveness() const;

protected:
  BitVector transfer(BitVector out, Instruction *inst) override;
  BitVector meet(BitVector in1, BitVector in2) override;
  void preprocessInst(Instruction *inst) override;
  void genBoundaryInner() override;

  Map<const Variable *, const Instruction *> def;
  Vec<const Variable *> variables;
  Map<const Variable *, std::size_t> varToIndex;
};