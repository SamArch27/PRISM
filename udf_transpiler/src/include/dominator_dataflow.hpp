#pragma once

#include "dataflow_framework.hpp"

class DominatorDataflow : public DataflowFramework<BitVector, true> {
public:
  Map<BasicBlock *, std::size_t> blockToIndex;
  Vec<BasicBlock *> basicBlocks;

  DominatorDataflow(Function &f) : DataflowFramework(f) {}

  Map<BasicBlock *, Set<BasicBlock *>> getDominanceFrontier();
  bool dominates(BasicBlock *b1, BasicBlock *b2);

protected:
  BitVector transfer(BitVector in, Instruction *inst) override;
  BitVector meet(BitVector in1, BitVector in2) override;
  void preprocessInst(Instruction *inst) override;
  void genBoundaryInner() override;
};