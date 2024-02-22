#pragma once

#include "instructions.hpp"
#include "utils.hpp"

class BasicBlock {
public:
  BasicBlock(const String &label)
      : label(label), predecessors(), successors() {}

  using InstIterator = ListOwn<Instruction>::iterator;

  friend std::ostream &operator<<(std::ostream &os, const BasicBlock &block) {
    block.print(os);
    return os;
  }

  const Vec<BasicBlock *> &getSuccessors() const;
  const Vec<BasicBlock *> &getPredecessors() const;
  void addSuccessor(BasicBlock *succ);
  void addPredecessor(BasicBlock *pred);
  void removeSuccessor(BasicBlock *succ);
  void removePredecessor(BasicBlock *pred);
  void addInstruction(Own<Instruction> inst);

  List<Own<Instruction>>::iterator insertBefore(const Instruction *targetInst,
                                                Own<Instruction> newInst);
  List<Own<Instruction>>::iterator insertAfter(const Instruction *targetInst,
                                               Own<Instruction> newInst);

  List<Own<Instruction>>::iterator removeInst(const Instruction *targetInst);
  List<Own<Instruction>>::iterator replaceInst(const Instruction *targetInst,
                                               Own<Instruction> newInst);

  const ListOwn<Instruction> &getInstructions() const;
  void appendBasicBlock(BasicBlock *toAppend);

  Instruction *getInitiator();
  Instruction *getTerminator();

  String getLabel() const;
  bool isConditional() const;

protected:
  void print(std::ostream &os) const;

private:
  String label;
  ListOwn<Instruction> instructions;
  Vec<BasicBlock *> predecessors;
  Vec<BasicBlock *> successors;
};
