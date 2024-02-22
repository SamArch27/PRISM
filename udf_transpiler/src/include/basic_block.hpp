#pragma once

#include "instructions.hpp"
#include "utils.hpp"
#include <cstddef>
#include <iterator>

class InstIterator {
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = Instruction;
  using pointer = Instruction *;
  using reference = Instruction &;

public:
  InstIterator(ListOwn<Instruction>::iterator iter) : iter(iter) {}

  reference operator*() const { return *iter->get(); }
  pointer operator->() { return iter->get(); }

  InstIterator &operator++() {
    ++iter;
    return *this;
  }

  InstIterator &operator--() {
    --iter;
    return *this;
  }

  InstIterator operator++(int) {
    InstIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  InstIterator operator--(int) {
    InstIterator tmp = *this;
    --(*this);
    return tmp;
  }

  friend bool operator==(const InstIterator &a, const InstIterator &b) {
    return a.iter->get() == b.iter->get();
  };
  friend bool operator!=(const InstIterator &a, const InstIterator &b) {
    return !(a == b);
  };

private:
  ListOwn<Instruction>::iterator iter;
};

class BasicBlock {
public:
  BasicBlock(const String &label)
      : label(label), predecessors(), successors() {}

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

  InstIterator begin() { return InstIterator(instructions.begin()); }

  InstIterator end() { return InstIterator(instructions.end()); }

  InstIterator insertBefore(const Instruction *targetInst,
                            Own<Instruction> newInst);
  InstIterator insertAfter(const Instruction *targetInst,
                           Own<Instruction> newInst);

  InstIterator removeInst(const Instruction *targetInst);
  InstIterator replaceInst(const Instruction *targetInst,
                           Own<Instruction> newInst);

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
