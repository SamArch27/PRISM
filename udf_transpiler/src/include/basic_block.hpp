#pragma once

#include "instructions.hpp"
#include "utils.hpp"
#include <cstddef>
#include <iterator>

class InstIterator {
  friend class BasicBlock;

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
    auto tmp = *this;
    ++(*this);
    return tmp;
  }

  InstIterator operator--(int) {
    auto tmp = *this;
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

class ConstInstIterator {
  friend class BasicBlock;

  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = Instruction;
  using pointer = Instruction *;
  using reference = Instruction &;

public:
  ConstInstIterator(ListOwn<Instruction>::const_iterator iter) : iter(iter) {}

  const reference operator*() const { return *iter->get(); }
  pointer operator->() { return iter->get(); }

  ConstInstIterator &operator++() {
    ++iter;
    return *this;
  }

  ConstInstIterator &operator--() {
    --iter;
    return *this;
  }

  ConstInstIterator operator++(int) {
    auto tmp = *this;
    ++(*this);
    return tmp;
  }

  ConstInstIterator operator--(int) {
    auto tmp = *this;
    --(*this);
    return tmp;
  }

  friend bool operator==(const ConstInstIterator &a,
                         const ConstInstIterator &b) {
    return a.iter->get() == b.iter->get();
  };
  friend bool operator!=(const ConstInstIterator &a,
                         const ConstInstIterator &b) {
    return !(a == b);
  };

private:
  ListOwn<Instruction>::const_iterator iter;
};

class Region;

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

  ConstInstIterator begin() const {
    return ConstInstIterator(instructions.cbegin());
  }
  ConstInstIterator end() const {
    return ConstInstIterator(instructions.cend());
  }
  InstIterator begin() { return InstIterator(instructions.begin()); }
  InstIterator end() { return InstIterator(instructions.end()); }

  InstIterator insertBefore(InstIterator targetInst, Own<Instruction> newInst);
  InstIterator insertAfter(InstIterator targetInst, Own<Instruction> newInst);

  InstIterator findInst(Instruction *inst);
  InstIterator removeInst(InstIterator targetInst);
  InstIterator replaceInst(InstIterator targetInst, Own<Instruction> newInst);

  void appendBasicBlock(BasicBlock *toAppend);

  Instruction *getInitiator();
  Instruction *getTerminator();

  void setParentRegion(Region *region) { parentRegion = region; }
  Region *getParentRegion() { return parentRegion; }

  String getLabel() const;
  bool isConditional() const;

protected:
  void print(std::ostream &os) const;

private:
  String label;
  ListOwn<Instruction> instructions;
  Vec<BasicBlock *> predecessors;
  Vec<BasicBlock *> successors;
  Region *parentRegion;
};
