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
    return a.iter == b.iter;
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
    return a.iter == b.iter;
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

  std::size_t getPredNumber(BasicBlock *pred) const {
    auto it = std::find(predecessors.begin(), predecessors.end(), pred);
    ASSERT(it != predecessors.end(),
           "Error! No predecessor found with predNumber function!");
    return std::distance(predecessors.begin(), it);
  };

  /**
   * This function maintains the order of predecessors of all existing blocks.
   *
   * When newsPrevPred == nullptr, assume newBlock is a fresh block that has no
   * predecessors or successors. Otherwise, newBlock is an existing block that
   * has a predecessor newsPrevPred.
   */
  void renameBasicBlock(const BasicBlock *oldBlock, BasicBlock *newBlock,
                        const BasicBlock *newsPrevPred = nullptr);

  const Vec<BasicBlock *> &getSuccessors() const;
  const Vec<BasicBlock *> &getPredecessors() const;
  void addSuccessor(BasicBlock *succ);
  void addPredecessor(BasicBlock *pred);
  void removeSuccessor(BasicBlock *succ);
  void removePredecessor(BasicBlock *pred);
  void replacePredecessor(const BasicBlock *oldPred, BasicBlock *newPred);

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
  InstIterator insertBeforeTerminator(Own<Instruction> newInst);

  InstIterator findInst(Instruction *inst);
  InstIterator removeInst(InstIterator targetInst);
  InstIterator replaceInst(InstIterator targetInst, Own<Instruction> newInst,
                           bool updateSuccPred = false);

  Instruction *getInitiator();
  Instruction *getTerminator();

  void setRegion(Region *region) { parentRegion = region; }
  Region *getRegion() const { return parentRegion; }

  void setLabel(const String &newLabel);
  String getLabel() const;
  bool isConditional() const;

  size_t size() const { return instructions.size(); }

  bool hasSelect() const {
    for (auto &inst : *this) {
      if (inst.hasSelect()) {
        return true;
      }
    }
    return false;
  }

  void clearPredecessors() { predecessors.clear(); }
  void clearSuccessors() { successors.clear(); }

protected:
  void print(std::ostream &os) const;

private:
  String label;
  ListOwn<Instruction> instructions;
  Vec<BasicBlock *> predecessors;
  Vec<BasicBlock *> successors;
  Region *parentRegion = nullptr;
};
