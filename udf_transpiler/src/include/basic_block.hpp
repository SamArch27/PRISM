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

  BasicBlock *getPred(std::size_t offset) const { return predecessors[offset]; }

  void renameBasicBlock(const Map<BasicBlock *, BasicBlock *> &oldToNew) {

    for (auto it = begin(); it != end(); ++it) {
      auto &inst = *it;
      if (auto *branchInst = dynamic_cast<BranchInst *>(&inst)) {
        auto *trueBlock = branchInst->getIfTrue();
        auto *falseBlock = branchInst->getIfFalse();

        if (oldToNew.find(branchInst->getIfTrue()) != oldToNew.end()) {
          removeSuccessor(trueBlock);
          trueBlock = oldToNew.at(trueBlock);
          successors.clear();
          addSuccessor(trueBlock);
          trueBlock->predecessors.clear();
          trueBlock->addPredecessor(this);
        }
        if (oldToNew.find(branchInst->getIfFalse()) != oldToNew.end()) {
          removeSuccessor(falseBlock);
          falseBlock = oldToNew.at(falseBlock);
          successors.clear();
          addSuccessor(falseBlock);
          falseBlock->predecessors.clear();
          falseBlock->addPredecessor(this);
        }

        if (branchInst->isUnconditional()) {
          it = replaceInst(it, Make<BranchInst>(trueBlock));
        } else {
          it =
              replaceInst(it, Make<BranchInst>(trueBlock, falseBlock,
                                               branchInst->getCond()->clone()));
        }
      }
    }
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
  InstIterator insertBeforeTerminator(Own<Instruction> newInst);
  InstIterator insertAfter(InstIterator targetInst, Own<Instruction> newInst);

  InstIterator findInst(Instruction *inst);
  InstIterator removeInst(InstIterator targetInst);
  InstIterator replaceInst(InstIterator targetInst, Own<Instruction> newInst);

  Instruction *getInitiator();
  Instruction *getTerminator();

  void setRegion(Region *region) { parentRegion = region; }
  Region *getRegion() const { return parentRegion; }

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

protected:
  void print(std::ostream &os) const;

private:
  String label;
  ListOwn<Instruction> instructions;
  Vec<BasicBlock *> predecessors;
  Vec<BasicBlock *> successors;
  Region *parentRegion = nullptr;
};
