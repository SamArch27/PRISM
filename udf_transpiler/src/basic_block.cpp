#include "basic_block.hpp"

const Vec<BasicBlock *> &BasicBlock::getSuccessors() const {
  return successors;
}
const Vec<BasicBlock *> &BasicBlock::getPredecessors() const {
  return predecessors;
}

/**
 * Replace the old predecessor with the new predecessor
 * If the old predecessor is not found, then add the new predecessor
 */
void BasicBlock::replacePredecessor(const BasicBlock *oldPred,
                                    BasicBlock *newPred) {
  auto it = std::find(predecessors.begin(), predecessors.end(), oldPred);
  if (it != predecessors.end()) {
    *it = newPred;
  } else {
    if (std::find(predecessors.begin(), predecessors.end(), newPred) ==
        predecessors.end()) {
      addPredecessor(newPred);
    }
  }
}

void BasicBlock::addInstruction(Own<Instruction> inst) {
  // if we are inserting a terminator instruction then we update the
  // successor/predecessors appropriately
  inst->setParent(this);
  if (inst->isTerminator()) {
    // clear current successors
    successors.clear();
    for (auto *succBlock : inst->getSuccessors()) {
      addSuccessor(succBlock);
      if (std::find(succBlock->getPredecessors().begin(),
                    succBlock->getPredecessors().end(),
                    this) == succBlock->getPredecessors().end()) {
        succBlock->addPredecessor(this);
      }
    }
  }
  instructions.emplace_back(std::move(inst));
}

InstIterator BasicBlock::insertBefore(InstIterator targetInst,
                                      Own<Instruction> newInst) {
  newInst->setParent(this);
  return instructions.emplace(targetInst.iter, std::move(newInst));
}

InstIterator BasicBlock::insertBeforeTerminator(Own<Instruction> newInst) {
  return insertBefore(std::prev(instructions.end()), std::move(newInst));
}

InstIterator BasicBlock::removeInst(InstIterator targetInst) {
  if (targetInst->isTerminator()) {
    // clear current successors
    for (auto *succ : successors) {
      succ->removePredecessor(this);
    }
    successors.clear();
  }
  return instructions.erase(targetInst.iter);
}

InstIterator BasicBlock::findInst(Instruction *inst) {
  for (auto it = begin(); it != end(); ++it) {
    auto &curr = *it;
    if (&curr == inst) {
      return it;
    }
  }
  ERROR("Could not find instruction in BasicBlock::findInst()!");
  return InstIterator(end());
}

InstIterator BasicBlock::replaceInst(InstIterator targetInst,
                                     Own<Instruction> newInst,
                                     bool updateSuccPred) {
  if (updateSuccPred) {
    // careful with terminators
    if (newInst->isTerminator()) {
      removeInst(targetInst);
      addInstruction(std::move(newInst));
      return InstIterator(std::prev(instructions.end()));
    } else {
      auto it = insertBefore(targetInst, std::move(newInst));
      removeInst(targetInst);
      return it;
    }
  } else {
    auto it = insertBefore(targetInst, std::move(newInst));
    instructions.erase(targetInst.iter);
    return it;
  }
}

Instruction *BasicBlock::getInitiator() { return instructions.begin()->get(); }

Instruction *BasicBlock::getTerminator() {
  auto &last = instructions.back();
  ASSERT(last->isTerminator(),
         "Last instruction of BasicBlock must be a Terminator instruction.");
  return last.get();
}

void BasicBlock::setLabel(const String &newLabel) { label = newLabel; }

String BasicBlock::getLabel() const { return label; }

bool BasicBlock::isConditional() const { return successors.size() == 2; }

void BasicBlock::print(std::ostream &os) const {
  os << label << ":" << std::endl;
  for (const auto &inst : instructions) {

    os << *inst << std::endl;
  }
}

void BasicBlock::addSuccessor(BasicBlock *succ) { successors.push_back(succ); }
void BasicBlock::addPredecessor(BasicBlock *pred) {
  predecessors.push_back(pred);
}
void BasicBlock::removeSuccessor(BasicBlock *succ) {
  successors.erase(std::remove(successors.begin(), successors.end(), succ),
                   successors.end());
}
void BasicBlock::removePredecessor(BasicBlock *pred) {
  predecessors.erase(
      std::remove(predecessors.begin(), predecessors.end(), pred),
      predecessors.end());
}

void BasicBlock::renameBasicBlock(const BasicBlock *oldBlock,
                                  BasicBlock *newBlock,
                                  const BasicBlock *newsPrevPred) {
  for (auto it = begin(); it != end(); ++it) {
    auto &inst = *it;
    if (auto *branchInst = dynamic_cast<BranchInst *>(&inst)) {
      auto *trueBlock = branchInst->getIfTrue();
      auto *falseBlock = branchInst->getIfFalse();

      if (trueBlock == oldBlock) {
        trueBlock = newBlock;

        // update the predecessor of the new block
        if (newsPrevPred == nullptr) {
          trueBlock->clearPredecessors();
          trueBlock->addPredecessor(this);
        } else {
          trueBlock->replacePredecessor(newsPrevPred, this);
        }
      }
      if (falseBlock != nullptr && falseBlock == oldBlock) {
        falseBlock = newBlock;

        // update the predecessor of the new block
        if (newsPrevPred == nullptr) {
          falseBlock->clearPredecessors();
          falseBlock->addPredecessor(this);
        } else {
          falseBlock->replacePredecessor(newsPrevPred, this);
        }
      }

      // update the successor of the current block
      successors.clear();
      addSuccessor(trueBlock);
      if (falseBlock != nullptr) {
        addSuccessor(falseBlock);
      }

      if (branchInst->isUnconditional()) {
        it = replaceInst(it, Make<BranchInst>(trueBlock));
      } else {
        it = replaceInst(it, Make<BranchInst>(trueBlock, falseBlock,
                                              branchInst->getCond()->clone()));
      }
    }
  }
}