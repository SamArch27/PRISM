#include "basic_block.hpp"

const Vec<BasicBlock *> &BasicBlock::getSuccessors() const {
  return successors;
}
const Vec<BasicBlock *> &BasicBlock::getPredecessors() const {
  return predecessors;
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

void BasicBlock::addInstruction(Own<Instruction> inst) {
  // if we are inserting a terminator instruction then we update the
  // successor/predecessors appropriately
  inst->setParent(this);
  if (inst->isTerminator()) {
    for (auto *succBlock : inst->getSuccessors()) {
      addSuccessor(succBlock);
      succBlock->addPredecessor(this);
    }
  }
  instructions.emplace_back(std::move(inst));
}

InstIterator BasicBlock::insertBefore(InstIterator targetInst,
                                      Own<Instruction> newInst) {
  newInst->setParent(this);
  return instructions.emplace(targetInst.iter, std::move(newInst));
}

InstIterator BasicBlock::insertAfter(InstIterator targetInst,
                                     Own<Instruction> newInst) {
  newInst->setParent(this);
  ++targetInst; // import that we increment the iterator before to insert after
  return instructions.insert(targetInst.iter, std::move(newInst));
}

InstIterator BasicBlock::removeInst(InstIterator targetInst) {
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
                                     Own<Instruction> newInst) {
  auto it = insertBefore(targetInst, std::move(newInst));
  removeInst(targetInst);
  return it;
}

void BasicBlock::appendBasicBlock(BasicBlock *toAppend) {
  // delete my terminator
  instructions.pop_back();
  // copy all instructions over from other basic block
  for (const auto &inst : *toAppend) {
    addInstruction(inst.clone());
  }
}

Instruction *BasicBlock::getInitiator() { return instructions.begin()->get(); }

Instruction *BasicBlock::getTerminator() {
  auto last = std::prev(instructions.end());
  ASSERT((*last)->isTerminator(),
         "Last instruction of BasicBlock must be a Terminator instruction.");
  return last->get();
}

String BasicBlock::getLabel() const { return label; }

bool BasicBlock::isConditional() const { return successors.size() == 2; }

void BasicBlock::print(std::ostream &os) const {
  os << label << ":" << std::endl;
  for (const auto &inst : instructions) {

    os << *inst << std::endl;
  }
}