#include "instructions.hpp"
#include "basic_block.hpp"

void BranchInst::print(std::ostream &os) const {
  if (conditional) {
    os << "br ";
  } else {
    os << "jmp ";
  }

  if (conditional) {
    os << *getCond();
    os << " [" << ifTrue->getLabel() << "," << ifFalse->getLabel() << "]";
  } else {
    os << " [" << ifTrue->getLabel() << "] ";
  }
}
Instruction *Instruction::replaceWith(Own<Instruction> replacement) {
  return &*parent->replaceInst(parent->findInst(this), std::move(replacement));
}
void Instruction::eraseFromParent() {
  parent->removeInst(parent->findInst(this));
}