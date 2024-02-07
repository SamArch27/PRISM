#pragma once
#include "bitvector.hpp"
#include "cfg.hpp"
#include "utils.hpp"

template <typename T> struct DataflowResult {
public:
  T in;
  T out;
};

template <typename T, bool forward> class DataflowFramework {
public:
  Map<Instruction *, DataflowResult<T>> results;

  DataflowFramework(Function &f) : f(f) {}

  void runAnalysis();

protected:
  T innerStart;
  T boundaryStart;
  Function &f;

  virtual T transfer(T in, Instruction *inst) = 0;
  virtual T meet(T in1, T in2) = 0;
  virtual void preprocessInst(Instruction *inst) = 0;
  virtual void genBoundaryInner() = 0;

private:
  void preprocess();
  void runForwards();
  void runBackwards();
};

template <typename T, bool forward>
void DataflowFramework<T, forward>::runForwards() {
  Set<BasicBlock *> worklist;
  worklist.insert(f.getEntryBlock());
  while (!worklist.empty()) {
    auto *basicBlock = *worklist.begin();
    worklist.erase(basicBlock);

    auto &instructions = basicBlock->getInstructions();
    auto *firstInst = basicBlock->getInitiator();

    T newIn = results[firstInst].in;
    bool changed = false;

    // iterate over predcessors, calling meet over their out sets
    for (auto *pred : basicBlock->getPredecessors()) {
      auto &predResult = results[pred->getTerminator()];
      if (!changed) {
        newIn = predResult.out;
        changed = true;
      } else {
        newIn = meet(predResult.out, newIn);
      }
    }

    results[firstInst].in = newIn;
    T oldOut = results[basicBlock->getTerminator()].out;
    results[firstInst].out = transfer(results[firstInst].in, firstInst);
    auto prevInst = firstInst;

    // for the remaining instructions:
    // (1) IN becomes OUT of the previous
    // (2) OUT because transfer of IN on the current instruction
    for (auto &inst : instructions) {
      auto *currentInst = inst.get();
      if (currentInst == firstInst) {
        continue;
      }
      results[currentInst].in = results[prevInst].out;
      results[currentInst].out = transfer(results[currentInst].in, currentInst);

      prevInst = currentInst;
    }

    if (results[basicBlock->getTerminator()].out != oldOut) {
      for (auto *succ : basicBlock->getSuccessors()) {
        worklist.insert(succ);
      }
    }
  }
}

template <typename T, bool forward>
void DataflowFramework<T, forward>::runBackwards() {
  Set<BasicBlock *> worklist;
  for (auto *exitBlock : f.getExitBlock()->getPredecessors()) {
    worklist.insert(exitBlock);
  }

  while (!worklist.empty()) {
    BasicBlock *basicBlock = *worklist.begin();
    worklist.erase(basicBlock);

    ASSERT(!basicBlock->getInstructions().empty(),
           "Cannot have empty block during Dataflow Analysis!");

    auto &instructions = basicBlock->getInstructions();
    auto *lastInst = basicBlock->getTerminator();

    T newOut = results[lastInst].out;
    bool changed = false;

    // iterate over successors, calling meet over their out sets
    for (auto *succ : basicBlock->getSuccessors()) {
      auto &succResult = results[succ->getInitiator()];
      if (!changed) {
        newOut = succResult.in;
        changed = true;
      } else {
        newOut = meet(succResult.in, newOut);
      }
    }

    results[lastInst].out = newOut;
    T oldIn = results[basicBlock->getInitiator()].in;
    results[lastInst].in = transfer(results[lastInst].out, lastInst);
    auto prevInst = lastInst;

    // iterate the remaining instructions
    for (auto &inst : reverse(instructions)) {
      auto *currentInst = inst.get();
      if (currentInst == lastInst) {
        continue;
      }
      results[currentInst].in = transfer(results[currentInst].out, currentInst);
      results[currentInst].out = results[prevInst].in;

      prevInst = currentInst;
    }

    if (results[basicBlock->getInitiator()].in != oldIn) {
      std::cout << "Add to worklist: ";
      for (auto *pred : basicBlock->getPredecessors()) {
        std::cout << *pred << ",";
        worklist.insert(pred);
      }
    }
  }
}

template <typename T, bool forward>
void DataflowFramework<T, forward>::preprocess() {

  // call pre-process for each inst
  for (auto &basicBlock : f.getBasicBlocks()) {
    for (auto &inst : basicBlock->getInstructions()) {
      auto *currentInst = inst.get();
      preprocessInst(currentInst);
    }
  }

  genBoundaryInner();

  // initialize the IN/OUT sets
  for (auto &basicBlock : f.getBasicBlocks()) {
    for (auto &inst : basicBlock->getInstructions()) {
      auto *currentInst = inst.get();
      results[currentInst].in = innerStart;
      results[currentInst].out = innerStart;
    }
  }

  results[f.getEntryBlock()->getInitiator()].in = boundaryStart;
  results[f.getExitBlock()->getTerminator()].out = boundaryStart;
}

template <typename T, bool forward>
void DataflowFramework<T, forward>::runAnalysis() {
  preprocess();
  if (forward) {
    runForwards();
  } else {
    runBackwards();
  }
}