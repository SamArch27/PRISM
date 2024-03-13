#pragma once
#include "analysis.hpp"
#include "basic_block.hpp"
#include "utils.hpp"

class PredicateAnalysis : public Analysis {
public:
  PredicateAnalysis(Function &f) : Analysis(f) {}

  void runAnalysis() override;

  Vec<String> getPredicates() const { return predicates; }

private:
  Vec<Vec<BasicBlock *>> getAllPathsToBlock(BasicBlock *startBlock) const;

  Vec<String> predicates;
};