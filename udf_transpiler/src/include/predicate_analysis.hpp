#pragma once
#include "analysis.hpp"
#include "basic_block.hpp"
#include "use_def_analysis.hpp"
#include "utils.hpp"

class PredicateAnalysis : public Analysis {
public:
  PredicateAnalysis(Function &f) : Analysis(f) {}

  void runAnalysis() override;

  Vec<String> getPredicates() const { return predicates; }

private:
  Vec<Vec<BasicBlock *>> getAllPathsToBlock(BasicBlock *startBlock) const;
  String getCondFromPath(const Vec<BasicBlock *> &path) const;
  String getExprOnPath(const Vec<BasicBlock *> &path,
                       const SelectExpression *returnValue) const;

  UseDefs *useDefs = nullptr;
  Vec<String> predicates;
};