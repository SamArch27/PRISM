#pragma once

#include "dominators.hpp"
#include "function_pass.hpp"

class SSAConstructionPass : public FunctionPass {
public:
  SSAConstructionPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  String getPassName() const override;

private:
  void insertPhiFunctions(Function &f,
                          const Own<DominanceFrontier> &dominanceFrontier);
  void renameVariablesToSSA(Function &f,
                            const Own<DominatorTree> &dominatorTree);
};