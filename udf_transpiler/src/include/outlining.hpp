#pragma once

#include "function_pass.hpp"

struct FunctionCloneAndRenameHelper {
  template <typename T> Own<T> cloneAndRename(const T &obj) {
    ERROR("Not implemented yet!");
    return nullptr;
  }

  static Own<Function> cloneAndRename(const Function &f);

  Map<const Variable *, const Variable *> variableMap;
  Map<const BasicBlock *, BasicBlock *> BasicBlockMap;
};

class OutliningPass : public FunctionPass {
public:
  OutliningPass() : FunctionPass() {}
  bool runOnFunction(Function &f) override;
  void OutlineRegion(Region *region, Function &f);
  String getPassName() const override { return "OutliningPass"; }
};