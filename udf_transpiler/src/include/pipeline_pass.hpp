#pragma once

#include "function_pass.hpp"
#include "utils.hpp"

class PipelinePass : public FunctionPass {
public:
  template <typename... Args> PipelinePass(Args... args) : FunctionPass() {
    Own<FunctionPass> tmp[] = {std::move(args)...};
    for (auto &cur : tmp) {
      pipeline.push_back(std::move(cur));
    }
  }

  PipelinePass(VecOwn<FunctionPass> pipeline)
      : FunctionPass(), pipeline(std::move(pipeline)) {}

  bool runOnFunction(Function &f) override {
    bool changed = false;
    for (auto &pass : pipeline) {
      std::cout << "Running pass: " << pass->getPassName() << std::endl;
      changed |= pass->runOnFunction(f);
    }
    return changed;
  }

  String getPassName() const override { return "Pipeline"; }

private:
  VecOwn<FunctionPass> pipeline;
};