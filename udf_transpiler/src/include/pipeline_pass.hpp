#pragma once

#include "function_pass.hpp"
#include "udf_transpiler_extension.hpp"
#include "utils.hpp"
#include <chrono>

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
      if (!passOn(pass->getPassName())) {
        continue;
      }
      std::cout << "Running: " << pass->getPassName() << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      auto passChanged = pass->runOnFunction(f);
      changed = changed || passChanged;
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
      std::cout << pass->getPassName() << ": " << duration.count() << "ms"
                << std::endl;
    }
    return changed;
  }

  String getPassName() const override { return "Pipeline"; }

  VecOwn<FunctionPass> &getPipeline() { return pipeline; }

private:
  VecOwn<FunctionPass> pipeline;
};