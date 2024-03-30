#include "aggify_pass.hpp"
#include "aggify_code_generator.hpp"
#include "cfg_code_generator.hpp"
#include "compiler.hpp"
#include "file.hpp"
#include "instructions.hpp"
#include "liveness_analysis.hpp"
#include "merge_regions.hpp"
#include "pipeline_pass.hpp"
#include "ssa_destruction.hpp"
#include "udf_transpiler_extension.hpp"
#include "utils.hpp"
#include <regex>

/**
 * Robust way to find the outgoing branch out of this list of blocks
 */
static Set<BasicBlock *>
getNextBasicBlock(const Vec<BasicBlock *> &basicBlocks) {
  Set<BasicBlock *> blockSet;
  Set<BasicBlock *> nextBlocks;
  for (auto *block : basicBlocks) {
    blockSet.insert(block);
  }
  for (auto *block : basicBlocks) {
    for (auto *succ : block->getSuccessors()) {
      if (blockSet.count(succ) == 0) {
        nextBlocks.insert(succ);
      }
    }
  }
  return nextBlocks;
}

static bool allBlocksNaive(const Vec<BasicBlock *> &basicBlocks) {
  // check if all the basic blocks are naive (just jmps)
  for (auto *block : basicBlocks) {
    size_t instCount = 0;
    for (auto &inst : *block) {
      instCount++;
    }
    if (instCount > 1) {
      return false;
    }
    if (block->getSuccessors().size() > 1) {
      return false;
    }
  }
  return true;
}

// only one block has outgoing branch
static bool supportedCursorLoop(const Vec<BasicBlock *> &basicBlocks) {
  Set<BasicBlock *> blockSet;
  for (auto *block : basicBlocks) {
    blockSet.insert(block);
  }

  // // header block has exactly two predecessors
  // if (basicBlocks[0]->getPredecessors().size() != 2) {
  //   return false;
  // }

  bool outgoing = false;
  for (auto *block : basicBlocks) {
    for (auto *succ : block->getSuccessors()) {
      if (blockSet.count(succ) == 0) {
        if (outgoing) {
          return false;
        }
        outgoing = true;
      }
    }
  }
  return outgoing;
}

/**
 * returns the call to custom aggregate in the context of the original function
 * @param newFunction the new function that was outlined
 * @param loopBodyBlocks the blocks that were outlined in the new function
 * @param oldFunction the original function
 * @param callerArgs the arguments used in the original function
 * @param customAggArgs the arguments used in the custom aggregate in the
 * orignal function context
 * @param returnVariable the return variable of the custom aggregate in the
 * original function context
 */
String AggifyPass::outlineCursorLoop(Function &newFunction,
                                     Vec<BasicBlock *> loopBodyBlocks,
                                     Function &oldFunction,
                                     Vec<const Variable *> callerArgs,
                                     Vec<const Variable *> customAggArgs,
                                     const Variable *returnVariable,
                                     const json &cursorLoopInfo) {
  // in SSA form
  // find the query in between /*fetchQueryStart*/ and
  // /*fetchQueryEnd*/
  String fetchQuery;
  for (const auto &blocks : newFunction) {
    for (const auto &inst : blocks) {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        if (assign->getRHS()->isSQLExpression() &&
            assign->getRHS()->getRawSQL().find("cursorloopEmptyTmp") !=
                std::string::npos) {
          fetchQuery = assign->getRHS()->getRawSQL();
        }
      } else if (auto *branch = dynamic_cast<const BranchInst *>(&inst)) {
        if (branch->isConditional() && branch->getCond()->isSQLExpression() &&
            branch->getCond()->getRawSQL().find("cursorloopEmptyTmp") !=
                std::string::npos) {
          fetchQuery = branch->getCond()->getRawSQL();
        }
      }
    }
  }
  ASSERT(fetchQuery != "", "Cannot find fetch query in the outlined code");
  auto start = fetchQuery.find("/*fetchQueryStart*/");
  auto end = fetchQuery.find("/*fetchQueryEnd*/");
  fetchQuery = fetchQuery.substr(start + 19, end - start - 19);

  auto ssaDestructionPipeline = Make<PipelinePass>(
      Make<SSADestructionPass>() /*, Make<AggressiveMergeRegionsPass>()*/);

  ssaDestructionPipeline->runOnFunction(newFunction);

  drawGraph(newFunction.getCFGString(), "after_aggify");

  // create a new function that will be used to generate the custom aggregate
  Vec<const Variable *> loopBodyFunctionArgs;

  // create a map that maps the new variable (not in SSA) to the old variables
  // (in SSA)
  // variables in the map should be live variables into the cursor loop
  Map<const Variable *, const Variable *> newToOld;
  for (size_t i = 0; i < callerArgs.size(); i++) {
    newToOld[newFunction.getArguments()[i].get()] = callerArgs[i];
  }

  Map<BasicBlock *, BasicBlock *> tmp;
  // create a new function that will be used to generate the custom aggregate
  // the data structure in this function is probably broken, so should only be
  // used for aggify code generation
  auto cursorLoopBodyFunction = newFunction.partialCloneAndRename(
      newFunction.getFunctionName() + "_custom_agg", loopBodyFunctionArgs,
      newFunction.getReturnType(), loopBodyBlocks, tmp);

  // remove the definition of cursorloopiter
  for (auto &block : *cursorLoopBodyFunction) {
    for (auto &inst : block) {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        if (assign->getLHS()->getName() == "cursorloopiter") {
          inst.eraseFromParent();
          break;
        }
      }
    }
  }

  // make this a dummy block
  auto returnBlock =
      cursorLoopBodyFunction->makeBasicBlock("accumulateReturnBlock");
  auto loopHeader = getNextBasicBlock(loopBodyBlocks);
  ASSERT(loopHeader.size() == 1, "Expected exactly one loop header");
  cursorLoopBodyFunction->renameBasicBlocks(*loopHeader.begin(), returnBlock);

  // From Aggify: all variables referenced in the loop body Δ
  Vec<const Variable *> loopBodyUsedVars;
  for (auto *var : customAggArgs) {
    if (cursorLoopBodyFunction->hasBinding(
            oldFunction.getOriginalName(var->getName())) &&
        oldFunction.getOriginalName(var->getName()) != "cursorloopiter") {
      loopBodyUsedVars.push_back(cursorLoopBodyFunction->getBinding(
          oldFunction.getOriginalName(var->getName())));
    }
  }
  // COUT << "Loop body used variables: " << ENDL;
  // for (auto var : loopBodyUsedVars) {
  //   COUT << var->getName() << " ";
  // }
  // COUT << ENDL;

  Vec<const Variable *> cursorVars;
  Map<const Variable *, String> cursorVarToFetchQueryVarName;

  for (const auto &blocks : newFunction) {
    for (const auto &inst : blocks) {
      if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
        // udf_todo: may not be the robust way to find the fetch query
        if (assign->getRHS()->isSQLExpression() &&
            assign->getRHS()->getRawSQL().find("cursorloopEmptyTmp") ==
                std::string::npos) {
          ASSERT(assign->getRHS()->getRawSQL().find("fetchQueryTmpTable") !=
                     std::string::npos,
                 "Do not support cursor loops with SELECT: " +
                     assign->getRHS()->getRawSQL());

          cursorVars.push_back(
              cursorLoopBodyFunction->getBinding(assign->getLHS()->getName()));

          // use the actual (first) selected fetchQueryVar\\d
          auto pattern = std::regex("fetchQueryVar\\d+");
          std::smatch matches;
          String input = assign->getRHS()->getRawSQL();
          if (std::regex_search(input, matches, pattern)) {
            cursorVarToFetchQueryVarName[cursorVars.back()] = matches[0];
          } else {
            ERROR("Cannot find fetchQueryVar\\d in the fetch query: " +
                  assign->getRHS()->getRawSQL());
          }
        }
      }
    }
  }
  // COUT << "Cursor vars: " << ENDL;
  // for (auto var : cursorVars) {
  //   COUT << var->getName() << " ";
  // }
  // COUT << ENDL;

  // generate the code for the custom aggregate

  // because of possible optimization of the database, we need to make sure that
  // at least one cursor variable is used in the loop body
  if (cursorVars.size() == 0) {
    ASSERT(cursorLoopInfo.contains("firstCursorVar") &&
               cursorLoopInfo["firstCursorVar"].contains("name") &&
               cursorLoopInfo["firstCursorVar"].contains("type"),
           "Cannot find firstCursorVar in cursorLoopInfo");
    auto var = cursorLoopInfo["firstCursorVar"];
    String varName = var["name"].get<String>();
    Type varType = Type::fromString(var["type"].get<String>());
    cursorLoopBodyFunction->addVariable(varName, varType, false);
    cursorVars.push_back(cursorLoopBodyFunction->getBinding(varName));
    cursorVarToFetchQueryVarName[cursorVars.back()] = "fetchQueryVar0";
    loopBodyUsedVars.push_back(cursorLoopBodyFunction->getBinding(varName));
  }

  INFO(fmt::format("Transpiling UDAF {} ...",
                   cursorLoopBodyFunction->getFunctionName()));
  AggifyCodeGenerator codeGenerator(compiler.getConfig());
  auto res = codeGenerator.run(
      *cursorLoopBodyFunction, cursorLoopInfo, cursorVars, loopBodyUsedVars,
      cursorLoopBodyFunction->getBinding(
          oldFunction.getOriginalName(returnVariable->getName())),
      compiler.getUdfCount());

  insertDefAndReg(res.code, res.registration, compiler.getUdfCount());
  // compile the template
  INFO("Compiling the UDAF...");
  compileUDF();
  // load the compiled library
  INFO("Installing and loading the UDAF...");
  loadUDF(*compiler.getConnection());

  // create a call to the custom aggregate in the original function

  // find in preheader, the initialization of used vars (except cursor vars)
  auto preheader = newFunction.getBlockFromLabel("preheader");
  ASSERT(preheader != nullptr, "Cannot find preheader block");
  Map<const Variable *, const SelectExpression *> varToInitExpr;
  for (auto &inst : *preheader) {
    if (auto *assign = dynamic_cast<const Assignment *>(&inst)) {
      varToInitExpr[assign->getLHS()] = assign->getRHS();
    }
  }

  // the initial values of the arguments to the custom aggregate
  Vec<String> customAggCallerArgs;
  String returnVariableInitValue;
  for (auto *var : loopBodyUsedVars) {
    if (cursorVarToFetchQueryVarName.find(var) !=
        cursorVarToFetchQueryVarName.end()) {
      // is a cursor loop variable
      // keep the original fetchQueryVar\\d
      customAggCallerArgs.push_back(cursorVarToFetchQueryVarName[var]);
    } else {
      var = newFunction.getBinding(var->getName());

      ASSERT(varToInitExpr.find(var) != varToInitExpr.end(),
             "Cannot find initialization for variable: " + var->getName());
      auto initialization =
          oldFunction.renameVarInExpression(varToInitExpr[var], newToOld);
      customAggCallerArgs.push_back(initialization->getRawSQL());

      // a return variable will always be used so there must be an
      // initialization
      if (Function::getOriginalName(returnVariable->getName()) ==
          var->getName()) {
        returnVariableInitValue = initialization->getRawSQL();
      }
    }
  }

  ASSERT(returnVariableInitValue != "",
         "Return variable initialization not found");

  Vec<String> fetchQueryVarNames;
  size_t varId = 0;
  for (auto &var : cursorLoopInfo["var"]["PLpgSQL_row"]["fields"]) {
    fetchQueryVarNames.push_back("fetchQueryVar" + std::to_string(varId));
    varId++;
  }
  String cursorQuery =
      fmt::format("SELECT * FROM ({}) fetchQueryTmpTable({})", fetchQuery,
                  joinVector(fetchQueryVarNames, ", "));

  String customAggCaller =
      fmt::format(fmt::runtime(compiler.getConfig().aggify["caller"].Scalar()),
                  fmt::arg("customAggName", res.name),
                  fmt::arg("funcArgs", joinVector(customAggCallerArgs, ", ")),
                  fmt::arg("returnVarName", returnVariableInitValue),
                  fmt::arg("cursorQuery", cursorQuery));

  compiler.getUdfCount()++;

  return customAggCaller;
}

bool AggifyPass::outlineRegion(const Region *region, Function &f) {
  auto blocksToOutline = region->getBasicBlocks();

  if (allBlocksNaive(blocksToOutline)) {
    return false;
  }

  if (!supportedCursorLoop(blocksToOutline)) {
    INFO("Aggify only supports cursor loops without break or return.");
    return false;
  }

  auto nextBasicBlocks = getNextBasicBlock(blocksToOutline);
  if (nextBasicBlocks.size() != 1) {
    INFO("Aggify only supports cursor loops with one outgoing branch.");
    return false;
  }

  BasicBlock *nextBasicBlock = *nextBasicBlocks.begin();

  // get live variables going into the region
  LivenessAnalysis livenessAnalysis(f);
  livenessAnalysis.runAnalysis();
  const auto &liveness = livenessAnalysis.getLiveness();

  auto *loopHeader = region->getHeader();

  Set<const Variable *> regionArgs;

  Set<const BasicBlock *> blocksToOutlineSet;
  for (auto *block : blocksToOutline) {
    blocksToOutlineSet.insert(block);
  }
  // region args are the live variables going into the region
  auto preds = loopHeader->getPredecessors();
  for (auto *pred : preds) {
    if (blocksToOutlineSet.find(pred) == blocksToOutlineSet.end()) {
      auto liveOut = liveness->getBlockLiveIn(pred);
      for (auto *var : liveOut) {
        regionArgs.insert(var);
      }
    }
  }

  // get live variable going out of the region
  Set<const Variable *> liveOut;
  Set<const Variable *> returnVars;

  // get the return variables:
  // variables that are live out of the region and defined in the region
  liveOut = liveness->getBlockLiveIn(nextBasicBlock);
  for (auto *var : liveOut) {
    if (regionArgs.count(var) == 0) {
      returnVars.insert(var);
    }
  }

  // COUT << "Outlining basic blocks for Aggify: " << ENDL;
  // for (auto *block : blocksToOutline) {
  //   COUT << block->getLabel() << " ";
  // }
  // COUT << ENDL;
  // COUT << "Loop header: " << loopHeader->getLabel() << ENDL;
  // COUT << "Fallthrough region: "
  //      << nextBasicBlock->getRegion()->getRegionLabel() << ENDL;
  // COUT << "Return variables: " << ENDL;
  // for (auto *var : returnVars) {
  //   COUT << var->getName() << " ";
  // }
  // COUT << ENDL;
  // COUT << "Input variables: " << ENDL;
  // for (auto *var : regionArgs) {
  //   COUT << var->getName() << " ";
  // }
  // COUT << ENDL;
  // COUT << ENDL;

  ASSERT(returnVars.size() == 1,
         fmt::format("Do not support one cursor loop to return {} variables",
                     returnVars.size()));

  auto *returnVariable = *returnVars.begin();

  String newFunctionName =
      fmt::format("{}_aggify_{}", f.getFunctionName(), outlinedCount);
  Vec<const Variable *> newFunctionArgs;
  for (auto *var : regionArgs) {
    newFunctionArgs.push_back(var);
  }
  Type returnType = returnVariable->getType();
  Map<BasicBlock *, BasicBlock *> oldToNew;
  auto newFunction = f.partialCloneAndRename(
      newFunctionName, newFunctionArgs, returnType, blocksToOutline, oldToNew);

  // add explicit return of the return variable to the end of the function
  auto *returnBlock = newFunction->makeBasicBlock("returnBlock");
  returnBlock->addInstruction(Make<ReturnInst>(newFunction->bindExpression(
      returnVariable->getName(), returnVariable->getType())));

  newFunction->renameBasicBlocks(nextBasicBlock, returnBlock);
  drawGraph(newFunction->getCFGString(), "aggify");

  // find the blocks that belong to the loop body
  Vec<BasicBlock *> loopBodyBlocks;
  const Region *loopBodyRegion = nullptr;
  Queue<const Region *> workList;
  workList.push(region);
  while (!workList.empty()) {
    auto *currentRegion = workList.front();
    workList.pop();
    if (currentRegion->getMetadata().find("udf_info") !=
        currentRegion->getMetadata().end()) {
      if (currentRegion->getMetadata()["udf_info"].get<String>() ==
          "cursorLoopBodyRegion") {
        loopBodyRegion = currentRegion;
        for (auto *block : currentRegion->getBasicBlocks()) {
          loopBodyBlocks.push_back(oldToNew.at(block));
        }
      }
    }
    if (auto *recursiveRegion =
            dynamic_cast<const RecursiveRegion *>(currentRegion)) {
      for (auto *nestedRegion : recursiveRegion->getNestedRegions()) {
        workList.push(nestedRegion);
      }
    }
  }
  ASSERT(loopBodyRegion != nullptr, "Could not find loop body region!");

  Vec<const Variable *> customAggArgs;
  auto loopBodyLiveIn = liveness->getBlockLiveIn(loopBodyRegion->getHeader());
  for (auto *var : loopBodyLiveIn) {
    customAggArgs.push_back(var);
  }

  String customAggCaller =
      outlineCursorLoop(*newFunction, loopBodyBlocks, f, newFunctionArgs,
                        customAggArgs, returnVariable, region->getMetadata());

  auto assign = Make<Assignment>(returnVariable,
                                 f.bindExpression(customAggCaller, returnType));
  ASSERT(nextBasicBlock != nullptr, "NextBasicBlock cannot be nullptr!!");
  nextBasicBlock->insertBefore(nextBasicBlock->begin(), std::move(assign));

  auto &loopHeaderPreds = loopHeader->getPredecessors();
  ASSERT(loopHeaderPreds.size() >= 2,
         "Must have at least two predecessor for region!");
  BasicBlock *pred = nullptr;
  for (auto *p : loopHeaderPreds) {
    if (blocksToOutlineSet.find(p) == blocksToOutlineSet.end()) {
      ASSERT(pred == nullptr,
             "Found more than one pred that is not in the region");
      pred = p;
    }
  }

  // predercessor of next basic block in the loop
  BasicBlock *nextPred = nullptr;
  for (auto *p : nextBasicBlock->getPredecessors()) {
    if (blocksToOutlineSet.find(p) != blocksToOutlineSet.end()) {
      ASSERT(nextPred == nullptr,
             "Found more than one pred that is not in the region");
      nextPred = p;
    }
  }

  pred->renameBasicBlock(loopHeader, nextBasicBlock, nextPred);

  auto *replacement = nextBasicBlock->getRegion();
  auto *currentRegion = loopHeader->getRegion();
  auto *parentRegion = currentRegion->getParentRegion();

  parentRegion->replaceNestedRegion(currentRegion, replacement);

  for (auto *block : blocksToOutline) {
    f.removeBasicBlock(block);
  }

  drawGraph(f.getRegionString(), "after_aggify_region");
  drawGraph(f.getCFGString(), "after_aggify_cfg");

  outlinedCount++;
  return true;
}

bool AggifyPass::runOnRegion(const Region *rootRegion, Function &f) {
  if (rootRegion->getMetadata().find("udf_info") !=
      rootRegion->getMetadata().end()) {
    ASSERT(rootRegion->getMetadata()["udf_info"].get<String>() ==
               "cursorLoopRegion",
           "Unexpected region type: " +
               rootRegion->getMetadata()["udf_info"].get<String>());
    outlineRegion(rootRegion, f);
    return true;
  }
  if (auto recursiveRegion =
          dynamic_cast<const RecursiveRegion *>(rootRegion)) {
    for (auto &region : recursiveRegion->getNestedRegions()) {
      runOnRegion(region, f);
    }
  }
  return true;
}

bool AggifyPass::runOnFunction(Function &f) {
  drawGraph(f.getCFGString(), "before_aggify");
  drawGraph(f.getRegionString(), "before_aggify_region");
  return runOnRegion(f.getRegion(), f);
}