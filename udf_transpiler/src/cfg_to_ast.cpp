#include "cfg_to_ast.hpp"
#include "compiler_fmt/format.h"

String joinCode(const PLpgSQLContainer &container) {
  return fmt::to_string(fmt::join(container.regionCodes, "\n")) + "\n";
}

/**
 * find the most nested loop region that the basic block belongs to
 */
const LoopRegion *findCurrentLoopRegion(const BasicBlock *bb) {
  ASSERT(bb->getRegion(), "Basic block should have a parent region");
  auto parentRegion = bb->getRegion();
  while (parentRegion) {
    if (auto loopRegion = dynamic_cast<const LoopRegion *>(parentRegion)) {
      return loopRegion;
    }
    parentRegion = parentRegion->getParentRegion();
  }
  return nullptr;
}

/**
 * whether the basic block belongs to the loop region
 */
bool belongToLoopRegion(const BasicBlock *bb, const LoopRegion *loopRegion) {
  ASSERT(bb->getRegion(),
         "Basic block " + bb->getLabel() + " should have a parent region");
  Region *parentRegion = bb->getRegion();
  while (parentRegion) {
    if (parentRegion == loopRegion) {
      return true;
    }
    parentRegion = parentRegion->getParentRegion();
  }
  return false;
}

/**
 * break statement jumps out of the loop region
 */
bool ifInsertBreak(const BasicBlock *currBlock, const BasicBlock *nextBlock) {
  auto loopRegion = findCurrentLoopRegion(currBlock);
  if (loopRegion == nullptr) {
    return false;
  }

  if (belongToLoopRegion(nextBlock, loopRegion)) {
    return false;
  }
  // if the next block does not belong to the loop region
  // insert the break statement
  else {
    return true;
  }
}

/**
 * continue statement jumps to the header of the current loop region
 */
bool ifInsertContinue(const BasicBlock *currBlock,
                      const BasicBlock *nextBlock) {
  auto loopRegion = findCurrentLoopRegion(currBlock);
  if (loopRegion == nullptr) {
    return false;
  }

  // if the next block is the header of the loop region
  // insert the continue statement
  if (nextBlock == loopRegion->getHeader()) {
    return true;
  } else {
    return false;
  }
}

String condBranchCodeGenerator(const BasicBlock *bb,
                               const Instruction *branch) {
  // only generate the condition part of the branch instruction
  // should also ensure that it is the only instruction in the basic block
  ASSERT(dynamic_cast<const BranchInst *>(branch),
         "The instruction should be a branch instruction");
  const auto *branchInst = dynamic_cast<const BranchInst *>(branch);
  return branchInst->getCond()->getRawSQL();
}

/**
 * generate the PL/pgSQL code for a basic block
 * do not code generate binary branch, conditional region should handle it
 */
String basicBlockCodeGenerator(const Function &function, const BasicBlock *bb,
                               const Region *currentRegion, int indent) {
  Vec<String> result;
  for (const auto &inst : *bb) {
    if (auto assign = dynamic_cast<const Assignment *>(&inst)) {
      // if the instruction is an assignment, generate the corresponding
      // PL/pgSQL code
      String code = fmt::format("{} := {};", assign->getLHS()->getName(),
                                assign->getRHS()->getRawSQL());
      result.push_back(code);
    } else if (auto ret = dynamic_cast<const ReturnInst *>(&inst)) {
      // if the instruction is a return instruction, generate the corresponding
      // PL/pgSQL code
      String code = fmt::format("RETURN {};", ret->getExpr()->getRawSQL());
      result.push_back(code);
    } else if (auto branch = dynamic_cast<const BranchInst *>(&inst)) {
      // we only consider adding break and continue statement at jmp position
      if (branch->getSuccessors().size() == 1) {
        auto insertBreak = ifInsertBreak(bb, branch->getSuccessors()[0]);
        auto insertContinue = ifInsertContinue(bb, branch->getSuccessors()[0]);
        ASSERT(!(insertBreak && insertContinue),
               "Cannot insert both break and continue statement");
        if (insertBreak) {
          result.push_back("EXIT;");
        }
        if (insertContinue) {
          result.push_back("CONTINUE;");
        }
      } else {
        // do nothing for the binary branch
      }
    } else {
      ERROR("Unexpected instruction type");
    }
  }
  return fmt::to_string(fmt::join(result, "\n"));
}

/**
 * recursively generate the PL/pgSQL code for the regions
 */
void PLpgSQLGenerator::regionCodeGenerator(const Function &function,
                                           PLpgSQLContainer &container,
                                           const Region *region, int indent) {
  // switch the region type
  if (auto currentRegion = dynamic_cast<const SequentialRegion *>(region)) {
    // generate code for the header
    auto headerCode = basicBlockCodeGenerator(
        function, currentRegion->getHeader(), currentRegion, indent);
    container.regionCodes.push_back(headerCode);

    // then generate code for the nested regions in order
    regionCodeGenerator(function, container, currentRegion->getNestedRegion(),
                        indent);
    if (currentRegion->getFallthroughRegion()) {
      regionCodeGenerator(function, container,
                          currentRegion->getFallthroughRegion(), indent);
    }
  } else if (auto currentRegion = dynamic_cast<const LeafRegion *>(region)) {
    // if the region is a leaf region, generate the code for the basic block
    auto headerCode = basicBlockCodeGenerator(
        function, currentRegion->getHeader(), currentRegion, indent);
    container.regionCodes.push_back(headerCode);
  } else if (auto currentRegion = dynamic_cast<const LoopRegion *>(region)) {
    // if the region is a loop region, generate the code for the loop
    auto headerCode = basicBlockCodeGenerator(
        function, currentRegion->getHeader(), currentRegion, indent);
    PLpgSQLContainer loopContainer;
    regionCodeGenerator(function, loopContainer, currentRegion->getBodyRegion(),
                        indent);
    auto loopCode = fmt::format("LOOP\n{}\n{}END LOOP;\n", headerCode,
                                joinCode(loopContainer));
    container.regionCodes.push_back(loopCode);
  } else if (auto currentRegion =
                 dynamic_cast<const ConditionalRegion *>(region)) {
    auto condBlock = currentRegion->getHeader();
    // if the region is a conditional region, generate the code for the
    // conditional
    auto headerCode =
        basicBlockCodeGenerator(function, condBlock, currentRegion, indent);
    container.regionCodes.push_back(headerCode);
    auto condCode =
        condBranchCodeGenerator(condBlock, condBlock->getTerminator());
    if (currentRegion->getFalseRegion() == nullptr) {
      // if the conditional region does not have a false region, it is a if
      // statement
      PLpgSQLContainer trueContainer;
      regionCodeGenerator(function, trueContainer,
                          currentRegion->getTrueRegion(), indent);

      // other kind of jmp is already handled in the basic block code generator
      String insert = "";
      BasicBlock *falseBlock = nullptr;
      for (auto &succ : condBlock->getSuccessors()) {
        if (succ != currentRegion->getTrueRegion()->getHeader()) {
          falseBlock = succ;
          break;
        }
      }
      ASSERT(condBlock->getSuccessors().size() == 2 &&
                 falseBlock != currentRegion->getTrueRegion()->getHeader(),
             "Unexpected conditional region structure");
      auto insertBreak = ifInsertBreak(condBlock, falseBlock);
      auto insertContinue = ifInsertContinue(condBlock, falseBlock);
      ASSERT(!(insertBreak && insertContinue),
             "Cannot insert both break and continue statement");
      if (insertBreak) {
        insert = "EXIT;";
      }
      if (insertContinue) {
        insert = "CONTINUE;";
      }

      auto ifCode = fmt::format("IF {} THEN\n{}END IF;\n{}", condCode,
                                joinCode(trueContainer), insert);
      container.regionCodes.push_back(ifCode);
    } else {
      // if the conditional region has a false region, it is a if-else statement
      PLpgSQLContainer trueContainer, falseContainer;
      regionCodeGenerator(function, trueContainer,
                          currentRegion->getTrueRegion(), indent);
      regionCodeGenerator(function, falseContainer,
                          currentRegion->getFalseRegion(), indent);
      auto ifElseCode =
          fmt::format("IF {} THEN\n{}ELSE\n{}END IF;", condCode,
                      joinCode(trueContainer), joinCode(falseContainer));
      container.regionCodes.push_back(ifElseCode);
    }
  } else {
    ERROR("Unexpected region type");
  }
}

void PLpgSQLGenerator::bodyCodeGenerator(const Function &function,
                                         PLpgSQLContainer &container) {
  return regionCodeGenerator(function, container, function.getRegion(), 0);
}

/**
 * based on the region of the cfg, generate the corresponding PL/pgSQL code
 */
PLpgSQLGeneratorResult PLpgSQLGenerator::run(const Function &function) {

  PLpgSQLContainer container;

  // generate the region code
  bodyCodeGenerator(function, container);
  String body = joinCode(container);

  Vec<String> functionArgs;

  // generate the function arguments
  for (const auto &arg : function.getArguments()) {
    functionArgs.push_back(
        fmt::format("{} {}", arg->getName(), arg->getType().getDuckDBType()));
  }

  Vec<String> varDeclarations;

  // generate the function declare section
  for (const auto &var : function.getVariables()) {
    varDeclarations.push_back(fmt::format("{} {};\n", var->getName(),
                                          var->getType().getDuckDBType()));
  }

  // generate the wrapper of the function
  String code = fmt::format(
      fmt::runtime(config.plpgsql["functionTemplate"].Scalar()),
      fmt::arg("functionName", function.getFunctionName()),
      fmt::arg("returnType", function.getReturnType().getDuckDBType()),
      fmt::arg("functionArgs", joinVector(functionArgs, ", ")),
      fmt::arg("varDeclarations", joinVector(varDeclarations, "")),
      fmt::arg("body", body));

  return {code};
}