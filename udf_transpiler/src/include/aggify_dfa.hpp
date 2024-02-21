/**
 * @file aggify_dfa.hpp
 * Implements Data Flow Analysis required for Aggify
 */

#pragma once
#include "function.hpp"
#include "instructions.hpp"
#include "utils.hpp"

class AggifyDFA {
public:
  AggifyDFA(const Function &function) : func(function) {
    analyzeFunction(func);
  }

  String getReturnVarName() const { return returnVarName; }

  const Variable *getReturnVar() const {
    return func.getBinding(returnVarName);
  }

private:
  const Function &func;
  String returnVarName;
  bool analyzeBasicBlock(BasicBlock *bb, const Function &func);
  void analyzeFunction(const Function &function);
};