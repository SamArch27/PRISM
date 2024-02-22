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
  AggifyDFA(const Function &f) : f(f) { analyzeFunction(f); }

  String getReturnVarName() const { return returnVarName; }

  const Variable *getReturnVar() const { return f.getBinding(returnVarName); }

private:
  const Function &f;
  String returnVarName;
  bool analyzeBasicBlock(BasicBlock *bb, const Function &f);
  void analyzeFunction(const Function &f);
};