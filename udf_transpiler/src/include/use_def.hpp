#pragma once

#include "analysis.hpp"
#include "function.hpp"
#include "utils.hpp"

class UseDefs {
public:
  void addUse(const Variable *var, Instruction *use) { uses[var].insert(use); }
  void addDef(const Variable *var, Instruction *def) {
    ASSERT(defs.find(var) == defs.end(),
           "Must have exactly one definition for a variable in SSA form!");
    defs[var] = def;
  }
  void removeUse(const Variable *var, Instruction *use) {
    uses[var].erase(use);
  }
  void removeDef(const Variable *var, Instruction *def) { defs.erase(var); }

  Set<Instruction *> getUses(const Variable *var) const {
    if (uses.find(var) == uses.end()) {
      return {};
    }
    return uses.at(var);
  }

  Instruction *getDef(const Variable *var) const { return defs.at(var); }

  Set<Instruction *> getAllDefs() const {
    Set<Instruction *> allDefs;
    for (auto &[_, def] : defs) {
      allDefs.insert(def);
    }
    return allDefs;
  }

private:
  Map<const Variable *, Set<Instruction *>> uses;
  Map<const Variable *, Instruction *> defs;
};

class Function;

class UseDefAnalysis : public Analysis {
public:
  UseDefAnalysis(Function &f) : Analysis(f) {}

  const Own<UseDefs> &getUseDefs() { return useDefs; }

  void runAnalysis() override;

private:
  Own<UseDefs> useDefs;
};