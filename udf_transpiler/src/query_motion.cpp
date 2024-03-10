#include "query_motion.hpp"

RegionDefs QueryMotionPass::computeDefs(const Region *root) const {
  RegionDefs defs;

  auto *header = root->getHeader();
  for (auto &inst : *header) {
    if (auto *def = inst.getResultOperand()) {
      defs[root].insert(def);
    }
  }

  if (auto *rec = dynamic_cast<const RecursiveRegion *>(root)) {
    for (auto *nested : rec->getNestedRegions()) {
      for (auto &[region, regionDefs] : computeDefs(nested)) {
        defs[region].insert(regionDefs.begin(), regionDefs.end());
        defs[root].insert(regionDefs.begin(), regionDefs.end());
      }
    }
  }

  return defs;
}

bool QueryMotionPass::runOnFunction(Function &f) {

  bool changed = false;
  auto regionDefinitions = computeDefs(f.getRegion());

  Set<Assignment *> worklist;
  for (auto &block : f) {
    for (auto &inst : block) {
      if (auto *assign = dynamic_cast<Assignment *>(&inst)) {
        if (assign->getRHS()->isSQLExpression()) {
          // Try hoisting the SQL expression
          worklist.insert(assign);
        }
      }
    }
  }

  while (!worklist.empty()) {
    auto *assign = *worklist.begin();
    worklist.erase(assign);

    // Now we want to check that every variable that we are using is defined
    // outside of the current region
    auto usedVariables = assign->getRHS()->getUsedVariables();
    auto *currentRegion = assign->getParent()->getRegion();
    auto regionDefs = regionDefinitions[currentRegion];

    // If any usedVariable is defined in this region then we can't hoist
    if (std::any_of(usedVariables.begin(), usedVariables.end(),
                    [&](const Variable *usedVar) {
                      return regionDefs.find(usedVar) != regionDefs.end();
                    })) {
      continue;
    }

    // If there's no parent region to hoist to then we can't hoist
    auto *parentRegion = currentRegion->getParentRegion();
    if (!parentRegion) {
      continue;
    }

    changed = true;
    auto *parentHeader = parentRegion->getHeader();

    // create the temporary variable using the LHS of the assignment
    auto *temp = f.createTempVariable(assign->getLHS()->getType(),
                                      assign->getLHS()->isNull());

    // create the new assignment
    auto newAssign = Make<Assignment>(temp, assign->getRHS()->clone());

    // we have to add the condition if it exists
    auto *terminator = parentHeader->getTerminator();
    if (auto *branch = dynamic_cast<const BranchInst *>(terminator)) {
      if (branch->isConditional()) {
        // SELECT <SELECT ...> WHERE <cond>
        auto newRHS = "SELECT (" + assign->getRHS()->getRawSQL() + ") WHERE " +
                      branch->getCond()->getRawSQL();
        newAssign = Make<Assignment>(temp, f.bindExpression(newRHS));
      }
    }

    // add the hoisted instruction to the worklist (we may hoist it further)
    worklist.insert(newAssign.get());

    // do the hoisting
    parentHeader->insertBefore(--parentHeader->end(), std::move(newAssign));

    // finally update the old assignment
    assign->replaceWith(
        Make<Assignment>(assign->getLHS(), f.bindExpression(temp->getName())));
  }

  return changed;
}