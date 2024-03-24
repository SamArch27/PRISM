#include "query_motion.hpp"
#include "types.hpp"

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

  // Handle the simplest case of a SELECT in a condition
  for (auto &block : f) {
    for (auto it = block.begin(); it != block.end(); ++it) {
      auto &inst = *it;
      if (auto *branch = dynamic_cast<BranchInst *>(&inst)) {
        if (branch->isUnconditional()) {
          continue;
        }
        auto *cond = branch->getCond();
        // we want to pull this out to its own boolean variable
        if (cond->isSQLExpression()) {

          // mark the change
          changed = true;

          // create a new boolean temp variable
          auto *temp = f.createTempVariable(
              Type(false, std::nullopt, std::nullopt, PostgresTypeTag::BOOLEAN),
              false);

          // and assignment temp = SELECT ...
          auto newAssign = Make<Assignment>(temp, cond->clone());
          auto preds = block.getPredecessors();
          ASSERT(preds.size() == 1,
                 "Must have unique predecessor for conditional block!");
          auto *predBlock = block.getPredecessors().front();

          // insert the assignment at the end of the previous block
          predBlock->insertBeforeTerminator(std::move(newAssign));

          // replace the condition i.e. if (SELECT ..) with if (temp)
          it = block.replaceInst(
              it, Make<BranchInst>(
                      branch->getIfTrue(), branch->getIfFalse(),
                      f.bindExpression(temp->getName(), Type::BOOLEAN)));
        }
      }
    }
  }

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

    Vec<const SelectExpression *> conds;
    Region *highestRegion = nullptr;
    while (true) {
      auto regionDefs = regionDefinitions[currentRegion];

      // If any usedVariable is defined in this region then we can't hoist
      if (std::any_of(usedVariables.begin(), usedVariables.end(),
                      [&](const Variable *usedVar) {
                        return regionDefs.find(usedVar) != regionDefs.end();
                      })) {
        break;
      }

      // If there's no parent region to hoist to then we can't hoist
      auto *parentRegion = currentRegion->getParentRegion();
      if (!parentRegion) {
        break;
      }

      auto *parentHeader = parentRegion->getHeader();
      if (parentHeader == f.getEntryBlock()) {
        break;
      }

      // keep track of the variables used by conditions
      auto *terminator = parentHeader->getTerminator();
      if (auto *branch = dynamic_cast<const BranchInst *>(terminator)) {
        if (branch->isConditional()) {
          auto condVariables = branch->getCond()->getUsedVariables();
          usedVariables.insert(condVariables.begin(), condVariables.end());
          conds.push_back(branch->getCond());
        }
      }

      // Don't hoist to join points
      if (parentHeader->getPredecessors().size() <= 1) {
        highestRegion = currentRegion;
      }

      currentRegion = currentRegion->getParentRegion();
    }

    // Failed to hoist
    if (!highestRegion) {
      continue;
    }

    // We can always hoist one higher
    if (highestRegion->getParentRegion()) {
      highestRegion = highestRegion->getParentRegion();
    }

    changed = true;

    // create the temporary variable using the LHS of the assignment
    auto *temp = f.createTempVariable(assign->getLHS()->getType(),
                                      assign->getLHS()->isNull());

    // create the new assignment
    auto newAssign = Make<Assignment>(temp, assign->getRHS()->clone());

    // we have to add the condition if it exists
    auto newRHS = assign->getRHS()->getRawSQL();
    String condString = "";
    for (auto *cond : conds) {
      if (condString != "") {
        condString += " AND ";
      }
      condString += "(" + cond->getRawSQL() + ")";
    }

    if (condString != "") {
      newRHS =
          "SELECT (" + assign->getRHS()->getRawSQL() + ") WHERE " + condString;
    }

    // make the temporary
    newAssign = Make<Assignment>(
        temp, f.bindExpression(newRHS, assign->getRHS()->getReturnType()));

    // do the hoisting
    highestRegion->getHeader()->insertBeforeTerminator(std::move(newAssign));

    // finally update the old assignment
    assign->replaceWith(Make<Assignment>(
        assign->getLHS(),
        f.bindExpression(temp->getName(), assign->getLHS()->getType())));
  }
  return changed;
}