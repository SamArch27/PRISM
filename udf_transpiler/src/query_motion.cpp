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

  std::cout << "QUERY MOTION" << std::endl;
  std::cout << f << std::endl;
  auto defs = computeDefs(f.getRegion());

  for (auto &[region, regionDefs] : defs) {
    std::cout << "Region " << region->getRegionLabel() << " has defs: ";
    std::cout << "{";
    bool first = true;
    for (auto *def : regionDefs) {
      if (first) {
        first = false;
      } else {
        std::cout << ", ";
      }
      std::cout << def->getName();
    }
    std::cout << "}" << std::endl;
  }

  // TODO:
  // 1. For each SELECT statement in the program
  // 2. Check if the SELECT statement is loop-invariant
  // 2a. Loop-invariant means that it only uses variables outside of the region
  // 2b. If so then create a temporary variable at the pre-header
  // of the region and assign the SELECT with a WHERE predicate added
  // 2c. Replace the original assignment with the assignment to temporary

  // Data structure:
  // Map<Region*, Set<Variable*>> definitions;
  return false;
}