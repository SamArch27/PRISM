#include "query_motion.hpp"

bool QueryMotionPass::runOnFunction(Function &f) {
  // TODO:
  // 1. For each SELECT statement in the program
  // 2. Check if the SELECT statement is loop-invariant
  // 2a. Loop-invariant means that it only uses variables outside of the region
  // 2b. If so then create a temporary variable at the pre-header
  // of the region and assign the SELECT with a WHERE predicate added
  // 2c. Replace the original assignment with the assignment to temporary

  // Data structure:
  // Map<Region*, Set<Variable*>> definitions;
  return true;
}