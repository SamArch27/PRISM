#include "regions_analysis.hpp"
#include "dominator_analysis.hpp"
#include "utils.hpp"

NewRegion *RegionsAnalysis::getTopMostParent(NewRegion *region) {
  while (region->getParent()) {
    region = region->getParent();
  }
  return region;
}

Own<NewRegion> RegionsAnalysis::createRegion(BasicBlock *entry,
                                             BasicBlock *exit) {
  auto region = Make<NewRegion>(entry, exit);
  blockToRegion.insert({entry, region.get()});
  return region;
}

void RegionsAnalysis::insertShortcut(BasicBlock *entry, BasicBlock *exit) {
  ASSERT(entry != nullptr && exit != nullptr,
         "Entry and exit must not be nullptr!\n");
  auto it = shortcut.find(exit);
  if (it == shortcut.end()) {
    shortcut[entry] = exit;
  } else {
    // prefer the larger region (entry,it->second) to (entry,exit)
    shortcut[entry] = it->second;
  }
}

String RegionsAnalysis::getNextPostdom(BasicBlock *block) {
  auto it = shortcut.find(block);
  if (it == shortcut.end()) {
    return postDominatorTree->getParent(block->getLabel());
  }
  return postDominatorTree->getParent(it->second->getLabel());
}

bool RegionsAnalysis::isCommonDomFrontier(BasicBlock *block, BasicBlock *entry,
                                          BasicBlock *exit) {
  for (auto *pred : block->getPredecessors()) {
    if (dominatorTree->dominates(entry->getLabel(), pred->getLabel()) &&
        !dominatorTree->dominates(exit->getLabel(), pred->getLabel())) {
      return false;
    }
  }
  return true;
}

bool RegionsAnalysis::isRegion(BasicBlock *entry, BasicBlock *exit) {
  ASSERT(entry != nullptr && exit != nullptr,
         "entry and exit must not be nullptr!\n");

  auto &entrySuccs = dominanceFrontier->getFrontier(entry);

  // Exit is the header of a loop that contains the entry. In this case,
  // the dominance frontier must only contain the exit.
  if (!dominatorTree->dominates(entry->getLabel(), exit->getLabel())) {
    for (auto *succ : entrySuccs) {
      if (succ != exit && succ != entry) {
        return false;
      }
    }
    return true;
  }

  auto &exitSuccs = dominanceFrontier->getFrontier(exit);

  // Do not allow edges leaving the region.
  for (auto *succ : entrySuccs) {
    if (succ == exit || succ == entry) {
      continue;
    }
    bool containsSucc = (exitSuccs.find(succ) != exitSuccs.end());
    if (!containsSucc) {
      return false;
    }
    if (!isCommonDomFrontier(succ, entry, exit)) {
      return false;
    }
  }

  // Do not allow edges pointing into the region.
  for (auto *succ : exitSuccs) {
    bool dominates =
        dominatorTree->dominates(entry->getLabel(), succ->getLabel());
    bool strictlyDominates = dominates && (entry != succ);
    if (strictlyDominates && exit != succ) {
      return false;
    }
  }

  return true;
}

void RegionsAnalysis::findRegionsWithEntry(BasicBlock *entry) {
  ASSERT(entry, "Entry should not be nullptr!\n");

  NewRegion *lastRegion = nullptr;
  BasicBlock *lastExit = entry;

  BasicBlock *exit = entry;

  while (true) {
    auto nextLabel = getNextPostdom(exit);
    if (nextLabel.empty()) {
      break;
    }
    exit = f.getBlockFromLabel(nextLabel);

    if (isRegion(entry, exit)) {
      auto newRegion = createRegion(entry, exit);

      if (lastRegion) {
        newRegion->addSubregion(lastRegion);
      }

      lastRegion = newRegion.release();
      lastExit = exit;
    }

    // This can never be a region, so stop the search.
    if (!dominatorTree->dominates(entry->getLabel(), exit->getLabel())) {
      break;
    }
  }
  if (lastExit != entry) {
    insertShortcut(entry, lastExit);
  }
}

void RegionsAnalysis::scanForRegions() {
  // Compute post order
  Vec<BasicBlock *> postorder;
  Set<BasicBlock *> visited;
  std::function<void(BasicBlock *)> postorderTraversal = [&](BasicBlock *root) {
    visited.insert(root);
    for (auto *succ : root->getSuccessors()) {
      if (visited.find(succ) == visited.end()) {
        postorderTraversal(succ);
      }
    }
    postorder.push_back(root);
  };
  postorderTraversal(f.getEntryBlock());

  // For each node in post order
  for (auto *block : postorder) {
    findRegionsWithEntry(block);
  }
}

void RegionsAnalysis::buildRegionsTree(BasicBlock *block, NewRegion *region) {
  while (block == region->getExit()) {
    region = region->getParent();
  }

  auto it = blockToRegion.find(block);

  // no region found for block
  if (it == blockToRegion.end()) {
    blockToRegion[block] = region;
    regionToBlocks[region].insert(block);
  } else {
    auto *newRegion = it->second;
    region->addSubregion(getTopMostParent(newRegion));
    region = newRegion;
  }

  // traverse the children using the dominator tree
  for (auto &childLabel : dominatorTree->getChildren(block->getLabel())) {
    buildRegionsTree(f.getBlockFromLabel(childLabel), region);
  }
};

void RegionsAnalysis::runAnalysis() {

  // Add artifical exit block
  auto *exitBlock = f.makeBasicBlock("exit");
  for (auto &block : f) {
    if (&block == exitBlock) {
      continue;
    }
    if (block.getSuccessors().empty()) {
      block.addSuccessor(exitBlock);
      exitBlock->addPredecessor(&block);
    }
  }

  // Compute dominator tree
  auto dominatorAnalysis = Make<DominatorAnalysis>(f);
  dominatorAnalysis->runAnalysis();
  dominatorTree = dominatorAnalysis->getDominatorTree().get();
  dominanceFrontier = dominatorAnalysis->getDominanceFrontier().get();
  std::cout << "Original function:" << std::endl;
  std::cout << f << std::endl;
  std::cout << *dominatorTree << std::endl;

  // Compute post-dominator tree
  auto reversed =
      Make<Function>(f.getConnection(), f.getFunctionName(), f.getReturnType());
  for (auto &block : f) {
    reversed->makeBasicBlock(block.getLabel());
  }
  for (auto &block : f) {
    auto *reversedBlock = reversed->getBlockFromLabel(block.getLabel());
    for (auto *pred : block.getPredecessors()) {
      auto *reversedPred = reversed->getBlockFromLabel(pred->getLabel());
      reversedBlock->addSuccessor(reversedPred);
    }
    for (auto *succ : block.getSuccessors()) {
      auto *reversedSucc = reversed->getBlockFromLabel(succ->getLabel());
      reversedBlock->addPredecessor(reversedSucc);
    }
  }

  std::cout << "Reversed function:" << std::endl;
  std::cout << *reversed << std::endl;
  std::cout << "Done!" << std::endl;

  auto postDominatorAnalysis = Make<DominatorAnalysis>(*reversed);
  postDominatorAnalysis->runAnalysis();
  postDominatorTree = postDominatorAnalysis->getDominatorTree().get();

  scanForRegions();
  topLevelRegion = Make<NewRegion>(f.getEntryBlock(), nullptr);
  buildRegionsTree(f.getEntryBlock(), topLevelRegion.get());

  std::cout << f << std::endl;

  int i = 0;
  for (auto &[region, blocks] : regionToBlocks) {
    std::cout << "Region " << i << " has blocks: " << std::endl;
    for (auto block : blocks) {
      std::cout << block->getLabel() << " ";
    }
    std::cout << std::endl;
    ++i;
  }

  // TODO:
  // 1. Define region data structure
  // 2. Construct regions (following LLVM)
  // 3. Figure out the correspondence with leaf/conditional/loop/sequential

  // Delete artificial exit block
  Vec<BasicBlock *> preds = exitBlock->getPredecessors();
  for (auto *pred : preds) {
    exitBlock->removePredecessor(pred);
    pred->removeSuccessor(exitBlock);
  }
  f.removeBasicBlock(exitBlock);
}