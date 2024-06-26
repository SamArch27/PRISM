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

void RegionsAnalysis::findRegionsWithEntry(BasicBlock *entry) {
  ASSERT(entry, "Entry should not be nullptr!\n");

  Own<NewRegion> lastRegion = nullptr;
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
        newRegion->addSubregion(std::move(lastRegion));
      }

      lastRegion = std::move(newRegion);
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
      reversedBlock->addSuccessor(pred);
    }
    for (auto *succ : block.getSuccessors()) {
      reversedBlock->addPredecessor(succ);
    }
  }
  auto postDominatorAnalysis = Make<DominatorAnalysis>(reversed);
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