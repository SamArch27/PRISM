#pragma once

#include "analysis.hpp"
#include "basic_block.hpp"
#include "dominator_analysis.hpp"
#include "utils.hpp"

class NewRegion {
public:
  NewRegion(BasicBlock *entry, BasicBlock *exit)
      : entry(entry), exit(exit), parent(nullptr) {}

  BasicBlock *getEntry() const { return entry; }
  BasicBlock *getExit() const { return exit; }
  NewRegion *getParent() const { return parent; }
  void setParent(NewRegion *parentRegion) { parent = parentRegion; }

  void addSubregion(NewRegion *subregion) {
    subregion->setParent(this);
    children.emplace_back(Own<NewRegion>(subregion));
  }

private:
  BasicBlock *entry = nullptr;
  BasicBlock *exit = nullptr;
  NewRegion *parent = nullptr;
  Vec<Own<NewRegion>> children;
};

class RegionsAnalysis : public Analysis {
public:
  RegionsAnalysis(Function &f) : Analysis(f) {}

  void runAnalysis() override;

  const Own<NewRegion> &getProgramStructureTree() const {
    return topLevelRegion;
  }

private:
  NewRegion *getTopMostParent(NewRegion *region);
  Own<NewRegion> createRegion(BasicBlock *entry, BasicBlock *exit);
  void buildRegionsTree(BasicBlock *block, NewRegion *region);
  void scanForRegions();
  BasicBlock *getNextPostdom(BasicBlock *block);
  bool isCommonDomFrontier(BasicBlock *block, BasicBlock *entry,
                           BasicBlock *exit);
  bool isRegion(BasicBlock *entry, BasicBlock *exit);
  void findRegionsWithEntry(BasicBlock *entry);
  void insertShortcut(BasicBlock *entry, BasicBlock *exit);

  DominanceFrontier *dominanceFrontier = nullptr;
  DominatorTree *dominatorTree = nullptr;
  DominatorTree *postDominatorTree = nullptr;

  Map<BasicBlock *, NewRegion *> blockToRegion;
  Map<BasicBlock *, BasicBlock *> shortcut;
  Map<NewRegion *, Set<BasicBlock *>> regionToBlocks;
  Own<NewRegion> topLevelRegion;
};