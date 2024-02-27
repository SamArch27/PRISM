#pragma once
#include "utils.hpp"

#include "basic_block.hpp"

// Every region has a single entry point (a header)
// It also has a unique parent region
class Region {

protected:
  // Make the constructor protected to ensure no one is creating raw regions
  Region(BasicBlock *header) : header(header) {}

public:
  virtual ~Region() = default;

  BasicBlock *getHeader() const { return header; }
  void setParent(Region *parent) { parentRegion = parent; }
  Region *getParent() const { return parentRegion; }

private:
  Region *parentRegion;
  BasicBlock *header;
};

// A LeafRegion signifies the end of the current region
class LeafRegion : public Region {
public:
  LeafRegion(BasicBlock *header) : Region(header) {}
};

// A SequentialRegion has a header and a nested region
class SequentialRegion : public Region {
public:
  SequentialRegion(BasicBlock *header, Own<Region> nestedRegion)
      : Region(header), nestedRegion(std::move(nestedRegion)) {}

  Region *getNestedRegion() const { return nestedRegion.get(); }

private:
  Own<Region> nestedRegion;
};

// A ConditionalRegion has a header (the condition block) and two nested regions
// with each nested region corresponding to the true/false block
class ConditionalRegion : public Region {
public:
  ConditionalRegion(BasicBlock *header, Own<Region> trueRegion,
                    Own<Region> falseRegion)
      : Region(header), trueRegion(std::move(trueRegion)),
        falseRegion(std::move(falseRegion)) {}

  Region *getTrueRegion() const { return trueRegion.get(); }
  Region *getFalseRegion() const { return falseRegion.get(); }

private:
  Own<Region> trueRegion;
  Own<Region> falseRegion;
};

// A LoopRegion has a header (the entry to the loop) and a nested region
// representing the body of the loop, note that back edges have been removed
class LoopRegion : public Region {
public:
  LoopRegion(BasicBlock *header, Own<Region> bodyRegion)
      : Region(header), bodyRegion(std::move(bodyRegion)) {}

  Region *getBodyRegion() const { return bodyRegion.get(); }

private:
  Own<Region> bodyRegion;
};
