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
  friend std::ostream &operator<<(std::ostream &os, const Region &region) {
    region.print(os);
    return os;
  }

  virtual ~Region() = default;
  virtual void print(std::ostream &os) const = 0;
  virtual String getRegionLabel() const = 0;

  BasicBlock *getHeader() const { return header; }
  void setParent(Region *parent) { parentRegion = parent; }
  Region *getParent() const { return parentRegion; }

private:
  Region *parentRegion;
  BasicBlock *header;
};

// A LeafRegion represents a single basic block
class LeafRegion : public Region {
public:
  LeafRegion(BasicBlock *header) : Region(header) {}

  String getRegionLabel() const override {
    return "L" + getHeader()->getLabel().substr(1);
  }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
  }
};

// A SequentialRegion has a header and a nested region
class SequentialRegion : public Region {
public:
  SequentialRegion(BasicBlock *header, Own<Region> inputRegion)
      : Region(header), nestedRegions() {
    nestedRegions.emplace_back(std::move(inputRegion));
    nestedRegions[0]->setParent(this);
  }

  template <typename... Args>
  SequentialRegion(BasicBlock *header, Args... args) : Region(header) {
    Own<Region> tmp[] = {std::move(args)...};
    for (auto &region : tmp) {
      if (region) {
        nestedRegions.emplace_back(std::move(region));
      }
    }
    for (std::size_t i = 0; i < nestedRegions.size(); i++) {
      nestedRegions[i]->setParent(this);
    }
  }

  String getRegionLabel() const override {
    return "SR" + getHeader()->getLabel().substr(1);
  }

  Vec<Region *> getNestedRegions() const {
    Vec<Region *> result;
    for (auto &nestedRegion : nestedRegions) {
      result.push_back(nestedRegion.get());
    }
    return result;
  }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    for (auto &nestedRegion : nestedRegions) {
      os << "\t" << nestedRegion->getRegionLabel() << " [label=\""
         << nestedRegion->getRegionLabel() << "\"];";
    }
    os << "\t" << getHeader()->getLabel() << " -> "
       << nestedRegions[0]->getRegionLabel() << ";\n";
    for (std::size_t i = 1; i < nestedRegions.size(); ++i) {
      os << "\t" << nestedRegions[i - 1]->getRegionLabel() << " -> "
         << nestedRegions[i]->getRegionLabel() << ";\n";
    }

    for (auto &nestedRegion : nestedRegions) {
      os << *nestedRegion;
    }
  }

private:
  VecOwn<Region> nestedRegions;
};

// A ConditionalRegion has a header (the condition block) and two nested regions
// with each nested region corresponding to the true/false block
class ConditionalRegion : public Region {
public:
  ConditionalRegion(BasicBlock *header, Own<Region> inputTrueRegion,
                    Own<Region> inputFalseRegion = nullptr)
      : Region(header), trueRegion(std::move(inputTrueRegion)),
        falseRegion(std::move(inputFalseRegion)) {
    trueRegion->setParent(this);
    if (falseRegion) {
      falseRegion->setParent(this);
    }
  }

  String getRegionLabel() const override {
    return "CR" + getHeader()->getLabel().substr(1);
  }

  Region *getTrueRegion() const { return trueRegion.get(); }
  Region *getFalseRegion() const { return falseRegion.get(); }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    os << "\t" << trueRegion->getRegionLabel() << " [label=\""
       << trueRegion->getRegionLabel() << "\"];";
    os << "\t" << getHeader()->getLabel() << " -> "
       << trueRegion->getRegionLabel() << ";\n";
    if (falseRegion) {
      os << "\t" << falseRegion->getRegionLabel() << " [label=\""
         << falseRegion->getRegionLabel() << "\"];";
      os << "\t" << getHeader()->getLabel() << " -> "
         << falseRegion->getRegionLabel() << ";\n";
    }

    os << *trueRegion;
    if (falseRegion) {
      os << *falseRegion;
    }
  }

private:
  Own<Region> trueRegion;
  Own<Region> falseRegion;
};

// A LoopRegion has a header (the entry to the loop) and a nested region
// representing the body of the loop, note that back edges have been removed
class LoopRegion : public Region {
public:
  LoopRegion(BasicBlock *header, Own<Region> inputBodyRegion)
      : Region(header), bodyRegion(std::move(inputBodyRegion)) {
    bodyRegion->setParent(this);
  }

  String getRegionLabel() const override {
    return "LR" + getHeader()->getLabel().substr(1);
  }

  Region *getBodyRegion() const { return bodyRegion.get(); }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    os << "\t" << bodyRegion->getRegionLabel() << " [label=\""
       << bodyRegion->getRegionLabel() << "\"];";
    os << "\t" << getHeader()->getLabel() << " -> "
       << bodyRegion->getRegionLabel() << ";\n";
    os << *bodyRegion;
  }

private:
  Own<Region> bodyRegion;
};
