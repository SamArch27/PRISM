#pragma once
#include "utils.hpp"

#include "basic_block.hpp"

class RecursiveRegion;

// Every region has a single entry point (a header)
// It also has a unique parent region
class Region {
protected:
  // Make the constructor protected to ensure no one is creating raw regions
  Region(BasicBlock *header, bool attach) : header(header) {
    if (attach) {
      header->setRegion(this);
    }
  }

public:
  friend std::ostream &operator<<(std::ostream &os, const Region &region) {
    region.print(os);
    return os;
  }

  virtual ~Region() = default;
  virtual void print(std::ostream &os) const = 0;
  virtual String getRegionLabel() const = 0;

  bool containsSELECT() const {
    for (auto &inst : *header) {
      if (auto *assign = dynamic_cast<Assignment *>(&inst)) {
        if (assign->getRHS()->isSQLExpression()) {
          return true;
        }
      }
    }
    return false;
  }
  BasicBlock *getHeader() const { return header; }
  void setParentRegion(RecursiveRegion *parent) { parentRegion = parent; }
  RecursiveRegion *getParentRegion() const { return parentRegion; }

  virtual bool hasSelect() const = 0;

private:
  RecursiveRegion *parentRegion = nullptr;
  BasicBlock *header;
};

class NonRecursiveRegion : public Region {
protected:
  NonRecursiveRegion(BasicBlock *header, bool attach)
      : Region(header, attach) {}

public:
  virtual ~NonRecursiveRegion() = default;
  virtual void print(std::ostream &os) const override = 0;
  virtual String getRegionLabel() const override = 0;

  bool hasSelect() const override { return getHeader()->hasSelect(); }
};

class RecursiveRegion : public Region {
protected:
  template <typename... Args>
  RecursiveRegion(BasicBlock *header, bool attach, Args... args)
      : Region(header, attach) {
    Own<Region> tmp[] = {std::move(args)...};
    for (auto &region : tmp) {
      if (region) {
        region->setParentRegion(this);
      }
      nestedRegions.push_back(std::move(region));
    }
  }

public:
  virtual ~RecursiveRegion() = default;
  virtual void print(std::ostream &os) const override = 0;
  virtual String getRegionLabel() const override = 0;

  Vec<const Region *> getNestedRegions() const {
    Vec<const Region *> result;
    for (auto &region : nestedRegions) {
      if (region) {
        result.push_back(region.get());
      }
    }
    return result;
  }

  void releaseNestedRegions() {
    for (auto &region : nestedRegions) {
      region.release();
    }
  }

  void replaceNestedRegion(RecursiveRegion *toReplace, Region *newRegion) {
    toReplace->releaseNestedRegions();
    for (auto &region : nestedRegions) {
      if (region.get() == toReplace) {
        newRegion->setParentRegion(this);
        region.reset(newRegion);
      }
    }
  }

  bool hasSelect() const override {
    COUT<< "hasSelect: " << this->getRegionLabel() << ENDL;
    if (getHeader()->hasSelect()) {
      return true;
    }
    for (auto &region : nestedRegions) {
      if (region && region->hasSelect()) {
        return true;
      }
    }
    return false;
  }

protected:
  VecOwn<Region> nestedRegions;
};

// A LeafRegion represents a single basic block
class LeafRegion : public NonRecursiveRegion {
public:
  LeafRegion(BasicBlock *header) : NonRecursiveRegion(header, true) {}

  String getRegionLabel() const override {
    return "L" + getHeader()->getLabel().substr(1);
  }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
  }
};

// A DummyRegion holds a basic block but is never part of the region hierarchy
class DummyRegion : public NonRecursiveRegion {
public:
  DummyRegion(BasicBlock *header) : NonRecursiveRegion(header, false) {}

  String getRegionLabel() const override {
    return "D" + getHeader()->getLabel().substr(1);
  }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
  }
};

// A SequentialRegion has a header and a nested region
class SequentialRegion : public RecursiveRegion {
public:
  SequentialRegion(BasicBlock *header, Own<Region> nested,
                   Own<Region> fallthrough = nullptr)
      : RecursiveRegion(header, true, std::move(nested),
                        std::move(fallthrough)) {}

  String getRegionLabel() const override {
    return "SR" + getHeader()->getLabel().substr(1);
  }

  Region *getNestedRegion() const { return nestedRegions[0].get(); }
  Region *getFallthroughRegion() const { return nestedRegions[1].get(); }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    os << "\t" << getNestedRegion()->getRegionLabel() << " [label=\""
       << getNestedRegion()->getRegionLabel() << "\"];";
    if (getFallthroughRegion()) {
      os << "\t" << getNestedRegion()->getRegionLabel() << " [label=\""
         << getNestedRegion()->getRegionLabel() << "\"];";
    }
    os << "\t" << getHeader()->getLabel() << " -> "
       << getNestedRegion()->getRegionLabel() << ";\n";
    if (getFallthroughRegion()) {
      os << "\t" << getNestedRegion()->getRegionLabel() << " -> "
         << getFallthroughRegion()->getRegionLabel() << ";\n";
    }

    os << *getNestedRegion();
    if (getFallthroughRegion()) {
      os << *getFallthroughRegion();
    }
  }
};

// A ConditionalRegion has a header (the condition block) and two nested regions
// with each nested region corresponding to the true/false block
class ConditionalRegion : public RecursiveRegion {
public:
  ConditionalRegion(BasicBlock *header, Own<Region> trueRegion,
                    Own<Region> falseRegion = nullptr)
      : RecursiveRegion(header, true, std::move(trueRegion),
                        std::move(falseRegion)) {}

  String getRegionLabel() const override {
    return "CR" + getHeader()->getLabel().substr(1);
  }

  Region *getTrueRegion() const { return nestedRegions[0].get(); }
  Region *getFalseRegion() const { return nestedRegions[1].get(); }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    os << "\t" << getTrueRegion()->getRegionLabel() << " [label=\""
       << getTrueRegion()->getRegionLabel() << "\"];";
    os << "\t" << getHeader()->getLabel() << " -> "
       << getTrueRegion()->getRegionLabel() << ";\n";
    if (getFalseRegion()) {
      os << "\t" << getFalseRegion()->getRegionLabel() << " [label=\""
         << getFalseRegion()->getRegionLabel() << "\"];";
      os << "\t" << getHeader()->getLabel() << " -> "
         << getFalseRegion()->getRegionLabel() << ";\n";
    }

    os << *getTrueRegion();
    if (getFalseRegion()) {
      os << *getFalseRegion();
    }
  }
};

// A LoopRegion has a header (the entry to the loop) and a nested region
// representing the body of the loop, note that back edges have been removed
class LoopRegion : public RecursiveRegion {
public:
  LoopRegion(BasicBlock *header, Own<Region> bodyRegion)
      : RecursiveRegion(header, true, std::move(bodyRegion)) {}

  String getRegionLabel() const override {
    return "LR" + getHeader()->getLabel().substr(1);
  }

  Region *getBodyRegion() const { return nestedRegions[0].get(); }

protected:
  void print(std::ostream &os) const override {
    os << "\t" << getHeader()->getLabel() << " [label=\""
       << getHeader()->getLabel() << "\"];";
    os << "\t" << getBodyRegion()->getRegionLabel() << " [label=\""
       << getBodyRegion()->getRegionLabel() << "\"];";
    os << "\t" << getHeader()->getLabel() << " -> "
       << getBodyRegion()->getRegionLabel() << ";\n";
    os << *getBodyRegion();
  }
};
