#pragma once

#include "analysis.hpp"
#include "utils.hpp"

class ControlDependenceGraph {
public:
  ControlDependenceGraph(const Vec<String> &blockLabels) : edges() {
    for (const auto &label : blockLabels) {
      edges.insert({label, Set<String>()});
    }
  }

  void addChild(const String &parent, const String &child) {
    edges.at(parent).insert(child);
  }

  friend std::ostream &
  operator<<(std::ostream &os,
             const ControlDependenceGraph &controlDependenceGraph) {
    controlDependenceGraph.print(os);
    return os;
  }

protected:
  void print(std::ostream &os) const {
    os << "Control Dependence Graph: \n" << std::endl;
    os << "digraph cfg {" << std::endl;
    for (const auto &[blockLabel, childrenLabels] : edges) {
      os << "\t" << blockLabel << " [label=\"" << blockLabel << "\"];\n";
      for (const auto &childLabel : childrenLabels) {
        os << "\t" << blockLabel << " -> " << childLabel << ";" << std::endl;
      }
    }
    os << "}" << std::endl;
  }

private:
  Map<String, Set<String>> edges;
};

class ControlDependenceAnalysis : public Analysis {
public:
  ControlDependenceAnalysis(Function &f) : Analysis(f) {}

  void runAnalysis() override;

  const Own<ControlDependenceGraph> &getControlDepdenceGraph() const {
    return dependenceGraph;
  }

private:
  Own<ControlDependenceGraph> dependenceGraph;
};