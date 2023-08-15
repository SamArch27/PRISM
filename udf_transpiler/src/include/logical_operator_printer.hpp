#pragma once
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"
#define FMT_HEADER_ONLY
#include "include/fmt/core.h"
#include <iostream>

namespace udf{
class LogicalOperatorPrinter : public duckdb::LogicalOperatorVisitor {
private:
    // duckdb::LogicalOperator *op;
public:
    LogicalOperatorPrinter(){
        // do nothing
    }

    void VisitOperatorHelper(duckdb::LogicalOperator &op, int indent) {
        std::cout<<fmt::format(fmt::runtime("{:<{}}{}[{}]"), "", indent, op.GetName())<<std::endl;
        for(auto &child : op.children){
            VisitOperatorHelper(*child, indent + 4);
        }
    }

    void VisitOperator(duckdb::LogicalOperator &op) override {
        return VisitOperatorHelper(op, 0);
    }
    // void VisitExpression(duckdb::unique_ptr<duckdb::Expression> *expression) override;
};
}