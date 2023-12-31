/**
 * @file cfg_code_generator.cpp
 * @brief Generate C++ code from CFG of PL/pgSQL
*/

#include "cfg_code_generator.hpp"
#include "logical_operator_code_generator.hpp"
#include "types.hpp"

string CFGCodeGenerator::createReturnValue(const std::string &retName, const Type *retType, const string &retValue) {
    // string retName = retVar.getName();
    // auto retType = retVar.getType();
    if (dynamic_cast<const DecimalType *>(retType)) {
        auto width = dynamic_cast<const DecimalType *>(retType)->getWidth();
        auto scale = dynamic_cast<const DecimalType *>(retType)->getScale();
        return fmt::format("{} = Value::DECIMAL({}, {}, {})", retName, retValue,
                        width, scale);
    } else if (retType->isNumeric()) {
        return fmt::format("{} = Value::{}({})", retName, retType->toString(), retValue);
    } else if (retType->isBLOB()) {
        return fmt::format(
            "{0} = Value({1});\n{0}.GetTypeMutable() = LogicalType::{2}", retName,
            retValue, retType->toString());
    } else {
        ERROR(fmt::format("Cannot create duckdb value from type {}", retType->toString()));
    }
}

/**
 * for each instruction in the basic block, generate the corresponding C++ code
*/
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb, const Function &func) {
    string code;
    code += fmt::format("/* ==== Basic block {} start ==== */\n", bb->getLabel());
    code += fmt::format("{}:\n", bb->getLabel());
    // if(bb->getLabel() == "entry"){
    //     // do nothing
    //     goto end;
    // }
    if(bb->getLabel() == "exit"){
        code += fmt::format("return;\n");
        goto end;
    }
    for(BasicBlock::InstIterator intr = bb->begin(); intr != bb->end(); intr++){
        // const Instruction &inst = *intr->get();
        if(dynamic_cast<const Assignment *>(intr->get())){
            const Assignment *assign = dynamic_cast<const Assignment *>(intr->get());
            // code += fmt::format("{} = {};\n", assign->getLvalue(), assign->getRvalue());
            duckdb::LogicalOperatorCodeGenerator locg;
            locg.VisitOperator((Expression &)*(assign->getExpr()));
            auto [header, res] = locg.getResult();
            code += header;
            code += fmt::format("{} = {};\n", assign->getVar()->getName(), res);
        }
        else if(dynamic_cast<const ReturnInst *>(intr->get())){
            const ReturnInst *ret = dynamic_cast<const ReturnInst *>(intr->get());
            duckdb::LogicalOperatorCodeGenerator locg;
            locg.VisitOperator((Expression &)*(ret->getExpr()));
            auto [header, res] = locg.getResult();
            code += header;
            code += fmt::format(
                "{};\ngoto exit;\n", createReturnValue(config.function["return_name"].Scalar(), func.getReturnType(), res)
            );
        }
        else if(dynamic_cast<const BranchInst *>(intr->get())){
            const BranchInst *br = dynamic_cast<const BranchInst *>(intr->get());
            if(br->isConditional()){
                duckdb::LogicalOperatorCodeGenerator locg;
                locg.VisitOperator((Expression &)*(br->getCond()));
                auto [header, res] = locg.getResult();
                code += header;
                code += fmt::format("if({}) goto {};\n", res, br->getIfTrue()->getLabel());
                code += fmt::format("goto {};\n", br->getIfFalse()->getLabel());
            }
            else{
                code += fmt::format("goto {};\n", br->getIfTrue()->getLabel());
            }
        }
        else{
            ERROR("Instruction does not fall into a specific type.");
        }
    }
end:
    std::cout<<code<<endl;
    //todo: insert code to the container
    return;
}

void CFGCodeGenerator::run(const Function &func) {
    cout<<fmt::format("Generating code for function {}", func.getFunctionName())<<endl;
    // std::ostringstream oss;
    // func.print(oss);
    // cout<<oss.str()<<endl;
    for(auto &bb_up : func.getBasicBlocks()){
        basicBlockCodeGenerator(bb_up.get(), func);
    }
    return;
}