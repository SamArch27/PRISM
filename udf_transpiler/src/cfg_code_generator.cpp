/**
 * @file cfg_code_generator.cpp
 * @brief Generate C++ code from CFG of PL/pgSQL
*/

#include "cfg_code_generator.hpp"
#include "logical_operator_code_generator.hpp"

/**
 * for each instruction in the basic block, generate the corresponding C++ code
*/
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb) {
    string code;
    code += fmt::format("/* ==== Basic block {} start ==== */\n", bb->getLabel());
    code += fmt::format("{}:\n", bb->getLabel());
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

        }
        else if(dynamic_cast<const BranchInst *>(intr->get())){

        }
        else{
            ERROR("Instruction does not fall into a specific type.");
        }
    }
    std::cout<<code<<endl;
}

void CFGCodeGenerator::run(const Function &func) {
    cout<<fmt::format("Generating code for function {}", func.getFunctionName())<<endl;
    for(auto &bb_up : func.getBasicBlocks()){
        basicBlockCodeGenerator(bb_up.get());
    }
    return;
}