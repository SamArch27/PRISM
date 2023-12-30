/**
 * @file cfg_code_generator.cpp
 * @brief Generate C++ code from CFG of PL/pgSQL
*/

#include "cfg_code_generator.hpp"

/**
 * for each instruction in the basic block, generate the corresponding C++ code
*/
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb) {
    // string code;
    // code += fmt::format("/* ==== Basic block {} start ==== */\n", bb->block_id);
    // code += fmt::format("{}:", bb->getLabel());
    // for(BasicBlock::InstIterator intr = bb->begin(); intr != bb->end(); intr++){
    //     const Instruction &inst = *intr->get();
    //     ASSERT()
    // }
}

void CFGCodeGenerator::run(const Function &func) {
    cout<<fmt::format("Generating code for function {}", func.getFunctionName())<<endl;

    return;
}