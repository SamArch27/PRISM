#include "aggify_dfa.hpp"

bool AggifyDFA::analyzeBasicBlock(BasicBlock *bb, const Function &func){
    for(BasicBlock::InstIterator intr = bb->begin(); intr != bb->end(); intr++){
        if(dynamic_cast<const Assignment *>(intr->get())){
            const Assignment *assign = dynamic_cast<const Assignment *>(intr->get());
            returnVarName = assign->getVar()->getName();
            return true;
        }
    }
    return false;
}

/**
 * for now, find the first mentioned variable
 * udf_todo: follow the actual aggify paper
*/
void AggifyDFA::analyzeFunction(const Function &function){
    for(auto &bbUniq : function.getBasicBlocks()){
        // fast exit
        if(analyzeBasicBlock(bbUniq.get(), function)) return;
    }
    ERROR("No assignment instruction found in the cursor loop.");
}