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
        return fmt::format("{} = Value::{}({})", retName, retType->getDuckDBType(), retValue);
    } else if (retType->isBLOB()) {
        return fmt::format(
            "{0} = Value({1});\n{0}.GetTypeMutable() = LogicalType::{2}", retName,
            retValue, retType->getDuckDBType());
    } else {
        ERROR(fmt::format("Cannot create duckdb value from type {}", retType->getDuckDBType()));
    }
}

/**
 * for each instruction in the basic block, generate the corresponding C++ code
*/
void CFGCodeGenerator::basicBlockCodeGenerator(BasicBlock *bb, const Function &func, CodeGenInfo &function_info) {
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
        try{
        if(dynamic_cast<const Assignment *>(intr->get())){
            const Assignment *assign = dynamic_cast<const Assignment *>(intr->get());
            // code += fmt::format("{} = {};\n", assign->getLvalue(), assign->getRvalue());
            duckdb::LogicalOperatorCodeGenerator locg;
            locg.VisitOperator((Expression &)*(assign->getExpr()), function_info);
            auto [header, res] = locg.getResult();
            code += header;
            code += fmt::format("{} = {};\n", assign->getVar()->getName(), res);
        }
        else if(dynamic_cast<const ReturnInst *>(intr->get())){
            const ReturnInst *ret = dynamic_cast<const ReturnInst *>(intr->get());
            duckdb::LogicalOperatorCodeGenerator locg;
            locg.VisitOperator((Expression &)*(ret->getExpr()), function_info);
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
                locg.VisitOperator((Expression &)*(br->getCond()), function_info);
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
        }catch(const std::exception &e){
            std::stringstream ss;
            ss << e.what() << "When compiling instruction: " << *intr;
            throw duckdb::ParserException(ss.str());
        }
    }
end:
    // std::cout<<code<<endl;
    //todo: insert code to the container
    container.basicBlockCodes.push_back(code);
    return;
}

/**
 * 
*/
std::string CFGCodeGenerator::extractVarFromChunk(const Function &func){
    string code;
    const auto &args = func.getArguments();
    for(size_t i=0;i<args.size();i++){
        auto &arg = args[i];
        string extract_data_from_value = fmt::format(
          "v{}.GetValueUnsafe<{}>()", i, arg->getType()->getCppType());
        code += fmt::format("{} {} = {};\n", arg->getType()->getCppType(),
                                arg->getName(), extract_data_from_value);
    }

    // just declare the local variables, they will be initialized in the basic blocks
    // also create the null indicator for each variable
    const auto &vars = func.getVariables();
    for(size_t i=0;i<vars.size();i++){
        auto &var = vars[i];
        code += fmt::format("{} {};\n", var->getType()->getCppType(),
                                var->getName());
        code += fmt::format("bool {}_null = {};\n", var->getName(), var->isNULL ? "true" : "false");
    }

    return code;
}

vector<string> CFGCodeGenerator::run(const Function &func) {
    cout<<fmt::format("Generating code for function {}", func.getFunctionName())<<endl;
    // std::ostringstream oss;
    // func.print(oss);
    // cout<<oss.str()<<endl;
    CodeGenInfo function_info(func);

    for(auto &bbUniq : func.getBasicBlocks()){
        basicBlockCodeGenerator(bbUniq.get(), func, function_info);
    }
    string function_args = "";
    string arg_indexes = "";
    string subfunc_args = "";
    string fbody_args = "";
    vector<string> check_null;

    int count = 0;
    for (const auto &arg : func.getArguments()) {
        string name = arg->getName();
        function_args +=
            fmt::format(fmt::runtime(config.function["fargs2"].Scalar()),
                        fmt::arg("var_name", name),
                        fmt::arg("i", count));
        function_args += "\n";

        arg_indexes +=
            fmt::format(fmt::runtime(config.function["argindex"].Scalar()),
                        fmt::arg("var_name", name));
        arg_indexes += "\n";

        subfunc_args +=
            fmt::format(fmt::runtime(config.function["subfunc_arg"].Scalar()),
                        fmt::arg("var_name", name));
        subfunc_args += ", ";

        fbody_args +=
            fmt::format(fmt::runtime(config.function["fbody_arg"].Scalar()),
                        fmt::arg("i", count),
                        fmt::arg("var_name", name));
        fbody_args += ", ";

        check_null.push_back(name + "_null");

        //todo: add {}_null, result to the argument list
        count++;
    }

    string vector_create = "";
    if (function_info.vectorCount > 0) {
        vector_create = fmt::format(
            fmt::runtime(config.function["vector_create"].Scalar()),
            fmt::arg("vector_count", function_info.vectorCount));
        for (int i = 0; i < function_info.vectorCount; i++) {
            subfunc_args += fmt::format("tmp_chunk.data[{}], ", i);
            fbody_args += fmt::format("Vector &tmp_vec{}, ", i);
        }
    }

    string vars_init = extractVarFromChunk(func);

    container.body = fmt::format(
        fmt::runtime(config.function["fbodyshell"].Scalar()),
        fmt::arg("function_name", func.getFunctionName()),
        fmt::arg("fbody_args", fbody_args),
        fmt::arg("check_null", vec_join(check_null, " or ")),
        fmt::arg("vars_init", vars_init), fmt::arg("action", vec_join(container.basicBlockCodes, "\n")));

    container.main = 
        fmt::format(fmt::runtime(config.function["fshell2"].Scalar()),
                    fmt::arg("function_name", func.getFunctionName()),
                    fmt::arg("function_args", function_args),
                    fmt::arg("arg_indexes", arg_indexes),
                    fmt::arg("vector_create", vector_create),
                    fmt::arg("subfunc_args", subfunc_args));

    vector<string> args_logical_types;
    for (auto &arg : func.getArguments()) {
        args_logical_types.push_back(
            arg->getType()->getDuckDBLogicalType());
    }
    container.registration = fmt::format(
        fmt::runtime(config.function["fcreate"].Scalar()),
        fmt::arg("function_name", func.getFunctionName()),
        fmt::arg("return_logical_type",
                func.getReturnType()->getDuckDBLogicalType()),
        fmt::arg("args_logical_types", vec_join(args_logical_types, ", ")));
    
    std::cout<<container.body<<std::endl;
    std::cout<<container.main<<std::endl;
    std::cout<<container.registration<<std::endl;
    return {vec_join(func.getCustomAggs(), "\n") + container.body + "\n" + container.main, container.registration};
}