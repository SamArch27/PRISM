#pragma once

#include "function.hpp"
#include "instructions.hpp"
#include "types.hpp"
#include "utils.hpp"

struct PLpgSQLGeneratorResult{
    String code;
};

struct PLpgSQLContainer {
    Vec<String> regionCodes;
};

class PLpgSQLGenerator {
private:
    const YAMLConfig &config;
    void regionCodeGenerator(const Function &function, PLpgSQLContainer &container, const Region *region, int indent);
    void bodyCodeGenerator(const Function &function, PLpgSQLContainer &container);
public:
    PLpgSQLGenerator(const YAMLConfig &config) : config(config) {}
    PLpgSQLGeneratorResult run(const Function &function);
};