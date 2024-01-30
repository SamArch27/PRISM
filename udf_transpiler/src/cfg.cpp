#include "cfg.hpp"

Function newFunction(const std::string &functionName) {
  return Function(functionName,
                  make_unique<NonDecimalType>(DuckdbTypeTag::INTEGER));
}