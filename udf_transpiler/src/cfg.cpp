#include "cfg.hpp"

Function newFunction(const String &functionName) {
  return Function(functionName,
                  Make<NonDecimalType>(DuckdbTypeTag::INTEGER));
}