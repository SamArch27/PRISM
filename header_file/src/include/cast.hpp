#include "functions.hpp"

using namespace duckdb;

namespace udf{

string_t int_to_string(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

}