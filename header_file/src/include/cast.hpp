#include "functions.hpp"

using namespace duckdb;

namespace udf{

string_t int_to_string(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

}

/**
 * numeric casts
 * Can inline
 * NumericTryCast
 * TryCastToDecimal
 * StringCast
 * NumericTryCastToBit
*/
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
