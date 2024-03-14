#include "functions.hpp"

using namespace duckdb;

// namespace udf{

// string_t int_to_string(int32_t input, Vector &vector) {
// 	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
// }

// }

/**
 * numeric casts
 * Cannot inline
 * NumericTryCast
 * TryCastToDecimal
 * StringCast
 * NumericTryCastToBit
*/
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"

/**
 * A helper function to expose target from a typical numeric cast
 * can accept operator: NumericTryCast, TryCast, etc
*/
template <typename S, typename T, typename op>
inline T NumericCastHelper(S input) {
    T output;
    op::template Operation<S, T>(input, output);
    return output;
}

/**
 * A helper function for exposing error message from a typical cast
*/
template <typename S, typename T, typename op>
inline T ErrorCastHelper(S input){
    T output;
    string_t error_message;
    op::template Operation<S, T>(input, output, &error_message);
    if(!error_message.empty()){
        throw std::runtime_error(error_message);
    }
}

/**
 * A helper function to expose target from a decimal cast
 * can accept operator: TryCastToDecimal
*/
template <typename S, typename T, typename op>
inline T DecimalCastHelper(S input, int width, int scale) {
    T output;
    std::string error_message;
    op::template Operation<S, T>(input, output, &error_message, width, scale);
    if(!error_message.empty()){
        throw std::runtime_error(error_message);
    }
    return output;
}

// udf_todo

/**
 * decimal casts
 * Cannot inline
 * TryCastFromDecimal
 * StringCastFromDecimalOperator
*/
#include "duckdb/common/operator/decimal_cast_operators.hpp"

/**
 * string casts
 * Cannot inline
 * TryCastErrorMessage
 * TryCastToTimestampNS
 * TryCastToTimestampSec
 * TryCastToTimestampMS
 * TryCast
 * Cast
*/
#include "duckdb/common/operator/cast_operators.hpp"
