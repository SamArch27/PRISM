#pragma once
#include "duckdb.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

using namespace duckdb;
namespace udf{
/**
 * Numeric functions
*/

template <class SRCTYPE, class UTYPE>
SRCTYPE AddOperation(SRCTYPE left, SRCTYPE right);



/**
 * String functions
 * 
*/

string_t int_to_string(int32_t input, Vector &vector);

}