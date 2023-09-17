#include "functions.hpp"
// using namespace duckdb;

/**
 * Add functions - done
 * Can inline
 * AddOperator
 * DecimalAddOverflowCheck
 * AddTimeOperator
*/
#include "duckdb/common/operator/add.hpp"

/**
 * Subtract functions - done
 * Can inline
 * SubtractOperator
 * DecimalSubtractOverflowCheck
 * SubtractTimeOperator
*/
#include "duckdb/common/operator/subtract.hpp"


/**
 * Multiply functions - done
 * Can inline
 * MultiplyOperator
 * MultiplyOperatorOverflowCheck
*/
#include "duckdb/common/operator/multiply.hpp"

/**
 * Divide functions - done
 * Can inline
 * DivideOperator
*/
#include "duckdb/common/operator/numeric_binary_operators.hpp"

/**
 * Modulo functions - done
 * Can inline
 * ModuloOperator
*/
// already included by divide
