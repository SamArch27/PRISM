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

struct NegateOperator {
  template <class T> static bool CanNegate(T input) {
    using Limits = std::numeric_limits<T>;
    return !(Limits::is_integer && Limits::is_signed &&
             Limits::lowest() == input);
  }

  template <class TA, class TR> static inline TR Operation(TA input) {
    auto cast = (TR)input;
    if (!CanNegate<TR>(cast)) {
      throw OutOfRangeException("Overflow in negation of integer!");
    }
    return -cast;
  }
};

template <> bool NegateOperator::CanNegate(float input) { return true; }

template <> bool NegateOperator::CanNegate(double input) { return true; }

template <> interval_t NegateOperator::Operation(interval_t input) {
  interval_t result;
  result.months = NegateOperator::Operation<int32_t, int32_t>(input.months);
  result.days = NegateOperator::Operation<int32_t, int32_t>(input.days);
  result.micros = NegateOperator::Operation<int64_t, int64_t>(input.micros);
  return result;
}

struct BinaryZeroIsNullWrapper {
  template <class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
  static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
    if (right == 0) {
      throw std::runtime_error("Division by zero");
    } else {
      return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left,
                                                                        right);
    }
  }
};
