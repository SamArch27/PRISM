#include "functions.hpp"
using namespace duckdb;

namespace udf{

template <class SRCTYPE, class UTYPE>
SRCTYPE AddOperation(SRCTYPE left, SRCTYPE right) {
    UTYPE uresult = UTYPE(left) + UTYPE(right);
    if (uresult < NumericLimits<SRCTYPE>::Minimum() || uresult > NumericLimits<SRCTYPE>::Maximum()) {
        throw std::runtime_error("addition overflow");
    }
    return SRCTYPE(uresult);
}

/**
 * Multiply functions - done
*/
struct MultiplyOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left * right;
	}
};

struct MultiplyOperatorOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryMultiplyOperator::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in multiplication of %s (%d * %d)!", TypeIdToString(GetTypeId<TA>()),
			                          left, right);
		}
		return result;
	}
};

// template <> 
// int32_t AddOperation<int32_t, int64_t>(int32_t left , int32_t right);

}
