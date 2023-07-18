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

// template <> 
// int32_t AddOperation<int32_t, int64_t>(int32_t left , int32_t right);

}
