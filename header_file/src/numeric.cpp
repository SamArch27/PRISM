#include "functions.hpp"
using namespace duckdb;

namespace udf{

template <class SRCTYPE, class UTYPE>
inline bool AddOperation(SRCTYPE left, SRCTYPE right, SRCTYPE &result) {
    UTYPE uresult = UTYPE(left) + UTYPE(right);
    if (uresult < NumericLimits<SRCTYPE>::Minimum() || uresult > NumericLimits<SRCTYPE>::Maximum()) {
        return false;
    }
    result = SRCTYPE(uresult);
    return true;
}

}
