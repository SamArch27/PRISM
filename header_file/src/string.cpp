#include "udf1_extension.hpp"
#include "functions.hpp"
#include "numeric.hpp"
#include "cast.hpp"
#include "string.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
using namespace duckdb;

inline void isListDistinct_body(const Value &v0, bool list_null, const Value &v1, bool delim_null, Value &result, bool &result_null)
{
    if (list_null or delim_null)
    {
        result_null = true;
        return;
    }
    // the declaration / initialization of local variables
    string_t list = v0.GetValueUnsafe<string_t>();
    string_t delim = v1.GetValueUnsafe<string_t>();
    string_t part;
    int32_t pos;

    ConcatOperator(TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(list)), delim);
    InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
    while (GreaterThan::Operation(pos, 0))
    {
        TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(LeftScalarFunction<LeftRightUnicode>(list, NumericTryCast::Operation<int32_t, int64_t>(pos))));
        SubstringUnicodeOp::Substring(list, NumericTryCast::Operation<int32_t, int64_t>(AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(pos, 1)), StringLengthOperator::Operation<string_t, int64_t>(list));
        if (NotEquals::Operation(InstrOperator::Operation<string_t, string_t, int64_t>(list, part), NumericTryCast::Operation<int32_t, int64_t>(0)))
        {
            result = Value::BOOLEAN(TryCast::Operation<string_t, bool>("f"));
            return;
        }

        InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
    }
    result = Value::BOOLEAN(TryCast::Operation<string_t, bool>("t"));
    return;
}
void isListDistinct(DataChunk &args, ExpressionState &state, Vector &result)
{
    const int count = args.size();
    result.SetVectorType(VectorType::FLAT_VECTOR);

    // the extraction of function arguments
    auto &list = args.data[0];
    auto list_type = list.GetVectorType();
    UnifiedVectorFormat list_data;
    list.ToUnifiedFormat(count, list_data);
    auto &delim = args.data[1];
    auto delim_type = delim.GetVectorType();
    UnifiedVectorFormat delim_data;
    delim.ToUnifiedFormat(count, delim_data);

    for (int base_idx = 0; base_idx < count; base_idx++)
    {
        auto list_index = list_data.sel->get_index(base_idx);
        auto delim_index = delim_data.sel->get_index(base_idx);

        Value temp_result;
        bool temp_result_null = false;
        isListDistinct_body(list.GetValue(list_index), !list_data.validity.RowIsValid(list_index), delim.GetValue(delim_index), !delim_data.validity.RowIsValid(delim_index), temp_result, temp_result_null);
        if (temp_result_null)
        {
            FlatVector::SetNull(result, base_idx, true);
        }
        else
        {
            result.SetValue(base_idx, temp_result);
        }
    }

    result.Verify(count);
}