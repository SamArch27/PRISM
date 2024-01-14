#define DUCKDB_EXTENSION_MAIN

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

// duckdb::DuckDB *db_instance = NULL;
std::unique_ptr<Connection> con;
ClientContext *context = NULL;

namespace duckdb
{

/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */

inline void isListDistinct_body(const Value& v0, bool list_null, const Value& v1, bool delim_null, Vector &tmp_vec0, Vector &tmp_vec1, Vector &tmp_vec2, Vector &tmp_vec3, Vector &tmp_vec4, Vector &tmp_vec5, Vector &tmp_vec6, Value& result, bool& result_null) {
  if (list_null or delim_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  string_t list = v0.GetValueUnsafe<string_t>();
string_t delim = v1.GetValueUnsafe<string_t>();
string_t part;
bool part_null = false;
int32_t pos;
bool pos_null = false;

  /* ==== Basic block entry start ==== */
entry:
goto L0;

/* ==== Basic block exit start ==== */
exit:
return;

/* ==== Basic block L0 start ==== */
L0:
// part = [CAST "NULL" AS INTEGER];
// pos = [CAST "NULL" AS INTEGER];
list = ConcatOperator(tmp_vec2, TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(list, tmp_vec0), tmp_vec1), delim);
pos = InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
goto L3;

/* ==== Basic block L3 start ==== */
L3:
if(GreaterThan::Operation(pos, 0)) goto L5;
goto L4;

/* ==== Basic block L4 start ==== */
L4:
result = Value::BOOLEAN(NumericCastHelper<string_t, bool, TryCast>("t"));
goto exit;

/* ==== Basic block L5 start ==== */
L5:
part = TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(LeftScalarFunction<LeftRightUnicode>(tmp_vec3, list, NumericCastHelper<int32_t, int64_t, NumericTryCast>(pos)), tmp_vec4), tmp_vec5);
list = SubstringUnicodeOp::Substring(tmp_vec6, list, NumericCastHelper<int32_t, int64_t, NumericTryCast>(AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(pos, 1)), StringLengthOperator::Operation<string_t, int64_t>(list));
if(NotEquals::Operation(InstrOperator::Operation<string_t, string_t, int64_t>(list, part), NumericCastHelper<int32_t, int64_t, NumericTryCast>(0))) goto L9;
goto L8;

/* ==== Basic block L8 start ==== */
L8:
pos = InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
goto L3;

/* ==== Basic block L9 start ==== */
L9:
result = Value::BOOLEAN(NumericCastHelper<string_t, bool, TryCast>("f"));
goto exit;

}

void isListDistinct(DataChunk &args, ExpressionState &state, Vector &result) {
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

  DataChunk tmp_chunk;
vector<LogicalType> tmp_types(7, LogicalType::VARCHAR);
tmp_chunk.Initialize(*context, tmp_types);

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto list_index = list_data.sel->get_index(base_idx);
auto delim_index = delim_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    isListDistinct_body(list.GetValue(list_index), !list_data.validity.RowIsValid(list_index), delim.GetValue(delim_index), !delim_data.validity.RowIsValid(delim_index), tmp_chunk.data[0], tmp_chunk.data[1], tmp_chunk.data[2], tmp_chunk.data[3], tmp_chunk.data[4], tmp_chunk.data[5], tmp_chunk.data[6], temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */
	

	static void LoadInternal(DatabaseInstance &instance)
	{
		con = make_uniq<Connection>(instance);
		context = con->context.get();
		// auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		// 													 LogicalType::BOOLEAN, isListDistinct);
		// ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */
auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, isListDistinct);
ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
/* ==== Unique identifier to indicate register insertion point end: 04rj39jds934 ==== */
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string Udf1Extension::Name()
	{
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
		return "udf1";
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	}

} // namespace duckdb

extern "C"
{
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API void udf1_init(duckdb::DatabaseInstance &db)
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		LoadInternal(db);
	}
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API const char *udf1_version()
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif