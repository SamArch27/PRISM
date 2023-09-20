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
  inline void isListDistinct_body(const Value &v0, bool list_null, const Value &v1, bool delim_null, Value &result, bool &result_null,
									Vector &tmp_vec1, Vector &tmp_vec2, Vector &tmp_vec3, Vector &tmp_vec4, Vector &tmp_vec5, Vector &tmp_vec6,
									Vector &tmp_vec7)
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

		list = ConcatOperator(tmp_vec3, TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(list, tmp_vec1), tmp_vec2), delim);
		pos = InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
		while (GreaterThan::Operation(pos, 0))
		{
			part = TrimOperator<true, false>::Operation<string_t, string_t>(TrimOperator<false, true>::Operation<string_t, string_t>(LeftScalarFunction<LeftRightUnicode>(tmp_vec4, list, pos), tmp_vec5), tmp_vec6);
			list = SubstringUnicodeOp::Substring(tmp_vec7, list, AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(pos, 1), StringLengthOperator::Operation<string_t, int64_t>(list));
			if (NotEquals::Operation(InstrOperator::Operation<string_t, string_t, int64_t>(list, part), (int64_t)0))
			{
				result = Value::BOOLEAN(false);
				return;
			}
			pos = InstrOperator::Operation<string_t, string_t, int64_t>(list, delim);
		}
		result = Value::BOOLEAN(true);
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

		DataChunk tmp_chunk;
		vector<LogicalType> types(7, LogicalType::VARCHAR);
		tmp_chunk.Initialize(*context, types);
		for (int base_idx = 0; base_idx < count; base_idx++)
		{
			auto list_index = list_data.sel->get_index(base_idx);
			auto delim_index = delim_data.sel->get_index(base_idx);

			Value temp_result;
			bool temp_result_null = false;
			isListDistinct_body(list.GetValue(list_index), !list_data.validity.RowIsValid(list_index), delim.GetValue(delim_index), !delim_data.validity.RowIsValid(delim_index), temp_result, temp_result_null, tmp_chunk.data[0], tmp_chunk.data[1], tmp_chunk.data[2], tmp_chunk.data[3], tmp_chunk.data[4], tmp_chunk.data[5], tmp_chunk.data[6]);
			if (temp_result_null)
			{
				FlatVector::SetNull(result, base_idx, true);
			}
			else
			{
				result.SetValue(base_idx, std::move(temp_result));
			}
		}

		result.Verify(count);
	}

/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */

inline void udf18_body(const Value& v0, bool numweb_null, const Value& v1, bool numstore_null, const Value& v2, bool numcat_null, Value& result, bool& result_null) {
  if (numweb_null or numstore_null or numcat_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int64_t numweb = v0.GetValueUnsafe<int64_t>();
int64_t numstore = v1.GetValueUnsafe<int64_t>();
int64_t numcat = v2.GetValueUnsafe<int64_t>();
string_t retvalue;

  if((GreaterThanEquals::Operation(numweb, numstore) && GreaterThanEquals::Operation(numweb, numcat))) {
  retvalue  = "web";

}
else if ((GreaterThanEquals::Operation(numstore, numweb) && GreaterThanEquals::Operation(numstore, numcat))) {
  retvalue  = "store";

}
else if ((GreaterThanEquals::Operation(numcat, numstore) && GreaterThanEquals::Operation(numcat, numweb))) {
  retvalue  = "Catalog";

}

else {
  retvalue  = "Logical error";

}
result = Value(retvalue);
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf18(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numweb = args.data[0];
auto numweb_type = numweb.GetVectorType();
UnifiedVectorFormat numweb_data;
numweb.ToUnifiedFormat(count, numweb_data);
auto &numstore = args.data[1];
auto numstore_type = numstore.GetVectorType();
UnifiedVectorFormat numstore_data;
numstore.ToUnifiedFormat(count, numstore_data);
auto &numcat = args.data[2];
auto numcat_type = numcat.GetVectorType();
UnifiedVectorFormat numcat_data;
numcat.ToUnifiedFormat(count, numcat_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numweb_index = numweb_data.sel->get_index(base_idx);
auto numstore_index = numstore_data.sel->get_index(base_idx);
auto numcat_index = numcat_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf18_body(numweb.GetValue(numweb_index), !numweb_data.validity.RowIsValid(numweb_index), numstore.GetValue(numstore_index), !numstore_data.validity.RowIsValid(numstore_index), numcat.GetValue(numcat_index), !numcat_data.validity.RowIsValid(numcat_index), temp_result, temp_result_null);
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
		auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
															 LogicalType::BOOLEAN, isListDistinct);
		ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */
auto udf18_scalar_function = ScalarFunction("udf18", {LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3)}, LogicalType::VARCHAR, udf18);
ExtensionUtil::RegisterFunction(instance, udf18_scalar_function);
/* ==== Unique identifier to indicate register insertion point end: 04rj39jds934 ==== */
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string Udf1Extension::Name()
	{
		return "udf2";
	}

} // namespace duckdb

extern "C"
{

	DUCKDB_EXTENSION_API void udf1_init(duckdb::DatabaseInstance &db)
	{
		LoadInternal(db);
	}

	DUCKDB_EXTENSION_API const char *udf1_version()
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
