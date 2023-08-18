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

namespace duckdb
{

	inline void itos_body(const Value &v0, bool val1_null, Value &result, bool &result_null, Vector &tmp_vec)
	{
		if (val1_null)
		{
			result_null = true;
			return;
		}
		// the declaration / initialization of local variables
		int32_t val1 = v0.GetValueUnsafe<int32_t>();
		
		result = Value(udf::int_to_string(AddOperator::Operation<int32_t, int32_t, int64_t>(val1, 1), tmp_vec));
		result.GetTypeMutable() = LogicalType::VARCHAR;
		return;
	}

	void itos(DataChunk &args, ExpressionState &state, Vector &result)
	{
		const int count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);

		// the extraction of function arguments
		auto &val1 = args.data[0];
		auto val1_type = val1.GetVectorType();
		UnifiedVectorFormat val1_data;
		val1.ToUnifiedFormat(count, val1_data);
		Vector tmp_vec(LogicalType::VARCHAR, 2048);

		for (int base_idx = 0; base_idx < count; base_idx++)
		{
			auto val1_index = val1_data.sel->get_index(base_idx);

			Value temp_result;
			bool temp_result_null = false;
			itos_body(val1.GetValue(val1_index), !val1_data.validity.RowIsValid(val1_index), temp_result, temp_result_null, tmp_vec);
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

	inline void Udf1ScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
	{
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name)
			{
				return StringVector::AddString(result, "Udf1 " + name.GetString() + " 🐥"+std::to_string(StrLenOperator::Operation<string_t, int64_t>(name.GetString())));
				;
			});
	}

	static void LoadInternal(DatabaseInstance &instance)
	{
		auto udf1_scalar_function = ScalarFunction("udf1", {LogicalType::VARCHAR},
															 LogicalType::VARCHAR, Udf1ScalarFun);
		ExtensionUtil::RegisterFunction(instance, udf1_scalar_function);
		
		auto itos_scalar_function = ScalarFunction("itos", {LogicalType::INTEGER},
															 LogicalType::VARCHAR, itos);
		ExtensionUtil::RegisterFunction(instance, itos_scalar_function);
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string Udf1Extension::Name()
	{
		return "udf1";
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
