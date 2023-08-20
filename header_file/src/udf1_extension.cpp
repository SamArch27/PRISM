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

duckdb::DuckDB* db_instance = NULL;
std::unique_ptr<Connection> con;
ClientContext *context = NULL;

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
			if (NotEquals::Operation(InstrOperator::Operation<string_t, string_t, int64_t>(list, part), (int64_t) 0))
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

	inline void Udf1ScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
	{
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name)
			{
				return StringVector::AddString(result, "Udf1 " + name.GetString() + " 🐥" + std::to_string(StrLenOperator::Operation<string_t, int64_t>(name.GetString())));
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

		auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
												   LogicalType::BOOLEAN, isListDistinct);
		ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		// this->db = &db;
		// this->con = std::make_unique<Connection>(db);
		// this->context = con->context.get();
		db_instance = &db;
		con = make_uniq<Connection>(db);
		context = con->context.get();
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
