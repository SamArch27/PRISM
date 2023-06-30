#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
using namespace duckdb;

//===--------------------------------------------------------------------===//
// Scalar function
//===--------------------------------------------------------------------===//
inline void hello_fun(const Value& v1, bool v1_null, Value& result, bool& result_null) {
	if (v1_null)
	{
		result_null = true;
		return;
	}
	string_t what = v1.GetValueUnsafe<string_t>();
	uint32_t x = what.GetSize() + 5;
	result = Value::INTEGER(x);
}


inline void HelloFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &vector = args.data[0];
	auto vector_type = vector.GetVectorType();
	const int count = args.size();

	UnifiedVectorFormat data;
	vector.ToUnifiedFormat(count, data);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	for (int base_idx = 0; base_idx < count; base_idx++) {
		auto index = data.sel->get_index(base_idx);
		Value temp_result;
		bool  temp_result_null = false;
		hello_fun(vector.GetValue(index),!data.validity.RowIsValid(index), temp_result, temp_result_null);
		if (temp_result_null) {
			FlatVector::SetNull(result, base_idx, true);
		}
		else {
			result.SetValue(index, temp_result);
		}
	}

	result.Verify(count);
}


//===--------------------------------------------------------------------===//
// Extension load + setup
//===--------------------------------------------------------------------===//
extern "C" {
DUCKDB_EXTENSION_API void sam_extension_init(duckdb::DatabaseInstance &db) {
	// create a scalar function
	Connection con(db);
	auto &client_context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(client_context);
	con.BeginTransaction();

	// Function add point
	ScalarFunction hello_func("hello", {LogicalTypeId::VARCHAR}, LogicalTypeId::INTEGER, HelloFunction);
	CreateScalarFunctionInfo hello_func_info(hello_func);
	catalog.CreateFunction(client_context, hello_func_info);
	con.Commit();
}

DUCKDB_EXTENSION_API const char *sam_extension_version() {
	return DuckDB::LibraryVersion();
}
}

