#define DUCKDB_EXTENSION_MAIN

#include "udf_transpiler_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void Udf_transpilerScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Udf_transpiler "+name.GetString()+" 🐥");;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    auto udf_transpiler_scalar_function = ScalarFunction("udf_transpiler", {LogicalType::VARCHAR},
        LogicalType::VARCHAR, Udf_transpilerScalarFun);
    ExtensionUtil::RegisterFunction(instance, udf_transpiler_scalar_function);
}

void Udf_transpilerExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string Udf_transpilerExtension::Name() {
	return "udf_transpiler";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void udf_transpiler_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *udf_transpiler_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
