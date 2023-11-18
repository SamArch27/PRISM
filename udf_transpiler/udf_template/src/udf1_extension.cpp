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


/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */
	

	static void LoadInternal(DatabaseInstance &instance)
	{
		con = make_uniq<Connection>(instance);
		context = con->context.get();
		// auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		// 													 LogicalType::BOOLEAN, isListDistinct);
		// ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */


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