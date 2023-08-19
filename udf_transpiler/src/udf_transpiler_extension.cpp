#define DUCKDB_EXTENSION_MAIN

#include "udf_transpiler_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "logical_operator_printer.hpp"
#include <iostream>
#include "logical_operator_code_generator.hpp"

namespace duckdb
{
    duckdb::DuckDB *db_instance;

	/**
	 * just play around duckdb
	*/
	void test(std::string query){
		// make a new connection to avoid deadlocks
		Connection con(*db_instance);
		string error;
		// duckdb::unique_ptr<duckdb::SQLStatement> statement = QueryRelation::ParseStatement(*con.context, "select 1*1", error);
		// if(error.size() > 0){
		// 	std::cerr<<error<<std::endl;
		// }
		// auto binder = Binder::CreateBinder(*con.context);
		// auto result = binder->Bind(*statement);
		// D_ASSERT(result.names.size() == result.types.size());

		auto context = con.context.get();
		context->config.enable_optimizer = false;
		auto plan = context->ExtractPlan(query);
		// udf::LogicalOperatorPrinter printer;
		// printer.VisitOperator(*plan);
		duckdb::LogicalOperatorCodeGenerator generator;
		generator.VisitOperator(*plan);
	}

	inline void Udf_transpilerScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
	{	
		// test();
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name)
			{
				test(name.GetString());
                // std::cout<<duckdb::CastFunctionSet::Get(*db_instance->instance).ImplicitCastCost(duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR)<<std::endl;
				return StringVector::AddString(result, "Transpilation Done.");
				;
			});
	}

	static void LoadInternal(DatabaseInstance &instance)
	{
		auto udf_transpiler_scalar_function = ScalarFunction("udf_transpiler", {LogicalType::VARCHAR},
															 LogicalType::VARCHAR, Udf_transpilerScalarFun);
		ExtensionUtil::RegisterFunction(instance, udf_transpiler_scalar_function);
		// auto itos_scalar_function = ScalarFunction("itos", {LogicalType::INTEGER},
		// 													 LogicalType::VARCHAR, itos);
		// ExtensionUtil::RegisterFunction(instance, itos_scalar_function);
	}

	void UdfTranspilerExtension::Load(DuckDB &db)
	{
        this->db = &db;
        db_instance = &db;
		LoadInternal(*db.instance);
	}
	std::string UdfTranspilerExtension::Name()
	{
		return "udf_transpiler";
	}

} // namespace duckdb

extern "C"
{

	DUCKDB_EXTENSION_API void udf_transpiler_init(duckdb::DatabaseInstance &db)
	{
		LoadInternal(db);
	}

	DUCKDB_EXTENSION_API const char *udf_transpiler_version()
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
