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
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <iostream>

// duckdb::DuckDB *db_instance = NULL;
std::unique_ptr<Connection> con;
ClientContext *context = NULL;

namespace duckdb
{

	/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	AggregateFunction
	UnaryBaseAggregate(const LogicalType &input_type, LogicalType return_type,
					   FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING)
	{
		return AggregateFunction(
			{input_type}, return_type, AggregateFunction::StateSize<STATE>,
			AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
			nullptr, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
			null_handling, nullptr);
	}

	template <class STATE, class INPUT_TYPE1, class INPUT_TYPE2, class RESULT_TYPE, class OP>
	AggregateFunction
	BinaryBaseAggregate(const LogicalType &input_type1, const LogicalType &input_type2, LogicalType return_type,
						FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING)
	{
		return AggregateFunction(
			{input_type1, input_type2}, return_type, AggregateFunction::StateSize<STATE>,
			AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::BinaryScatterUpdate<STATE, INPUT_TYPE1, INPUT_TYPE2, OP>,
			nullptr, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
			null_handling, nullptr);
	}

	template <class STATE, class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class RESULT_TYPE, class OP>
	AggregateFunction
	TrinaryBaseAggregate(const LogicalType &input_type1, const LogicalType &input_type2, const LogicalType &input_type3, LogicalType return_type,
						 FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING)
	{
		return AggregateFunction(
			{input_type1, input_type2, input_type3}, return_type, AggregateFunction::StateSize<STATE>,
			AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::TrinaryScatterUpdate<STATE, INPUT_TYPE1, INPUT_TYPE2, INPUT_TYPE3, OP>,
			nullptr, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
			null_handling, nullptr);
	}

	inline void ifcopy_body(const Value &v0, bool input_null, const Value &v1, bool org_null, Vector &tmp_vec0, Vector &tmp_vec1, Value &result, bool &result_null)
	{
		if (input_null or org_null)
		{
			result_null = true;
			return;
		}
		// the declaration / initialization of local variables
		string_t input = v0.GetValueUnsafe<string_t>();
		int32_t org = v1.GetValueUnsafe<int32_t>();
		int32_t ret;

		if (Equals::Operation<string_t>(LeftScalarFunction<LeftRightUnicode>(tmp_vec0, input, NumericCastHelper<int32_t, int64_t, NumericTryCast>(1)), "1"))
		{
			ret = NumericCastHelper<string_t, int32_t, TryCast>(SubstringUnicodeOp::Substring(tmp_vec1, input, NumericCastHelper<int32_t, int64_t, NumericTryCast>(2), StringLengthOperator::Operation<string_t, int64_t>(input)));
		}

		else
		{
			ret = org;
		}
		result = Value::INTEGER(ret);
		return;
	}
	void ifcopy(DataChunk &args, ExpressionState &state, Vector &result)
	{
		const int count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);

		// the extraction of function arguments
		auto &input = args.data[0];
		auto input_type = input.GetVectorType();
		UnifiedVectorFormat input_data;
		input.ToUnifiedFormat(count, input_data);
		auto &org = args.data[1];
		auto org_type = org.GetVectorType();
		UnifiedVectorFormat org_data;
		org.ToUnifiedFormat(count, org_data);

		DataChunk tmp_chunk;
		vector<LogicalType> tmp_types(2, LogicalType::VARCHAR);
		tmp_chunk.Initialize(*context, tmp_types);

		for (int base_idx = 0; base_idx < count; base_idx++)
		{
			auto input_index = input_data.sel->get_index(base_idx);
			auto org_index = org_data.sel->get_index(base_idx);

			Value temp_result;
			bool temp_result_null = false;
			ifcopy_body(input.GetValue(input_index), !input_data.validity.RowIsValid(input_index), org.GetValue(org_index), !org_data.validity.RowIsValid(org_index), tmp_chunk.data[0], tmp_chunk.data[1], temp_result, temp_result_null);
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
	inline void ifcopy_str_body(const Value &v0, bool input_null, const Value &v1, bool org_null, Vector &tmp_vec0, Vector &tmp_vec1, Value &result, bool &result_null)
	{
		if (input_null or org_null)
		{
			result_null = true;
			return;
		}
		// the declaration / initialization of local variables
		string_t input = v0.GetValueUnsafe<string_t>();
		string_t org = v1.GetValueUnsafe<string_t>();
		string_t ret;

		if (Equals::Operation<string_t>(LeftScalarFunction<LeftRightUnicode>(tmp_vec0, input, NumericCastHelper<int32_t, int64_t, NumericTryCast>(1)), "1"))
		{
			ret = SubstringUnicodeOp::Substring(tmp_vec1, input, NumericCastHelper<int32_t, int64_t, NumericTryCast>(2), StringLengthOperator::Operation<string_t, int64_t>(input));
		}

		else
		{
			ret = org;
		}
		result = Value(ret);
		result.GetTypeMutable() = LogicalType::VARCHAR;
		return;
	}
	void ifcopy_str(DataChunk &args, ExpressionState &state, Vector &result)
	{
		const int count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);

		// the extraction of function arguments
		auto &input = args.data[0];
		auto input_type = input.GetVectorType();
		UnifiedVectorFormat input_data;
		input.ToUnifiedFormat(count, input_data);
		auto &org = args.data[1];
		auto org_type = org.GetVectorType();
		UnifiedVectorFormat org_data;
		org.ToUnifiedFormat(count, org_data);

		DataChunk tmp_chunk;
		vector<LogicalType> tmp_types(2, LogicalType::VARCHAR);
		tmp_chunk.Initialize(*context, tmp_types);

		for (int base_idx = 0; base_idx < count; base_idx++)
		{
			auto input_index = input_data.sel->get_index(base_idx);
			auto org_index = org_data.sel->get_index(base_idx);

			Value temp_result;
			bool temp_result_null = false;
			ifcopy_str_body(input.GetValue(input_index), !input_data.validity.RowIsValid(input_index), org.GetValue(org_index), !org_data.validity.RowIsValid(org_index), tmp_chunk.data[0], tmp_chunk.data[1], temp_result, temp_result_null);
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

	struct AggState13
	{
		bool isInitialized;
		int32_t count;
	};

	struct CustomAggOperation13
	{
		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
			// state.count = 0;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, AggregateBinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE1, INPUT_TYPE2, STATE, OP>(state, input1, input2, tmp);
			}
			// state.count += count;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, AggregateBinaryInput &)
		{
			if (state.isInitialized == false)
			{
				state.isInitialized = true;
				state.count = input2;
			}
			state.count++;
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				finalize_data.ReturnNull();
				return;
			}
			target = state.count;
		}

		static bool IgnoreNull()
		{
			return false;
		}
	};

	struct AggState14
	{
		bool isInitialized;
		int64_t revenue;
	};

	struct CustomAggOperation14
	{

		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE1, INPUT_TYPE2, INPUT_TYPE3, STATE, OP>(state, input1, input2, input3, tmp);
			}
			// state.count += count;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &)
		{
			if (state.isInitialized == false)
			{
				state.isInitialized = true;
				state.revenue = input3;
			}
			state.revenue += input1 * (1 - input2);
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				finalize_data.ReturnNull();
				return;
			}
			target = state.revenue;
		}

		static bool IgnoreNull()
		{
			return true;
		}
	};

	struct AggState18
	{
		bool isInitialized;
		int64_t volumn;
	};

	struct CustomAggOperation18
	{
		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
		}

		template <class INPUT_TYPE, class INPUT_TYPE2, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE &input, const INPUT_TYPE2 &input2, AggregateBinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE, INPUT_TYPE2, STATE, OP>(state, input, input2, tmp);
			}
		}

		template <class INPUT_TYPE, class INPUT_TYPE2, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE &input, const INPUT_TYPE2 &input2, AggregateBinaryInput &)
		{
			if (state.isInitialized == false)
			{
				state.isInitialized = true;
				// state.volumn = 0;
				state.volumn = input2;
			}
			state.volumn += input;
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				// target = 0;
				finalize_data.ReturnNull();
				return;
			}
			target = state.volumn;
		}

		static bool IgnoreNull()
		{
			return false;
		}
	};

	struct AggState19
	{
		bool isInitialized;
		int64_t revenue;
	};

	struct CustomAggOperation19
	{

		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
			// state.count = 0;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE1, INPUT_TYPE2, INPUT_TYPE3, STATE, OP>(state, input1, input2, input3, tmp);
			}
			// state.count += count;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &)
		{
			if (state.isInitialized == false)
			{
				state.isInitialized = true;
				// state.revenue = 0;
				state.revenue = input3;
			}
			state.revenue += input1 * (1 - input2);
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				finalize_data.ReturnNull();
				// target = 0;
				return;
			}
			target = state.revenue;
		}

		static bool IgnoreNull()
		{
			return true;
		}
	};

	struct AggState2
	{
		bool isInitialized;
		int32_t min_cost;
		string_t supp_name;
	};

	struct CustomAggOperation2
	{

		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE1, INPUT_TYPE2, INPUT_TYPE3, STATE, OP>(state, input1, input2, input3, tmp);
			}
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class INPUT_TYPE3, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, const INPUT_TYPE3 &input3, AggregateTrinaryInput &)
		{
			if (state.isInitialized == false)
			{
				state.isInitialized = true;
				state.min_cost = input3;
				// state.supp_name = input2;
			}
			if (input1 < state.min_cost)
			{
				state.min_cost = input1;
				state.supp_name = input2;
			}
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				finalize_data.ReturnNull();
				return;
			}
			target = state.supp_name;
		}

		static bool IgnoreNull()
		{
			return true;
		}
	};

	/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */

	static void LoadInternal(DatabaseInstance &instance)
	{
		con = make_uniq<Connection>(instance);
		context = con->context.get();
		// auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		// 													 LogicalType::BOOLEAN, isListDistinct);
		// ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
		/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */
		// auto ordersbycustomeraggregate = UnaryBaseAggregate<AggState13, int32_t, int32_t, CustomAggOperation13>(LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);
		auto ordersbycustomeraggregate = BinaryBaseAggregate<AggState13, int32_t, int32_t, int32_t, CustomAggOperation13>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);

		// custom_agg_function.update = CountScatter<int64_t, AggState, CustomAggOperation>;
		// auto custom_agg_function = AggregateFunction::UnaryAggregate<AggState, int64_t, int32_t, CustomAggOperation>(LogicalType::BIGINT, LogicalType::INTEGER);
		ordersbycustomeraggregate.name = "ordersbycustomeraggregate";
		ExtensionUtil::RegisterFunction(instance, ordersbycustomeraggregate);
		// auto custom_agg = BinaryBaseAggregate<AggState13, int32_t, int32_t, int32_t, CustomAggOperation13>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);

		// // // custom_agg_function.update = CountScatter<int64_t, AggState, CustomAggOperation>;
		// // // auto custom_agg_function = AggregateFunction::UnaryAggregate<AggState, int64_t, int32_t, CustomAggOperation>(LogicalType::BIGINT, LogicalType::INTEGER);
		// custom_agg.name = "customeraggregate";
		// ExtensionUtil::RegisterFunction(instance, custom_agg);

		// auto promo_revenue_agg = BinaryBaseAggregate<AggState14, int64_t, int64_t, int64_t, CustomAggOperation14>(LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		auto promo_revenue_agg = TrinaryBaseAggregate<AggState14, int64_t, int64_t, int64_t, int64_t, CustomAggOperation14>(LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		promo_revenue_agg.name = "promo_revenue_agg";
		ExtensionUtil::RegisterFunction(instance, promo_revenue_agg);
		// equivalent to sum
		// auto volume_customer_agg = UnaryBaseAggregate<AggState18, int32_t, int64_t, CustomAggOperation18>(LogicalType::INTEGER, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		auto volume_customer_agg = BinaryBaseAggregate<AggState18, int32_t, int64_t, int64_t, CustomAggOperation18>(LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		volume_customer_agg.name = "volume_customer_agg";
		ExtensionUtil::RegisterFunction(instance, volume_customer_agg);
		// same as 14
		// auto sum_discounted_price = BinaryBaseAggregate<AggState19, int32_t, int32_t, int32_t, CustomAggOperation19>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);
		auto sum_discounted_price = TrinaryBaseAggregate<AggState19, int32_t, int32_t, int32_t, int32_t, CustomAggOperation19>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);
		sum_discounted_price.name = "sum_discounted_price";
		ExtensionUtil::RegisterFunction(instance, sum_discounted_price);
		// equivalent to arg_min
		// auto MinCostSuppWithCustomAgg = BinaryBaseAggregate<AggState2, int32_t, string_t, string_t, CustomAggOperation2>(LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, FunctionNullHandling::SPECIAL_HANDLING);
		auto MinCostSuppWithCustomAgg = TrinaryBaseAggregate<AggState2, int32_t, string_t, int32_t, string_t, CustomAggOperation2>(LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR, FunctionNullHandling::SPECIAL_HANDLING);
		MinCostSuppWithCustomAgg.name = "MinCostSuppAggregate";
		ExtensionUtil::RegisterFunction(instance, MinCostSuppWithCustomAgg);
		auto ifcopy_scalar_function = ScalarFunction("ifcopy", {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::INTEGER, ifcopy);
		ExtensionUtil::RegisterFunction(instance, ifcopy_scalar_function);
		auto ifcopy_str_scalar_function = ScalarFunction("ifcopy_str", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, ifcopy_str);
		ExtensionUtil::RegisterFunction(instance, ifcopy_str_scalar_function);
		// 21 is the same as 13 which is count

		/* ==== Unique identifier to indicate register insertion point end: 04rj39jds934 ==== */
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string Udf1Extension::Name()
	{
		/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
		return "udf_agg";
		/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	}

} // namespace duckdb

extern "C"
{
	/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API void udf_agg_init(duckdb::DatabaseInstance &db)
	/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		LoadInternal(db);
	}
	/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API const char *udf_agg_version()
	/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
