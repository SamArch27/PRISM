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

	// template <class INPUT_TYPE, class STATE, class OP>
	// static inline void CountFlatLoop(STATE **__restrict states, ValidityMask &mask, idx_t count) {
	// 	if (!mask.AllValid()) {
	// 		idx_t base_idx = 0;
	// 		auto entry_count = ValidityMask::EntryCount(count);
	// 		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
	// 			auto validity_entry = mask.GetValidityEntry(entry_idx);
	// 			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
	// 			if (ValidityMask::AllValid(validity_entry)) {
	// 				// all valid: perform operation
	// 				for (; base_idx < next; base_idx++) {
	// 					OP::template Operation(*states[base_idx]);
	// 				}
	// 			} else if (ValidityMask::NoneValid(validity_entry)) {
	// 				// nothing valid: skip all
	// 				base_idx = next;
	// 				continue;
	// 			} else {
	// 				// partially valid: need to check individual elements for validity
	// 				idx_t start = base_idx;
	// 				for (; base_idx < next; base_idx++) {
	// 					if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
	// 						OP::template Operation(*states[base_idx]);
	// 					}
	// 				}
	// 			}
	// 		}
	// 	} else {
	// 		for (idx_t i = 0; i < count; i++) {
	// 			OP::template Operation(*states[i]);
	// 		}
	// 	}
	// }

	// template <class INPUT_TYPE, class STATE, class OP>
	// static inline void CountScatterLoop(STATE **__restrict states, const SelectionVector &isel,
	//                                     const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
	// 	if (!mask.AllValid()) {
	// 		// potential NULL values
	// 		for (idx_t i = 0; i < count; i++) {
	// 			auto idx = isel.get_index(i);
	// 			auto sidx = ssel.get_index(i);
	// 			if (mask.RowIsValid(idx)) {
	// 				OP::template Operation(*states[sidx]);
	// 			}
	// 		}
	// 	} else {
	// 		// quick path: no NULL values
	// 		for (idx_t i = 0; i < count; i++) {
	// 			auto sidx = ssel.get_index(i);
	// 			OP::template Operation(*states[sidx]);
	// 		}
	// 	}
	// }

	// template <class INPUT_TYPE, class STATE, class OP>
	// static void CountScatter(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	//                          idx_t count) {
	// 	auto &input = inputs[0];
	// 	if (input.GetVectorType() == VectorType::FLAT_VECTOR && states.GetVectorType() == VectorType::FLAT_VECTOR) {
	// 		auto sdata = FlatVector::GetData<STATE *>(states);
	// 		CountFlatLoop<INPUT_TYPE, STATE, OP>(sdata, FlatVector::Validity(input), count);
	// 	} else {
	// 		UnifiedVectorFormat idata, sdata;
	// 		input.ToUnifiedFormat(count, idata);
	// 		states.ToUnifiedFormat(count, sdata);
	// 		CountScatterLoop<INPUT_TYPE, STATE, OP>(reinterpret_cast<STATE **>(sdata.data), *idata.sel, *sdata.sel, idata.validity, count);
	// 	}
	// }

	struct AggState13
	{
		bool isInitialized;
		int32_t count;
	};

	struct CustomAggOperation
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
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.count = input2;
			}
			state.count ++;
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				// finalize_data.ReturnNull();
				target = 0;
				return;
			}
			target = state.count;
		}

		static bool IgnoreNull()
		{
			return false;
		}
	};

	struct CustomAggOperation13
	{
		// static idx_t StateSize() {
		//   return sizeof(AggState);
		// }

		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
			// state.count = 0;
		}

		template <class INPUT_TYPE, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
			}
			// state.count += count;
		}

		template <class INPUT_TYPE, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input)
		{
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.count = 0;
			}
			state.count++;
		}

		template <class STATE, class OP>
		static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
			if (source.isInitialized == false) {
				target.count = 0;
			}
			else{
				target.count += source.count;
			}
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				// finalize_data.ReturnNull();
				target = (int64_t)0;
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
		// static idx_t StateSize() {
		//   return sizeof(AggState);
		// }

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
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.revenue = 0;
			}
			state.revenue += input1 * (1 - input2);
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				// finalize_data.ReturnNull();
				target = 0;
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
		// static idx_t StateSize() {
		//   return sizeof(AggState);
		// }

		template <class STATE>
		static void Initialize(STATE &state)
		{
			state.isInitialized = false;
		}

		template <class INPUT_TYPE, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
			}
		}

		template <class INPUT_TYPE, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input)
		{
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.volumn = 0;
			}
			state.volumn += input;
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				target = 0;
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
		// static idx_t StateSize() {
		//   return sizeof(AggState);
		// }

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
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.revenue = 0;
			}
			state.revenue += input1 * (1 - input2);
		}

		template <class TARGET_TYPE, class STATE>
		static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
		{
			// std::cout<<"finalize called"<<std::endl;
			if (state.isInitialized == false)
			{
				// finalize_data.ReturnNull();
				target = 0;
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

		template <class INPUT_TYPE1, class INPUT_TYPE2, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, AggregateBinaryInput &tmp, idx_t count)
		{
			for (idx_t i = 0; i < count; i++)
			{
				Operation<INPUT_TYPE1, INPUT_TYPE2, STATE, OP>(state, input1, input2, tmp);
			}
		}

		template <class INPUT_TYPE1, class INPUT_TYPE2, class STATE, class OP>
		static void Operation(STATE &state, const INPUT_TYPE1 &input1, const INPUT_TYPE2 &input2, AggregateBinaryInput &)
		{
			if(state.isInitialized == false){
				state.isInitialized = true;
				state.min_cost = input1;
			}
			if(input1 < state.min_cost){
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
		auto ordersbycustomeraggregate = AggregateFunction::UnaryAggregate<AggState13, int32_t, int32_t, CustomAggOperation13>(LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);

		// custom_agg_function.update = CountScatter<int64_t, AggState, CustomAggOperation>;
		// auto custom_agg_function = AggregateFunction::UnaryAggregate<AggState, int64_t, int32_t, CustomAggOperation>(LogicalType::BIGINT, LogicalType::INTEGER);
		ordersbycustomeraggregate.name = "ordersbycustomeraggregate";
		ExtensionUtil::RegisterFunction(instance, ordersbycustomeraggregate);
		auto custom_agg = BinaryBaseAggregate<AggState13, int32_t, int32_t, int32_t, CustomAggOperation>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);

		// // custom_agg_function.update = CountScatter<int64_t, AggState, CustomAggOperation>;
		// // auto custom_agg_function = AggregateFunction::UnaryAggregate<AggState, int64_t, int32_t, CustomAggOperation>(LogicalType::BIGINT, LogicalType::INTEGER);
		custom_agg.name = "customeraggregate";
		ExtensionUtil::RegisterFunction(instance, custom_agg);

		auto promo_revenue_agg = BinaryBaseAggregate<AggState14, int64_t, int64_t, int64_t, CustomAggOperation14>(LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		promo_revenue_agg.name = "promo_revenue_agg";
		ExtensionUtil::RegisterFunction(instance, promo_revenue_agg);
		// equivalent to sum
		auto volume_customer_agg = UnaryBaseAggregate<AggState18, int32_t, int64_t, CustomAggOperation18>(LogicalType::INTEGER, LogicalType::BIGINT, FunctionNullHandling::SPECIAL_HANDLING);
		volume_customer_agg.name = "volume_customer_agg";
		ExtensionUtil::RegisterFunction(instance, volume_customer_agg);
		// same as 14
		auto sum_discounted_price = BinaryBaseAggregate<AggState19, int32_t, int32_t, int32_t, CustomAggOperation19>(LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);
		sum_discounted_price.name = "sum_discounted_price";
		ExtensionUtil::RegisterFunction(instance, sum_discounted_price);
		// equivalent to arg_min
		auto MinCostSuppWithCustomAgg = BinaryBaseAggregate<AggState2, int32_t, string_t, string_t, CustomAggOperation2>(LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, FunctionNullHandling::SPECIAL_HANDLING);
		MinCostSuppWithCustomAgg.name = "MinCostSuppAggregate";
		ExtensionUtil::RegisterFunction(instance, MinCostSuppWithCustomAgg);
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
