#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {
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

    struct AggregateTrinaryInput {
        AggregateTrinaryInput(AggregateInputData &input_p, ValidityMask &left_mask_p, ValidityMask &middle_mask_p,
                            ValidityMask &right_mask_p)
            : input(input_p), left_mask(left_mask_p), middle_mask(middle_mask_p), right_mask(right_mask_p) {
        }

        AggregateInputData &input;
        ValidityMask &left_mask;
        ValidityMask &middle_mask;
        ValidityMask &right_mask;
        idx_t lidx;
        idx_t midx;
        idx_t ridx;
    };

    template <class STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, class OP>
static inline void Viable5ScatterLoop(AggregateInputData &aggr_input_data, const TYPE0 *__restrict data0, const TYPE1 *__restrict data1, const TYPE2 *__restrict data2, const TYPE3 *__restrict data3, const TYPE4 *__restrict data4,
                                      STATE_TYPE **__restrict states, idx_t count,
                                      const SelectionVector &sel0, const SelectionVector &sel1, const SelectionVector &sel2, const SelectionVector &sel3, const SelectionVector &sel4, const SelectionVector &ssel,
                                      ValidityMask &validity0, ValidityMask &validity1, ValidityMask &validity2, ValidityMask &validity3, ValidityMask &validity4) {
  if (OP::IgnoreNull() && (!validity0.AllValid() || !validity1.AllValid() || !validity2.AllValid() || !validity3.AllValid() || !validity4.AllValid()) {
    for (idx_t i = 0; i < count; i++) {
      idx_t idx0 = sel0.get_index(i);
idx_t idx1 = sel1.get_index(i);
idx_t idx2 = sel2.get_index(i);
idx_t idx3 = sel3.get_index(i);
idx_t idx4 = sel4.get_index(i);

      auto sidx = ssel.get_index(i);
      if (validity0.RowIsValid(idx0) && validity1.RowIsValid(idx1) && validity2.RowIsValid(idx2) && validity3.RowIsValid(idx3) && validity4.RowIsValid(idx4)) {
        OP::template Operation<class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, STATE_TYPE, OP>(*states[sidx], data0[idx0], data1[idx1], data2[idx2], data3[idx3], data4[idx4], true, true, true, true, true);
      }
    }
  } else {
    for (idx_t i = 0; i < count; i++) {
      idx_t idx0 = sel0.get_index(i);
idx_t idx1 = sel1.get_index(i);
idx_t idx2 = sel2.get_index(i);
idx_t idx3 = sel3.get_index(i);
idx_t idx4 = sel4.get_index(i);

      auto sidx = ssel.get_index(i);
      OP::template Operation<class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, STATE_TYPE, OP>(*states[sidx], data0[idx0], data1[idx1], data2[idx2], data3[idx3], data4[idx4], validity0.RowIsValid(idx0), validity1.RowIsValid(idx1), validity2.RowIsValid(idx2), validity3.RowIsValid(idx3), validity4.RowIsValid(idx4));
    }
  }
}
template <class STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, class OP>
static void ViableScatter(AggregateInputData &aggr_input_data, Vector &a, Vector &b, Vector &c, Vector &states,
                           idx_t count) {
  UnifiedVectorFormat adata, bdata, cdata, sdata;

  a.ToUnifiedFormat(count, adata);
  b.ToUnifiedFormat(count, bdata);
  c.ToUnifiedFormat(count, cdata);
  states.ToUnifiedFormat(count, sdata);

  ViableScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, C_TYPE, OP>(
      aggr_input_data, UnifiedVectorFormat::GetData<A_TYPE>(adata), UnifiedVectorFormat::GetData<B_TYPE>(bdata),
      UnifiedVectorFormat::GetData<C_TYPE>(cdata), (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel,
      *cdata.sel, *sdata.sel, adata.validity, bdata.validity, cdata.validity);
}

template <class STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, class OP>
static inline void ViableScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                        Vector &states, idx_t count) {
  D_ASSERT(input_count == 3);
  ViableScatter<STATE_TYPE, A_TYPE, B_TYPE, C_TYPE, OP>(aggr_input_data, inputs[0], inputs[1],
                                                                            inputs[2], states, count);
}
}