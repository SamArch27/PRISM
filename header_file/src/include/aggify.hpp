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
    template <class STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, class OP>
static inline void Viable5ScatterLoop(AggregateInputData &aggr_input_data, const TYPE0 *__restrict data0, const TYPE1 *__restrict data1, const TYPE2 *__restrict data2, const TYPE3 *__restrict data3, const TYPE4 *__restrict data4,
                                      STATE_TYPE **__restrict states, idx_t count,
                                      const SelectionVector &sel0, const SelectionVector &sel1, const SelectionVector &sel2, const SelectionVector &sel3, const SelectionVector &sel4, const SelectionVector &ssel,
                                      ValidityMask &validity0, ValidityMask &validity1, ValidityMask &validity2, ValidityMask &validity3, ValidityMask &validity4) {
  if (OP::IgnoreNull() && (!validity0.AllValid() || !validity1.AllValid() || !validity2.AllValid() || !validity3.AllValid() || !validity4.AllValid())) {
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
static void ViableScatter(AggregateInputData &aggr_input_data, Vector &vec0, Vector &vec1, Vector &vec2, Vector &vec3, Vector &vec4, Vector &states,
                           idx_t count) {
  UnifiedVectorFormat data0, data1, data2, data3, data4, sdata;

  vec0.ToUnifiedFormat(count, data0);
vec1.ToUnifiedFormat(count, data1);
vec2.ToUnifiedFormat(count, data2);
vec3.ToUnifiedFormat(count, data3);
vec4.ToUnifiedFormat(count, data4);

  states.ToUnifiedFormat(count, sdata);

  ViableScatterLoop<STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, OP>(
      aggr_input_data, UnifiedVectorFormat::GetData<TYPE0>(data0),UnifiedVectorFormat::GetData<TYPE1>(data1),UnifiedVectorFormat::GetData<TYPE2>(data2),UnifiedVectorFormat::GetData<TYPE3>(data3),UnifiedVectorFormat::GetData<TYPE4>(data4), (STATE_TYPE **)sdata.data, count, *data0.sel, *data1.sel, *data2.sel, *data3.sel, *data4.sel, *sdata.sel, data0.validity, data1.validity, data2.validity, data3.validity, data4.validity);
}

template <class STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, class OP>
static inline void ViableScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                        Vector &states, idx_t count) {
  D_ASSERT(input_count == 3);
  ViableScatter<STATE_TYPE, class TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4, OP>(aggr_input_data, inputs[0], inputs[1], inputs[2], inputs[3], inputs[4], states, count);
}
}