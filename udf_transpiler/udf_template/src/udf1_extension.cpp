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
			if (NotEquals::Operation(InstrOperator::Operation<string_t, string_t, int64_t>(list, part), (int64_t)0))
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

/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */

inline void udf_sam_body(const Value& v0, bool spending1_null, const Value& v1, bool spending2_null, Value& result, bool& result_null) {
  if (spending1_null or spending2_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int64_t spending1 = v0.GetValueUnsafe<int64_t>();
int64_t spending2 = v1.GetValueUnsafe<int64_t>();
int64_t increase;

  if(LessThan::Operation(spending1, spending2)) {
  increase  = -1;

}

else {
  increase  = DecimalSubtractOverflowCheck::Operation<int64_t, int64_t, int64_t>(spending1, spending2);

}
result = Value::DECIMAL(increase, 18, 3);
return;

}
void udf_sam(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &spending1 = args.data[0];
auto spending1_type = spending1.GetVectorType();
UnifiedVectorFormat spending1_data;
spending1.ToUnifiedFormat(count, spending1_data);
auto &spending2 = args.data[1];
auto spending2_type = spending2.GetVectorType();
UnifiedVectorFormat spending2_data;
spending2.ToUnifiedFormat(count, spending2_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto spending1_index = spending1_data.sel->get_index(base_idx);
auto spending2_index = spending2_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf_sam_body(spending1.GetValue(spending1_index), !spending1_data.validity.RowIsValid(spending1_index), spending2.GetValue(spending2_index), !spending2_data.validity.RowIsValid(spending2_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf13_body(const Value& v0, bool numsalesfromstore_null, const Value& v1, bool numsalesfromcatalog_null, const Value& v2, bool numsalesfromweb_null, Value& result, bool& result_null) {
  if (numsalesfromstore_null or numsalesfromcatalog_null or numsalesfromweb_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int32_t numsalesfromstore = v0.GetValueUnsafe<int32_t>();
int32_t numsalesfromcatalog = v1.GetValueUnsafe<int32_t>();
int32_t numsalesfromweb = v2.GetValueUnsafe<int32_t>();
string_t maxchannel;

  if(GreaterThan::Operation(numsalesfromstore, numsalesfromcatalog)) {
  maxchannel  = "Store";
if(GreaterThan::Operation(numsalesfromweb, numsalesfromstore)) {
  maxchannel  = "Web";

}


}

else {
  maxchannel  = "Catalog";
if(GreaterThan::Operation(numsalesfromweb, numsalesfromcatalog)) {
  maxchannel  = "Web";

}


}
result = Value(maxchannel);
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf13(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numsalesfromstore = args.data[0];
auto numsalesfromstore_type = numsalesfromstore.GetVectorType();
UnifiedVectorFormat numsalesfromstore_data;
numsalesfromstore.ToUnifiedFormat(count, numsalesfromstore_data);
auto &numsalesfromcatalog = args.data[1];
auto numsalesfromcatalog_type = numsalesfromcatalog.GetVectorType();
UnifiedVectorFormat numsalesfromcatalog_data;
numsalesfromcatalog.ToUnifiedFormat(count, numsalesfromcatalog_data);
auto &numsalesfromweb = args.data[2];
auto numsalesfromweb_type = numsalesfromweb.GetVectorType();
UnifiedVectorFormat numsalesfromweb_data;
numsalesfromweb.ToUnifiedFormat(count, numsalesfromweb_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numsalesfromstore_index = numsalesfromstore_data.sel->get_index(base_idx);
auto numsalesfromcatalog_index = numsalesfromcatalog_data.sel->get_index(base_idx);
auto numsalesfromweb_index = numsalesfromweb_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf13_body(numsalesfromstore.GetValue(numsalesfromstore_index), !numsalesfromstore_data.validity.RowIsValid(numsalesfromstore_index), numsalesfromcatalog.GetValue(numsalesfromcatalog_index), !numsalesfromcatalog_data.validity.RowIsValid(numsalesfromcatalog_index), numsalesfromweb.GetValue(numsalesfromweb_index), !numsalesfromweb_data.validity.RowIsValid(numsalesfromweb_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf15_body(const Value& v0, bool incomeband_null, Value& result, bool& result_null) {
  if (incomeband_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int32_t incomeband = v0.GetValueUnsafe<int32_t>();
string_t clevel;

  if((GreaterThanEquals::Operation(incomeband, 0) && LessThanEquals::Operation(incomeband, 3))) {
  clevel  = "low";

}

if((GreaterThanEquals::Operation(incomeband, 4) && LessThanEquals::Operation(incomeband, 7))) {
  clevel  = "lowerMiddle";

}

if((GreaterThanEquals::Operation(incomeband, 8) && LessThanEquals::Operation(incomeband, 11))) {
  clevel  = "upperMiddle";

}

if((GreaterThanEquals::Operation(incomeband, 12) && LessThanEquals::Operation(incomeband, 16))) {
  clevel  = "high";

}

if((GreaterThanEquals::Operation(incomeband, 17) && LessThanEquals::Operation(incomeband, 20))) {
  clevel  = "affluent";

}

result = Value(clevel);
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf15(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &incomeband = args.data[0];
auto incomeband_type = incomeband.GetVectorType();
UnifiedVectorFormat incomeband_data;
incomeband.ToUnifiedFormat(count, incomeband_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto incomeband_index = incomeband_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf15_body(incomeband.GetValue(incomeband_index), !incomeband_data.validity.RowIsValid(incomeband_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf17_body(const Value& v0, bool numweb_null, const Value& v1, bool numstore_null, const Value& v2, bool numcat_null, Value& result, bool& result_null) {
  if (numweb_null or numstore_null or numcat_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int32_t numweb = v0.GetValueUnsafe<int32_t>();
int32_t numstore = v1.GetValueUnsafe<int32_t>();
int32_t numcat = v2.GetValueUnsafe<int32_t>();

  if((GreaterThanEquals::Operation(numweb, numstore) && GreaterThanEquals::Operation(numweb, numcat))) {
  result = Value("web");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numstore, numweb) && GreaterThanEquals::Operation(numstore, numcat))) {
  result = Value("store");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numcat, numstore) && GreaterThanEquals::Operation(numcat, numweb))) {
  result = Value("Catalog");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

result = Value("Logical error");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf17(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numweb = args.data[0];
auto numweb_type = numweb.GetVectorType();
UnifiedVectorFormat numweb_data;
numweb.ToUnifiedFormat(count, numweb_data);
auto &numstore = args.data[1];
auto numstore_type = numstore.GetVectorType();
UnifiedVectorFormat numstore_data;
numstore.ToUnifiedFormat(count, numstore_data);
auto &numcat = args.data[2];
auto numcat_type = numcat.GetVectorType();
UnifiedVectorFormat numcat_data;
numcat.ToUnifiedFormat(count, numcat_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numweb_index = numweb_data.sel->get_index(base_idx);
auto numstore_index = numstore_data.sel->get_index(base_idx);
auto numcat_index = numcat_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf17_body(numweb.GetValue(numweb_index), !numweb_data.validity.RowIsValid(numweb_index), numstore.GetValue(numstore_index), !numstore_data.validity.RowIsValid(numstore_index), numcat.GetValue(numcat_index), !numcat_data.validity.RowIsValid(numcat_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf18_body(const Value& v0, bool numweb_null, const Value& v1, bool numstore_null, const Value& v2, bool numcat_null, Value& result, bool& result_null) {
  if (numweb_null or numstore_null or numcat_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int64_t numweb = v0.GetValueUnsafe<int64_t>();
int64_t numstore = v1.GetValueUnsafe<int64_t>();
int64_t numcat = v2.GetValueUnsafe<int64_t>();
string_t retvalue;

  if((GreaterThanEquals::Operation(numweb, numstore) && GreaterThanEquals::Operation(numweb, numcat))) {
  retvalue  = "web";

}
else if ((GreaterThanEquals::Operation(numstore, numweb) && GreaterThanEquals::Operation(numstore, numcat))) {
  retvalue  = "store";

}
else if ((GreaterThanEquals::Operation(numcat, numstore) && GreaterThanEquals::Operation(numcat, numweb))) {
  retvalue  = "Catalog";

}

else {
  retvalue  = "Logical error";

}
result = Value(retvalue);
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf18(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numweb = args.data[0];
auto numweb_type = numweb.GetVectorType();
UnifiedVectorFormat numweb_data;
numweb.ToUnifiedFormat(count, numweb_data);
auto &numstore = args.data[1];
auto numstore_type = numstore.GetVectorType();
UnifiedVectorFormat numstore_data;
numstore.ToUnifiedFormat(count, numstore_data);
auto &numcat = args.data[2];
auto numcat_type = numcat.GetVectorType();
UnifiedVectorFormat numcat_data;
numcat.ToUnifiedFormat(count, numcat_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numweb_index = numweb_data.sel->get_index(base_idx);
auto numstore_index = numstore_data.sel->get_index(base_idx);
auto numcat_index = numcat_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf18_body(numweb.GetValue(numweb_index), !numweb_data.validity.RowIsValid(numweb_index), numstore.GetValue(numstore_index), !numstore_data.validity.RowIsValid(numstore_index), numcat.GetValue(numcat_index), !numcat_data.validity.RowIsValid(numcat_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf20b_body(const Value& v0, bool m_null, const Value& v1, bool cnt1_null, const Value& v2, bool cnt2_null, Value& result, bool& result_null) {
  if (m_null or cnt1_null or cnt2_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  string_t m = v0.GetValueUnsafe<string_t>();
int32_t cnt1 = v1.GetValueUnsafe<int32_t>();
int32_t cnt2 = v2.GetValueUnsafe<int32_t>();
string_t man;

  if((GreaterThan::Operation(cnt1, 0) && GreaterThan::Operation(cnt2, 0))) {
  man  = m;

}

else {
  man  = "outdated item";

}
result = Value(man);
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf20b(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &m = args.data[0];
auto m_type = m.GetVectorType();
UnifiedVectorFormat m_data;
m.ToUnifiedFormat(count, m_data);
auto &cnt1 = args.data[1];
auto cnt1_type = cnt1.GetVectorType();
UnifiedVectorFormat cnt1_data;
cnt1.ToUnifiedFormat(count, cnt1_data);
auto &cnt2 = args.data[2];
auto cnt2_type = cnt2.GetVectorType();
UnifiedVectorFormat cnt2_data;
cnt2.ToUnifiedFormat(count, cnt2_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto m_index = m_data.sel->get_index(base_idx);
auto cnt1_index = cnt1_data.sel->get_index(base_idx);
auto cnt2_index = cnt2_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf20b_body(m.GetValue(m_index), !m_data.validity.RowIsValid(m_index), cnt1.GetValue(cnt1_index), !cnt1_data.validity.RowIsValid(cnt1_index), cnt2.GetValue(cnt2_index), !cnt2_data.validity.RowIsValid(cnt2_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf7_body(const Value& v0, bool netprofit_null, Value& result, bool& result_null) {
  if (netprofit_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int64_t netprofit = v0.GetValueUnsafe<int64_t>();

  if(GreaterThan::Operation(netprofit, DecimalCastHelper<int32_t, int64_t, TryCastToDecimal>(0, 15, 2))) {
  result = Value::INTEGER(1);
return;

}

else {
  result = Value::INTEGER(0);
return;

}
result = Value::INTEGER(0);
return;

}
void udf7(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &netprofit = args.data[0];
auto netprofit_type = netprofit.GetVectorType();
UnifiedVectorFormat netprofit_data;
netprofit.ToUnifiedFormat(count, netprofit_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto netprofit_index = netprofit_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf7_body(netprofit.GetValue(netprofit_index), !netprofit_data.validity.RowIsValid(netprofit_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf9a_body(const Value& v0, bool numstates_null, Value& result, bool& result_null) {
  if (numstates_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int32_t numstates = v0.GetValueUnsafe<int32_t>();

  if(GreaterThanEquals::Operation(numstates, 4)) {
  result = Value("highly correlated");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numstates, 2) && LessThanEquals::Operation(numstates, 3))) {
  result = Value("somewhat correlated");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numstates, 0) && LessThanEquals::Operation(numstates, 1))) {
  result = Value("no correlation");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

result = Value("error");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf9a(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numstates = args.data[0];
auto numstates_type = numstates.GetVectorType();
UnifiedVectorFormat numstates_data;
numstates.ToUnifiedFormat(count, numstates_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numstates_index = numstates_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf9a_body(numstates.GetValue(numstates_index), !numstates_data.validity.RowIsValid(numstates_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}
inline void udf9b_body(const Value& v0, bool numstates_null, Value& result, bool& result_null) {
  if (numstates_null)
  {
    result_null = true;
    return;
  }
  // the declaration / initialization of local variables
  int32_t numstates = v0.GetValueUnsafe<int32_t>();

  if(GreaterThanEquals::Operation(numstates, 4)) {
  result = Value("highly correlated");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numstates, 2) && LessThanEquals::Operation(numstates, 3))) {
  result = Value("somewhat correlated");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

if((GreaterThanEquals::Operation(numstates, 0) && LessThanEquals::Operation(numstates, 1))) {
  result = Value("no correlation");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}

result = Value("error");
result.GetTypeMutable() = LogicalType::VARCHAR;
return;

}
void udf9b(DataChunk &args, ExpressionState &state, Vector &result) {
  const int count = args.size();
  result.SetVectorType(VectorType::FLAT_VECTOR);

  // the extraction of function arguments
  auto &numstates = args.data[0];
auto numstates_type = numstates.GetVectorType();
UnifiedVectorFormat numstates_data;
numstates.ToUnifiedFormat(count, numstates_data);

  

  for (int base_idx = 0; base_idx < count; base_idx++) {
    auto numstates_index = numstates_data.sel->get_index(base_idx);

    Value temp_result;
    bool  temp_result_null = false;
    udf9b_body(numstates.GetValue(numstates_index), !numstates_data.validity.RowIsValid(numstates_index), temp_result, temp_result_null);
    if (temp_result_null) {
      FlatVector::SetNull(result, base_idx, true);
    }
    else {
      result.SetValue(base_idx, std::move(temp_result));
    }
  }

  result.Verify(count);
}/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */
	

	static void LoadInternal(DatabaseInstance &instance)
	{
		con = make_uniq<Connection>(instance);
		context = con->context.get();
		// auto isListDistinct_scalar_function = ScalarFunction("isListDistinct", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		// 													 LogicalType::BOOLEAN, isListDistinct);
		// ExtensionUtil::RegisterFunction(instance, isListDistinct_scalar_function);
/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */
auto udf_sam_scalar_function = ScalarFunction("udf_sam", {LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3)}, LogicalType::DECIMAL(18, 3), udf_sam);
ExtensionUtil::RegisterFunction(instance, udf_sam_scalar_function);
auto udf13_scalar_function = ScalarFunction("udf13", {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::VARCHAR, udf13);
ExtensionUtil::RegisterFunction(instance, udf13_scalar_function);
auto udf15_scalar_function = ScalarFunction("udf15", {LogicalType::INTEGER}, LogicalType::VARCHAR, udf15);
ExtensionUtil::RegisterFunction(instance, udf15_scalar_function);
auto udf17_scalar_function = ScalarFunction("udf17", {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::VARCHAR, udf17);
ExtensionUtil::RegisterFunction(instance, udf17_scalar_function);
auto udf18_scalar_function = ScalarFunction("udf18", {LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3)}, LogicalType::VARCHAR, udf18);
ExtensionUtil::RegisterFunction(instance, udf18_scalar_function);
auto udf20b_scalar_function = ScalarFunction("udf20b", {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::VARCHAR, udf20b);
ExtensionUtil::RegisterFunction(instance, udf20b_scalar_function);
auto udf7_scalar_function = ScalarFunction("udf7", {LogicalType::DECIMAL(15, 2)}, LogicalType::INTEGER, udf7);
ExtensionUtil::RegisterFunction(instance, udf7_scalar_function);
auto udf9a_scalar_function = ScalarFunction("udf9a", {LogicalType::INTEGER}, LogicalType::VARCHAR, udf9a);
ExtensionUtil::RegisterFunction(instance, udf9a_scalar_function);
auto udf9b_scalar_function = ScalarFunction("udf9b", {LogicalType::INTEGER}, LogicalType::VARCHAR, udf9b);
ExtensionUtil::RegisterFunction(instance, udf9b_scalar_function);
/* ==== Unique identifier to indicate register insertion point end: 04rj39jds934 ==== */
	}

	void Udf1Extension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string Udf1Extension::Name()
	{
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
		return "udf2";
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	}

} // namespace duckdb

extern "C"
{
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API void udf2_init(duckdb::DatabaseInstance &db)
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		LoadInternal(db);
	}
/* ==== Unique identifier to indicate change required start: 9340jfsa034 ==== */
	DUCKDB_EXTENSION_API const char *udf2_version()
/* ==== Unique identifier to indicate change required end: 9340jfsa034 ==== */
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
