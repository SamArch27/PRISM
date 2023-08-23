#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"

namespace duckdb {

template <class SRC>
static BoundCastInfo InternalNumericCastSwitch(const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::BOOLEAN:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, bool, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "bool"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, int8_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "int8_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, int16_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "int16_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, int32_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "int32_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, int64_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "int64_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, uint8_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "uint8_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, uint16_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "uint16_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, uint32_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "uint32_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, uint64_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "uint64_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, hugeint_t, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "hugeint_t"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, float, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "float"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, double, duckdb::NumericTryCast>, ScalarFunctionInfo("NumericTryCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), "double"}, {ScalarFunctionInfo::NumericCastWrapper}));
	case LogicalTypeId::DECIMAL:
		return BoundCastInfo(&VectorCastHelpers::ToDecimalCast<SRC>, ScalarFunctionInfo("TryCastToDecimal::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id()), ScalarFunctionInfo::PhysicalTypeIdToCppType(target.InternalType())}, {ScalarFunctionInfo::DecimalCastWrapper}));
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<SRC, duckdb::StringCast>, ScalarFunctionInfo("StringCast::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id())}, {ScalarFunctionInfo::VectorBackWrapper}));
	case LogicalTypeId::BIT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<SRC, duckdb::NumericTryCastToBit>, ScalarFunctionInfo("NumericTryCastToBit::Operation", {ScalarFunctionInfo::LogicalTypeIdToCppType(source.id())}, {ScalarFunctionInfo::VectorBackWrapper}));
	default:
		return DefaultCasts::TryVectorNullCast; // udf_todo
	}
}

BoundCastInfo DefaultCasts::NumericCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	switch (source.id()) {
	case LogicalTypeId::BOOLEAN:
		return InternalNumericCastSwitch<bool>(source, target);
	case LogicalTypeId::TINYINT:
		return InternalNumericCastSwitch<int8_t>(source, target);
	case LogicalTypeId::SMALLINT:
		return InternalNumericCastSwitch<int16_t>(source, target);
	case LogicalTypeId::INTEGER:
		return InternalNumericCastSwitch<int32_t>(source, target);
	case LogicalTypeId::BIGINT:
		return InternalNumericCastSwitch<int64_t>(source, target);
	case LogicalTypeId::UTINYINT:
		return InternalNumericCastSwitch<uint8_t>(source, target);
	case LogicalTypeId::USMALLINT:
		return InternalNumericCastSwitch<uint16_t>(source, target);
	case LogicalTypeId::UINTEGER:
		return InternalNumericCastSwitch<uint32_t>(source, target);
	case LogicalTypeId::UBIGINT:
		return InternalNumericCastSwitch<uint64_t>(source, target);
	case LogicalTypeId::HUGEINT:
		return InternalNumericCastSwitch<hugeint_t>(source, target);
	case LogicalTypeId::FLOAT:
		return InternalNumericCastSwitch<float>(source, target);
	case LogicalTypeId::DOUBLE:
		return InternalNumericCastSwitch<double>(source, target);
	default:
		throw InternalException("NumericCastSwitch called with non-numeric argument");
	}
}

} // namespace duckdb
