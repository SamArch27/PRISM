#include "types.hpp"
#include "utils.hpp"
#include <regex>
#define FMT_HEADER_ONLY

std::ostream &operator<<(std::ostream &os, DuckdbTypeTag tag) {
  switch (tag) {
  case DuckdbTypeTag::BIGINT:
    os << "BIGINT";
    break;
  case DuckdbTypeTag::BIT:
    os << "BIT";
    break;
  case DuckdbTypeTag::BLOB:
    os << "BLOB";
    break;
  case DuckdbTypeTag::BOOLEAN:
    os << "BOOLEAN";
    break;
  case DuckdbTypeTag::DATE:
    os << "DATE";
    break;
  case DuckdbTypeTag::DECIMAL:
    os << "DECIMAL";
    break;
  case DuckdbTypeTag::DOUBLE:
    os << "DOUBLE";
    break;
  case DuckdbTypeTag::HUGEINT:
    os << "HUGEINT";
    break;
  case DuckdbTypeTag::INTEGER:
    os << "INTEGER";
    break;
  case DuckdbTypeTag::INTERVAL:
    os << "INTERVAL";
    break;
  case DuckdbTypeTag::REAL:
    os << "REAL";
    break;
  case DuckdbTypeTag::SMALLINT:
    os << "SMALLINT";
    break;
  case DuckdbTypeTag::TIME:
    os << "TIME";
    break;
  case DuckdbTypeTag::TIMESTAMP:
    os << "TIMESTAMP";
    break;
  case DuckdbTypeTag::TINYINT:
    os << "TINYINT";
    break;
  case DuckdbTypeTag::UBIGINT:
    os << "UBIGINT";
    break;
  case DuckdbTypeTag::UINTEGER:
    os << "UINTEGER";
    break;
  case DuckdbTypeTag::UNKNOWN:
    os << "UNKNOWN";
    break;
  case DuckdbTypeTag::USMALLINT:
    os << "USMALLINT";
    break;
  case DuckdbTypeTag::UTINYINT:
    os << "UTINYINT";
    break;
  case DuckdbTypeTag::UUID:
    os << "UUID";
    break;
  case DuckdbTypeTag::VARCHAR:
    os << "VARCHAR";
    break;
  }
  return os;
}
std::ostream &operator<<(std::ostream &os, CppTypeTag tag) {
  switch (tag) {
  case CppTypeTag::BOOL:
    os << "bool";
    break;
  case CppTypeTag::INT8_T:
    os << "int8_t";
    break;
  case CppTypeTag::INT16_T:
    os << "int16_t";
    break;
  case CppTypeTag::INT32_T:
    os << "int32_t";
    break;
  case CppTypeTag::INT64_T:
    os << "int64_t";
    break;
  case CppTypeTag::HUGEINT_T:
    os << "duckdb::hugeint_t";
    break;
  case CppTypeTag::UINT8_T:
    os << "uint8_t";
    break;
  case CppTypeTag::UINT16_T:
    os << "uint16_t";
    break;
  case CppTypeTag::UINT32_T:
    os << "uint32_t";
    break;
  case CppTypeTag::UINT64_T:
    os << "uint64_t";
    break;
  case CppTypeTag::FLOAT:
    os << "float";
    break;
  case CppTypeTag::DOUBLE:
    os << "double";
    break;
  case CppTypeTag::STRING_T:
    os << "string_t";
    break;
  case CppTypeTag::DATE_T:
    os << "date_t";
    break;
  default:
    os << "UNKNOWN";
  }
  return os;
}

DuckdbTypeTag Type::lookupDuckdbTag(PostgresTypeTag pgTag) const {
  switch (pgTag) {
  case PostgresTypeTag::BIGINT:
    return DuckdbTypeTag::BIGINT;
  case PostgresTypeTag::BINARY:
    return DuckdbTypeTag::BLOB;
  case PostgresTypeTag::BIT:
    return DuckdbTypeTag::BIT;
  case PostgresTypeTag::BITSTRING:
    return DuckdbTypeTag::BIT;
  case PostgresTypeTag::BLOB:
    return DuckdbTypeTag::BLOB;
  case PostgresTypeTag::BOOL:
    return DuckdbTypeTag::BOOLEAN;
  case PostgresTypeTag::BOOLEAN:
    return DuckdbTypeTag::BOOLEAN;
  case PostgresTypeTag::BPCHAR:
    return DuckdbTypeTag::VARCHAR;
  case PostgresTypeTag::BYTEA:
    return DuckdbTypeTag::BLOB;
  case PostgresTypeTag::CHAR:
    return DuckdbTypeTag::VARCHAR;
  case PostgresTypeTag::DATE:
    return DuckdbTypeTag::DATE;
  case PostgresTypeTag::DATETIME:
    return DuckdbTypeTag::TIMESTAMP;
  case PostgresTypeTag::DECIMAL:
    return DuckdbTypeTag::DECIMAL;
  case PostgresTypeTag::DOUBLE:
    return DuckdbTypeTag::DOUBLE;
  case PostgresTypeTag::FLOAT4:
    return DuckdbTypeTag::REAL;
  case PostgresTypeTag::FLOAT8:
    return DuckdbTypeTag::DOUBLE;
  case PostgresTypeTag::FLOAT:
    return DuckdbTypeTag::REAL;
  case PostgresTypeTag::HUGEINT:
    return DuckdbTypeTag::HUGEINT;
  case PostgresTypeTag::INT1:
    return DuckdbTypeTag::TINYINT;
  case PostgresTypeTag::INT2:
    return DuckdbTypeTag::SMALLINT;
  case PostgresTypeTag::INT4:
    return DuckdbTypeTag::INTEGER;
  case PostgresTypeTag::INT8:
    return DuckdbTypeTag::BIGINT;
  case PostgresTypeTag::INT:
    return DuckdbTypeTag::INTEGER;
  case PostgresTypeTag::INTEGER:
    return DuckdbTypeTag::INTEGER;
  case PostgresTypeTag::INTERVAL:
    return DuckdbTypeTag::INTERVAL;
  case PostgresTypeTag::LOGICAL:
    return DuckdbTypeTag::BOOLEAN;
  case PostgresTypeTag::LONG:
    return DuckdbTypeTag::BIGINT;
  case PostgresTypeTag::NUMERIC:
    return DuckdbTypeTag::DOUBLE;
  case PostgresTypeTag::REAL:
    return DuckdbTypeTag::REAL;
  case PostgresTypeTag::SHORT:
    return DuckdbTypeTag::SMALLINT;
  case PostgresTypeTag::SIGNED:
    return DuckdbTypeTag::INTEGER;
  case PostgresTypeTag::SMALLINT:
    return DuckdbTypeTag::SMALLINT;
  case PostgresTypeTag::STRING:
    return DuckdbTypeTag::VARCHAR;
  case PostgresTypeTag::TEXT:
    return DuckdbTypeTag::VARCHAR;
  case PostgresTypeTag::TIME:
    return DuckdbTypeTag::TIME;
  case PostgresTypeTag::TIMESTAMP:
    return DuckdbTypeTag::TIMESTAMP;
  case PostgresTypeTag::TINYINT:
    return DuckdbTypeTag::TINYINT;
  case PostgresTypeTag::UBIGINT:
    return DuckdbTypeTag::UBIGINT;
  case PostgresTypeTag::UINTEGER:
    return DuckdbTypeTag::UBIGINT;
  case PostgresTypeTag::USMALLINT:
    return DuckdbTypeTag::USMALLINT;
  case PostgresTypeTag::UTINYINT:
    return DuckdbTypeTag::UTINYINT;
  case PostgresTypeTag::UUID:
    return DuckdbTypeTag::UUID;
  case PostgresTypeTag::VARBINARY:
    return DuckdbTypeTag::BLOB;
  case PostgresTypeTag::VARCHAR:
    return DuckdbTypeTag::VARCHAR;
  case PostgresTypeTag::UNKNOWN:
    return DuckdbTypeTag::UNKNOWN;
  }
}

CppTypeTag Type::lookupCppTag(DuckdbTypeTag duckdbTag, Opt<int> width,
                              Opt<int> scale) const {
  switch (duckdbTag) {
  case DuckdbTypeTag::BIGINT:
    return CppTypeTag::INT64_T;
  case DuckdbTypeTag::BIT:
    ERROR("BIT type is unsupported.");
  case DuckdbTypeTag::BLOB:
    return CppTypeTag::STRING_T;
  case DuckdbTypeTag::BOOLEAN:
    return CppTypeTag::BOOL;
  case DuckdbTypeTag::DATE:
    return CppTypeTag::DATE_T;
  case DuckdbTypeTag::DECIMAL:
    ASSERT(width > 0, "Width of decimal should be > 0.");
    if (width <= 4)
      return CppTypeTag::INT16_T;
    else if (width <= 9)
      return CppTypeTag::INT32_T;
    else if (width <= 18)
      return CppTypeTag::INT64_T;
    else if (width <= 38)
      return CppTypeTag::HUGEINT_T;
    else
      ERROR("Width larger than 38.");
  case DuckdbTypeTag::DOUBLE:
    return CppTypeTag::DOUBLE;
  case DuckdbTypeTag::HUGEINT:
    return CppTypeTag::HUGEINT_T;
  case DuckdbTypeTag::INTEGER:
    return CppTypeTag::INT32_T;
  case DuckdbTypeTag::INTERVAL:
    ERROR("INTERVAL type is unsupported.");
  case DuckdbTypeTag::REAL:
    return CppTypeTag::FLOAT;
  case DuckdbTypeTag::SMALLINT:
    return CppTypeTag::INT16_T;
  case DuckdbTypeTag::TIME:
    return CppTypeTag::INT32_T;
  case DuckdbTypeTag::TIMESTAMP:
    return CppTypeTag::INT64_T;
  case DuckdbTypeTag::TINYINT:
    return CppTypeTag::INT8_T;
  case DuckdbTypeTag::UBIGINT:
    return CppTypeTag::UINT64_T;
  case DuckdbTypeTag::UINTEGER:
    return CppTypeTag::UINT32_T;
  case DuckdbTypeTag::UNKNOWN:
    ERROR("UNKNOWN type is unsupported.");
  case DuckdbTypeTag::USMALLINT:
    return CppTypeTag::UINT16_T;
  case DuckdbTypeTag::UTINYINT:
    return CppTypeTag::UINT8_T;
  case DuckdbTypeTag::UUID:
    ERROR("UUID type is unsupported.");
  case DuckdbTypeTag::VARCHAR:
    return CppTypeTag::STRING_T;
  }
}

void Type::print(std::ostream &os) const {
  if (isDecimal()) {
    os << fmt::format("DECIMAL({}, {})", *width, *scale);
  } else {
    std::stringstream ss;
    ss << duckdbTag;
    os << ss.str();
  }
}