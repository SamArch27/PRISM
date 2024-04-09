#include "types.hpp"
#include "utils.hpp"
#include <regex>
#define FMT_HEADER_ONLY

Type Type::BOOLEAN = Type(false, std::nullopt, std::nullopt,
                          PostgresTypeTag::BOOLEAN, "BOOLEAN");
Type Type::INT =
    Type(false, std::nullopt, std::nullopt, PostgresTypeTag::INT, "INT");

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

duckdb::LogicalType Type::getDuckDBLogicalType() const {
  switch (duckdbTag) {
    {
    case DuckdbTypeTag::BIGINT:
      return duckdb::LogicalType::BIGINT;
    }
    {
    case DuckdbTypeTag::BIT:
      return duckdb::LogicalType::BIT;
    }
    {
    case DuckdbTypeTag::BLOB:
      return duckdb::LogicalType::BLOB;
    }
    {
    case DuckdbTypeTag::BOOLEAN:
      return duckdb::LogicalType::BOOLEAN;
    }
    {
    case DuckdbTypeTag::DATE:
      return duckdb::LogicalType::DATE;
    }
    {
    case DuckdbTypeTag::DECIMAL:
      return duckdb::LogicalType::DECIMAL(*width, *scale);
    }
    {
    case DuckdbTypeTag::DOUBLE:
      return duckdb::LogicalType::DOUBLE;
    }
    {
    case DuckdbTypeTag::HUGEINT:
      return duckdb::LogicalType::HUGEINT;
    }
    {
    case DuckdbTypeTag::INTEGER:
      return duckdb::LogicalType::INTEGER;
    }
    {
    case DuckdbTypeTag::INTERVAL:
      return duckdb::LogicalType::INTERVAL;
    }
    {
    case DuckdbTypeTag::REAL:
      return duckdb::LogicalType::FLOAT;
    }
    {
    case DuckdbTypeTag::SMALLINT:
      return duckdb::LogicalType::SMALLINT;
    }
    {
    case DuckdbTypeTag::TIME:
      return duckdb::LogicalType::TIME;
    }
    {
    case DuckdbTypeTag::TIMESTAMP:
      return duckdb::LogicalType::TIMESTAMP;
    }
    {
    case DuckdbTypeTag::TINYINT:
      return duckdb::LogicalType::TINYINT;
    }
    {
    case DuckdbTypeTag::UBIGINT:
      return duckdb::LogicalType::UBIGINT;
    }
    {
    case DuckdbTypeTag::UINTEGER:
      return duckdb::LogicalType::UINTEGER;
    }
    {
    case DuckdbTypeTag::UNKNOWN:
      return duckdb::LogicalType::INVALID;
    }
    {
    case DuckdbTypeTag::USMALLINT:
      return duckdb::LogicalType::USMALLINT;
    }
    {
    case DuckdbTypeTag::UTINYINT:
      return duckdb::LogicalType::UTINYINT;
    }
    {
    case DuckdbTypeTag::UUID:
      return duckdb::LogicalType::UUID;
    }
    {
    case DuckdbTypeTag::VARCHAR:
      return duckdb::LogicalType::VARCHAR;
    }
  default:
    ERROR("Unknown type.");
  }
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
  ERROR("Unknown type.");
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
  ERROR("Unknown type.");
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

PostgresTypeTag Type::getPostgresTag(const String &type) {
  // remove spaces and capitalize the name
  String upper = toUpper(removeSpaces(type));

  Map<String, PostgresTypeTag> nameToTag = {
      {"BIGINT", PostgresTypeTag::BIGINT},
      {"BINARY", PostgresTypeTag::BINARY},
      {"BIT", PostgresTypeTag::BIT},
      {"BITSTRING", PostgresTypeTag::BITSTRING},
      {"BLOB", PostgresTypeTag::BLOB},
      {"BOOL", PostgresTypeTag::BOOL},
      {"BOOLEAN", PostgresTypeTag::BOOLEAN},
      {"BPCHAR", PostgresTypeTag::BPCHAR},
      {"BYTEA", PostgresTypeTag::BYTEA},
      {"CHAR", PostgresTypeTag::CHAR},
      {"DATE", PostgresTypeTag::DATE},
      {"DATETIME", PostgresTypeTag::DATETIME},
      {"DECIMAL", PostgresTypeTag::DECIMAL},
      {"DOUBLE", PostgresTypeTag::DOUBLE},
      {"FLOAT", PostgresTypeTag::FLOAT},
      {"FLOAT4", PostgresTypeTag::FLOAT4},
      {"FLOAT8", PostgresTypeTag::FLOAT8},
      {"HUGEINT", PostgresTypeTag::HUGEINT},
      {"INT", PostgresTypeTag::INT},
      {"INT1", PostgresTypeTag::INT1},
      {"INT2", PostgresTypeTag::INT2},
      {"INT4", PostgresTypeTag::INT4},
      {"INT8", PostgresTypeTag::INT8},
      {"INTEGER", PostgresTypeTag::INTEGER},
      {"INTERVAL", PostgresTypeTag::INTERVAL},
      {"LOGICAL", PostgresTypeTag::LOGICAL},
      {"LONG", PostgresTypeTag::LONG},
      {"NUMERIC", PostgresTypeTag::NUMERIC},
      {"REAL", PostgresTypeTag::REAL},
      {"SHORT", PostgresTypeTag::SHORT},
      {"SIGNED", PostgresTypeTag::SIGNED},
      {"SMALLINT", PostgresTypeTag::SMALLINT},
      {"STRING", PostgresTypeTag::STRING},
      {"TEXT", PostgresTypeTag::TEXT},
      {"TIME", PostgresTypeTag::TIME},
      {"TIMESTAMP", PostgresTypeTag::TIMESTAMP},
      {"TINYINT", PostgresTypeTag::TINYINT},
      {"UBIGINT", PostgresTypeTag::UBIGINT},
      {"UINTEGER", PostgresTypeTag::UINTEGER},
      {"UNKNOWN", PostgresTypeTag::UNKNOWN},
      {"USMALLINT", PostgresTypeTag::USMALLINT},
      {"UTINYINT", PostgresTypeTag::UTINYINT},
      {"UUID", PostgresTypeTag::UUID},
      {"VARBINARY", PostgresTypeTag::VARBINARY},
      {"VARCHAR", PostgresTypeTag::VARCHAR}};

  // Edge case for DECIMAL(width,scale)
  if (upper.starts_with("DECIMAL")) {
    return nameToTag.at("DECIMAL");
  }

  else if (upper.starts_with("VARCHAR")) {
    return nameToTag.at("VARCHAR");
  }

  if (nameToTag.find(upper) == nameToTag.end()) {
    EXCEPTION(type + " is not a valid Postgres Type.");
  }
  return nameToTag.at(upper);
}

Opt<WidthScale> Type::getDecimalWidthScale(const String &type) {
  std::regex decimalPattern("DECIMAL\\((\\d+),(\\d+)\\)",
                            std::regex_constants::icase);
  std::smatch decimalMatch;
  auto strippedString = removeSpaces(type);
  std::regex_search(strippedString, decimalMatch, decimalPattern);
  if (decimalMatch.size() == 3) {
    auto width = std::stoi(decimalMatch[1]);
    auto scale = std::stoi(decimalMatch[2]);
    return {std::make_pair(width, scale)};
  }
  return {};
}