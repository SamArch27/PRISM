#pragma once
#include "duckdb/common/types.hpp"
#include "utils.hpp"
#define FMT_HEADER_ONLY
#include <iostream>
#include <set>

using WidthScale = Pair<int, int>;

enum class PostgresTypeTag {
  BIGINT,
  BINARY,
  BIT,
  BITSTRING,
  BLOB,
  BOOL,
  BOOLEAN,
  BPCHAR,
  BYTEA,
  CHAR,
  DATE,
  DATETIME,
  DECIMAL,
  DOUBLE,
  FLOAT,
  FLOAT4,
  FLOAT8,
  HUGEINT,
  INT,
  INT1,
  INT2,
  INT4,
  INT8,
  INTEGER,
  INTERVAL,
  LOGICAL,
  LONG,
  NUMERIC,
  REAL,
  SHORT,
  SIGNED,
  SMALLINT,
  STRING,
  TEXT,
  TIME,
  TIMESTAMP,
  TINYINT,
  UBIGINT,
  UINTEGER,
  UNKNOWN,
  USMALLINT,
  UTINYINT,
  UUID,
  VARBINARY,
  VARCHAR
};
enum class DuckdbTypeTag {
  BIGINT,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  HUGEINT,
  INTEGER,
  INTERVAL,
  REAL,
  SMALLINT,
  TIME,
  TIMESTAMP,
  TINYINT,
  UBIGINT,
  UINTEGER,
  UNKNOWN,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR
};
enum class CppTypeTag {
  BOOL,
  DOUBLE,
  FLOAT,
  HUGEINT_T,
  INT16_T,
  INT32_T,
  INT64_T,
  INT8_T,
  STRING_T,
  UINT16_T,
  UINT32_T,
  UINT64_T,
  UINT8_T,
  DATE_T
};

std::ostream &operator<<(std::ostream &os, DuckdbTypeTag);
std::ostream &operator<<(std::ostream &os, CppTypeTag);

class Type {
public:
  static constexpr int DEFAULT_WIDTH = 18;
  static constexpr int DEFAULT_SCALE = 3;

  static Type BOOLEAN;
  static Type INT;

  Type(bool decimal, Opt<int> width, Opt<int> scale,
       PostgresTypeTag postgresTag, String typeString)
      : decimal(decimal), width(getWidth(isDecimal(), width)),
        scale(getScale(isDecimal(), scale)), postgresTag(postgresTag),
        duckdbTag(lookupDuckdbTag(postgresTag)),
        cppTag(lookupCppTag(duckdbTag, width, scale)), typeString(typeString) {}

  String serialize() const { return typeString; }

  static Opt<WidthScale> getDecimalWidthScale(const String &type);
  static PostgresTypeTag getPostgresTag(const String &name);

  static Type fromString(const String &str) {
    auto tag = getPostgresTag(str);
    if (tag == PostgresTypeTag::DECIMAL) {
      // provide width, scale info if available
      auto widthScale = getDecimalWidthScale(str);
      if (widthScale) {
        auto [width, scale] = *widthScale;
        return Type(true, width, scale, tag, str);
      } else {
        return Type(true, std::nullopt, std::nullopt, tag, str);
      }
    } else {
      return Type(false, std::nullopt, std::nullopt, tag, str);
    }
  }

  static Opt<int> getWidth(bool decimal, Opt<int> width) {
    if (decimal) {
      if (!width) {
        return DEFAULT_WIDTH;
      }
    }
    return width;
  }

  static Opt<int> getScale(bool decimal, Opt<int> scale) {
    if (decimal) {
      if (!scale) {
        return DEFAULT_SCALE;
      }
    }
    return scale;
  }

  const static inline Set<DuckdbTypeTag> NumericTypes = {
      DuckdbTypeTag::BOOLEAN,  DuckdbTypeTag::TINYINT, DuckdbTypeTag::SMALLINT,
      DuckdbTypeTag::INTEGER,  DuckdbTypeTag::BIGINT,  DuckdbTypeTag::UBIGINT,
      DuckdbTypeTag::UINTEGER, DuckdbTypeTag::DECIMAL, DuckdbTypeTag::DOUBLE};

  const static inline Set<DuckdbTypeTag> BlobTypes = {DuckdbTypeTag::BLOB,
                                                      DuckdbTypeTag::VARCHAR};

  DuckdbTypeTag getDuckDBTag() const { return duckdbTag; }
  CppTypeTag getCppTag() const { return cppTag; }
  friend std::ostream &operator<<(std::ostream &os, const Type &type) {
    type.print(os);
    return os;
  }
  Opt<int> getWidth() const { return width; }
  Opt<int> getScale() const { return scale; }

  String getDuckDBType() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
  }

  String getCppType() const {
    if (isDecimal()) {
      ASSERT(width > 0, "Width of decimal should be > 0.");
      if (width <= 4)
        return "int16_t";
      else if (width <= 9)
        return "int32_t";
      else if (width <= 18)
        return "int64_t";
      else if (width <= 38)
        return "duckdb::hugeint_t";
      else
        ERROR("Width larger than 38.");
    } else {
      std::stringstream ss;
      ss << cppTag;
      return ss.str();
    }
  }
  String getDefaultValue(bool singleQuote) const {
    if (isNumeric()) {
      return "0";
    } else if (isBlob()) {
      if (singleQuote)
        return "''";
      else
        return "\"\"";
    } else if (duckdbTag == DuckdbTypeTag::DATE) {
      return "0::DATE";
    } else {
      ERROR("Cannot get default value for non-numeric and non-BLOB type!");
    }
  }

  String getDuckDBLogicalTypeStr() const {
    return "LogicalType::" + getDuckDBType();
  }

  duckdb::LogicalType getDuckDBLogicalType() const;

  bool isDecimal() const { return decimal; }
  bool isNumeric() const { return NumericTypes.count(duckdbTag); }
  bool isBlob() const { return BlobTypes.count(duckdbTag); }

protected:
  void print(std::ostream &os) const;
  DuckdbTypeTag lookupDuckdbTag(PostgresTypeTag pgTag) const;
  CppTypeTag lookupCppTag(DuckdbTypeTag duckdbTag, Opt<int> width,
                          Opt<int> scale) const;

  bool decimal;
  Opt<int> width;
  Opt<int> scale;
  PostgresTypeTag postgresTag;
  DuckdbTypeTag duckdbTag;
  CppTypeTag cppTag;
  String typeString;
};
