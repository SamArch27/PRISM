#pragma once
#include "utils.hpp"
#define FMT_HEADER_ONLY
#include <iostream>

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
  UINT8_T
};

std::ostream &operator<<(std::ostream &os, DuckdbTypeTag);
std::ostream &operator<<(std::ostream &os, CppTypeTag);

class Type {
public:
  DuckdbTypeTag getDuckDBTag() const { return duckdbTag; }
  CppTypeTag getCppTag() const { return cppTag; }
  friend std::ostream &operator<<(std::ostream &os, const Type &type) {
    type.print(os);
    return os;
  }
  virtual void print(std::ostream &os) const = 0;

protected:
  Type(DuckdbTypeTag duckdbTag, CppTypeTag cppTag)
      : duckdbTag(duckdbTag), cppTag(cppTag) {}

  DuckdbTypeTag lookupDuckdbTag(PostgresTypeTag pgTag) const;

  DuckdbTypeTag duckdbTag;
  CppTypeTag cppTag;
};

class DecimalType : public Type {

  static constexpr int DEFAULT_WIDTH = 18;
  static constexpr int DEFAULT_SCALE = 3;

public:
  DecimalType(DuckdbTypeTag duckdbTag, int width = DEFAULT_WIDTH,
              int scale = DEFAULT_SCALE)
      : Type(duckdbTag, lookupCppTag(duckdbTag, width)), width(width),
        scale(scale) {
    if (duckdbTag != DuckdbTypeTag::DECIMAL) {
      ERROR("Calling DecimalType constructor for non-decimal type!");
    }
  }
  DecimalType(PostgresTypeTag pgTag, int width = DEFAULT_WIDTH,
              int scale = DEFAULT_SCALE)
      : DecimalType(lookupDuckdbTag(pgTag), width, scale) {}

  friend std::ostream &operator<<(std::ostream &os, const DecimalType &type) {
    type.print(os);
    return os;
  }

  virtual void print(std::ostream &os) const {
    os << fmt::format("DECIMAL({}, {})", width, scale);
  }

  int getWidth() const { return width; }
  int getScale() const { return scale; }

private:
  CppTypeTag lookupCppTag(DuckdbTypeTag duckdbTag, int width) const;

  int width;
  int scale;
};

class NonDecimalType : public Type {
public:
  NonDecimalType(DuckdbTypeTag duckdbTag)
      : Type(duckdbTag, lookupCppTag(duckdbTag)) {
    if (duckdbTag == DuckdbTypeTag::DECIMAL) {
      ERROR("Calling NonDecimalType constructor with decimal type!");
    }
  }
  NonDecimalType(PostgresTypeTag pgTag)
      : NonDecimalType(lookupDuckdbTag(pgTag)) {}

  friend std::ostream &operator<<(std::ostream &os,
                                  const NonDecimalType &type) {
    type.print(os);
    return os;
  }

  virtual void print(std::ostream &os) const { os << duckdbTag; }

private:
  CppTypeTag lookupCppTag(DuckdbTypeTag duckdbTag) const;
};