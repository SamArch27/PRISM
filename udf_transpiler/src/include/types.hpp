#pragma once
#include "utils.hpp"
#define FMT_HEADER_ONLY
#include <iostream>
#include <set>

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
  const static inline std::set<DuckdbTypeTag> NumricTypes = {
    DuckdbTypeTag::BOOLEAN, DuckdbTypeTag::TINYINT, DuckdbTypeTag::SMALLINT,
    DuckdbTypeTag::INTEGER, DuckdbTypeTag::BIGINT, DuckdbTypeTag::DATE, 
    DuckdbTypeTag::TIME, DuckdbTypeTag::TIMESTAMP, DuckdbTypeTag::UBIGINT,
    DuckdbTypeTag::UINTEGER, DuckdbTypeTag::DOUBLE
  };

  const static inline std::set<DuckdbTypeTag> BLOBTypes = {
    DuckdbTypeTag::BLOB, DuckdbTypeTag::VARCHAR
  };

  DuckdbTypeTag getDuckDBTag() const { return duckdbTag; }
  CppTypeTag getCppTag() const { return cppTag; }
  friend std::ostream &operator<<(std::ostream &os, const Type &type) {
    type.print(os);
    return os;
  }
  // no need to consider decimal case here
  virtual std::string getDuckDBType() const = 0;
  virtual std::string getCppType() const = 0;
  virtual std::string defaultValue(bool singleQuote) const = 0;
  std::string getDuckDBLogicalType() const {
    // if(dynamic_cast<const DecimalType*>(this))
    //   return "LogicalType::Decimal";
    return "LogicalType::" + getDuckDBType();
  }
  bool isNumeric() const { return NumricTypes.count(duckdbTag); }
  bool isBLOB() const { return BLOBTypes.count(duckdbTag); }

protected:
  Type(DuckdbTypeTag duckdbTag, CppTypeTag cppTag)
      : duckdbTag(duckdbTag), cppTag(cppTag) {}

  virtual void print(std::ostream &os) const = 0;

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

  std::string getDuckDBType() const override{
    // ERROR("Calling DecimalType::getDuckDBType()!");
    return fmt::format("DECIMAL({}, {})", width, scale);
  }

  std::string getCppType() const override{
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
  }

  std::string defaultValue(bool singleQuote) const {
    return "0";
  }

  int getWidth() const { return width; }
  int getScale() const { return scale; }

private:
  void print(std::ostream &os) const override {
    os << fmt::format("DECIMAL({}, {})", width, scale);
  }

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

  std::string getDuckDBType() const override{
    std::stringstream ss;
    ss << duckdbTag;
    return ss.str();
  }

  std::string getCppType() const override{
    std::stringstream ss;
    ss << cppTag;
    return ss.str();
  }

  std::string defaultValue(bool singleQuote) const override{
    if(isNumeric()){
      return "0";
    }
    else if(isBLOB()){
      if(singleQuote)
        return "''";
      else
        return "\"\"";
    }
    else{
      ERROR("Cannot get default value for non-numeric and non-BLOB type!");
    }
  }

private:
  void print(std::ostream &os) const override { os << duckdbTag; }
  CppTypeTag lookupCppTag(DuckdbTypeTag duckdbTag) const;
};