#pragma once
#include <cassert>
#include <iostream>
#include <list>
#include <numeric>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#define FMT_HEADER_ONLY
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include "compiler_fmt/core.h"
#include <regex>
#include <yaml-cpp/yaml.h>

template <class A> using Own = std::unique_ptr<A>;
template <class A> using Shared = std::shared_ptr<A>;

template <typename A, typename B = A, typename... Args>
Own<A> Make(Args &&... xs) {
  return std::make_unique<B>(std::forward<Args>(xs)...);
}

template <typename A, typename B = A, typename... Args>
Own<A> MakeShared(Args &&... xs) {
  return std::make_shared<B>(std::forward<Args>(xs)...);
}

template <typename A> using Queue = std::queue<A>;

template <typename A> using Set = std::unordered_set<A>;

template <typename A> using List = std::list<A>;

template <typename A> using Vec = std::vector<A>;

template <typename A> using VecOwn = Vec<Own<A>>;

template <typename A> using ListOwn = List<Own<A>>;

template <typename A> using Opt = std::optional<A>;

template <typename A, typename B> using Map = std::unordered_map<A, B>;

using String = std::string;

#define COUT std::cout
#define ENDL std::endl

#define ASSERT(condition, message)                                             \
  do {                                                                         \
    if (!(condition)) {                                                        \
      COUT << "Assertion `" #condition "` failed in " << __FILE__         \
                << " line " << __LINE__ << ": " << message << ENDL;       \
      std::terminate();                                                        \
    }                                                                          \
  } while (false)

#define ERROR(message)                                                         \
  do {                                                                         \
    COUT << "Error: " << message << " (" << __FILE__ << ":" << __LINE__   \
              << ")" << ENDL;                                             \
    std::terminate();                                                          \
  } while (false)

#define EXCEPTION(message)                                                     \
  do {                                                                         \
    COUT << "Exception: " << message << " (" << __FILE__ << ":"           \
              << __LINE__ << ")" << ENDL;                                 \
    throw duckdb::ParserException("See the above message.");                   \
  } while (false)

template <typename T>
String vector_join(const Vec<T> &vec, String sep) {
  ERROR("vector_join not implemented for this type");
}

template <>
String vector_join(const Vec<String> &vec, String sep);

template <typename T>
String list_join(List<T> &any_list, String sep) {
  ERROR("list_join not implemented for this type");
}

template <>
String list_join(List<String> &any_list, String sep);

String toLower(const String &str);
String toUpper(const String &str);
String removeSpaces(const String &str);

Vec<String> extractMatches(const String &str,
                                        const char *pattern,
                                        std::size_t group = 1);

static Map<String, String> alias_to_duckdb_type = {
    {"UNKNOWN", "UNKNOWN"},
    {"BIGINT", "BIGINT"},
    {"INT8", "BIGINT"},
    {"LONG", "BIGINT"},
    {"BIT", "BIT"},
    {"BITSTRING", "BIT"},
    {"BOOLEAN", "BOOLEAN"},
    {"BOOL", "BOOLEAN"},
    {"LOGICAL", "BOOLEAN"},
    {"BLOB", "BLOB"},
    {"BYTEA", "BLOB"},
    {"BINARY", "BLOB"},
    {"VARBINARY", "BLOB"},
    {"DATE", "DATE"},
    {"DOUBLE", "DOUBLE"},
    {"FLOAT8", "DOUBLE"},
    {"NUMERIC", "DOUBLE"},
    {"DECIMAL", "DOUBLE"},
    {"HUGEINT", "HUGEINT"},
    {"INTEGER", "INTEGER"},
    {"INT", "INTEGER"},
    {"INT4", "INTEGER"},
    {"SIGNED", "INTEGER"},
    {"INTERVAL", "INTERVAL"},
    {"REAL", "REAL"},
    {"FLOAT4", "REAL"},
    {"FLOAT", "REAL"},
    {"SMALLINT", "SMALLINT"},
    {"INT2", "SMALLINT"},
    {"SHORT", "SMALLINT"},
    {"TIME", "TIME"},
    {"TIMESTAMP", "TIMESTAMP"},
    {"DATETIME", "TIMESTAMP"},
    {"TINYINT", "TINYINT"},
    {"INT1", "TINYINT"},
    {"UBIGINT", "UBIGINT"},
    {"UINTEGER", "UINTEGER"},
    {"USMALLINT", "USMALLINT"},
    {"UTINYINT", "UTINYINT"},
    {"UUID", "UUID"},
    {"VARCHAR", "VARCHAR"},
    {"CHAR", "VARCHAR"},
    {"BPCHAR", "VARCHAR"},
    {"TEXT", "VARCHAR"},
    {"STRING", "VARCHAR"}};
static Map<String, String> duckdb_to_cpp_type = {
    {"BOOLEAN", "bool"},   {"TINYINT", "int8_t"},    {"SMALLINT", "int16_t"},
    {"DATE", "int32_t"},   {"TIME", "int32_t"},      {"INTEGER", "int32_t"},
    {"BIGINT", "int64_t"}, {"TIMESTAMP", "int64_t"}, {"FLOAT", "float"},
    {"DOUBLE", "double"},  {"DECIMAL", "double"},    {"VARCHAR", "string_t"},
    {"CHAR", "string_t"},  {"BLOB", "string_t"}};

class YAMLConfig {
public:
  YAML::Node query;
  YAML::Node function;
  YAML::Node control;
  YAML::Node aggify;
  YAMLConfig();
};
