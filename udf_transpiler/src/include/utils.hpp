#pragma once
#include <cassert>
#include <iostream>
#include <list>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#define FMT_HEADER_ONLY
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <include/fmt/core.h>
#include <regex>
#include <yaml-cpp/yaml.h>
using namespace std;

#define ASSERT(condition, message)                                             \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::cout << "Assertion `" #condition "` failed in " << __FILE__         \
                << " line " << __LINE__ << ": " << message << std::endl;       \
      std::terminate();                                                        \
    }                                                                          \
  } while (false)

#define ERROR(message)                                                         \
  do {                                                                         \
    std::cout << "Error: " << message << " (" << __FILE__ << ":" << __LINE__   \
              << ")" << std::endl;                                             \
    std::terminate();                                                          \
  } while (false)

#define EXCEPTION(message)                                                     \
  do {                                                                         \
    std::cout << "Exception: " << message << " (" << __FILE__ << ":"           \
              << __LINE__ << ")" << std::endl;                                 \
    throw duckdb::ParserException("See the above message.");                   \
  } while (false)

template <class A> using Own = std::unique_ptr<A>;
template <typename A, typename B = A, typename... Args>
Own<A> Make(Args &&... xs) {
  return std::make_unique<B>(std::forward<Args>(xs)...);
}
template <typename A> using List = std::list<A>;

template <typename A> using Vec = std::vector<A>;

template <typename A> using VecOwn = std::vector<Own<A>>;

template <typename A> using ListOwn = std::list<Own<A>>;

template <typename A> using Opt = std::optional<A>;

template <typename A, typename B> using Map = std::unordered_map<A, B>;

template <typename T>
std::string vec_join(const std::vector<T> &vec, std::string sep) {
  ERROR("vec_join not implemented for this type");
}

template <>
std::string vec_join(const std::vector<std::string> &vec, std::string sep);

template <typename T>
std::string list_join(std::list<T> &any_list, std::string sep) {
  ERROR("list_join not implemented for this type");
}

template <>
std::string list_join(std::list<std::string> &any_list, std::string sep);

std::string toLower(const std::string &str);
std::string toUpper(const std::string &str);
std::string removeSpaces(const std::string &str);

std::vector<std::string> extractMatches(const std::string &str,
                                        const char *pattern,
                                        std::size_t group = 1);

static unordered_map<string, string> alias_to_duckdb_type = {
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
static unordered_map<string, string> duckdb_to_cpp_type = {
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
