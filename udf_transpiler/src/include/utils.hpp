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
#include <algorithm>
#include <include/fmt/core.h>
#include <regex>
#include <yaml-cpp/yaml.h>
#include "duckdb/common/exception.hpp"
using namespace std;

#define ASSERT(condition, message)                                             \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::cout << "Assertion `" #condition "` failed in " << __FILE__         \
                << " line " << __LINE__ << ": " << message << std::endl;       \
      throw exception();                                                       \
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
    throw duckdb::ParserException("See the above message.");                 \                                       
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
std::string vec_join(std::vector<T> &vec, std::string sep) {
  ERROR("vec_join not implemented for this type");
}

template <>
std::string vec_join(std::vector<std::string> &vec, std::string sep);

template <typename T>
std::string list_join(std::list<T> &any_list, std::string sep) {
  ERROR("list_join not implemented for this type");
}

template <>
std::string list_join(std::list<std::string> &any_list, std::string sep);

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

class UDFType {
private:
  static constexpr int DEFAULT_WIDTH = 18;
  static constexpr int DEFAULT_SCALE = 3;
  // used for decimal
  int width;
  int scale;
  string duckdb_type;
  static void get_decimal_width_scale(string &duckdb_type, int &width,
                                      int &scale);
  static string get_decimal_int_type(int width, int scale);
  void resolve_type(string type_name, const string &udf_str);

public:
  UDFType(){};
  UDFType(const string &type_name) { resolve_type(type_name, ""); }
  UDFType(const string &type_name, const string &udf_str) {
    resolve_type(type_name, udf_str);
  }
  const string get_duckdb_type() const;
  const string get_duckdb_logical_type() const;
  const string get_cpp_type() const;
  string create_duckdb_value(const string &ret_name, const string &cpp_value);
  bool is_unknown() const {
    return strcmp(duckdb_type.c_str(), "UNKNOWN") == 0;
  }
};

class VarInfo {
public:
  int id;
  bool init;
  UDFType type;
  VarInfo(){};
  VarInfo(int id, string &type_name, const string &udf_str, bool i)
      : id(id), init(i), type(type_name, udf_str){};
};

class CodeContainer {
public:
  bool query_macro = false;
  vector<string> global_macros;
  vector<string> global_variables;
  vector<string> global_functions;
};

class FunctionInfo {
public:
  int function_count = 0;
  int vector_size = 2048;
  string func_name;
  UDFType func_return_type;
  int tmp_var_count = 0;
  vector<string> func_args_vec;
  unordered_map<string, VarInfo> func_args;
  unordered_map<string, VarInfo> func_vars;
  /**
   * key of this overwrite key in func_vars
   */
  unordered_map<string, string> tmp_var_substitutes;

  /**
   * string functions needs special treatment when invoking
   * preparation should happen in the caller
   */
  int string_function_count = 0;

  string new_variable() {
    tmp_var_count += 1;
    return "tmpvar" + std::to_string(tmp_var_count);
  }

  bool if_exists(const string &var_name) {
    return func_vars.find(var_name) != func_vars.end() ||
           func_args.find(var_name) != func_args.end();
  }

  VarInfo &get_var_info(const string &var_name) {
    if (func_vars.find(var_name) != func_vars.end()) {
      if (tmp_var_substitutes.count(var_name)) {
        return func_vars[tmp_var_substitutes[var_name]];
      }
      return func_vars[var_name];
    } else if (func_args.find(var_name) != func_args.end()) {
      return func_args[var_name];
    } else {
      ERROR(fmt::format("Variable {} not found.", var_name));
    }
  }

  vector<pair<const string &, const VarInfo &>> get_vars() {
    vector<pair<const string &, const VarInfo &>> ret;
    for (auto &var : func_vars) {
      if (tmp_var_substitutes.count(var.first))
        continue;
      // cout<<var.first<<" "<<var.second.type.duckdb_type<<endl;
      ret.push_back({var.first, var.second});
    }
    for (auto &var : func_args) {
      // cout<<var.first<<" "<<var.second.type.duckdb_type<<endl;
      ret.push_back({var.first, var.second});
    }
    return ret;
  }
};

class YAMLConfig {
public:
  YAML::Node query;
  YAML::Node function;
  YAML::Node control;
  YAMLConfig();
};

bool is_const_or_var(string &expr, FunctionInfo &funtion_info, string &res);

std::string get_var_name(const std::string &var_name);
