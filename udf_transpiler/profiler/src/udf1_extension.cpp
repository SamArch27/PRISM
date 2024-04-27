#define DUCKDB_EXTENSION_MAIN

#include "udf1_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <chrono>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fstream>
#include <iostream>

#define PREPARATION_FILE "preparation.sql"
#define WORKLOAD_FILE "workload.sql"
#define REPEAT_TIMES 5
#define WARMUP_TIMES 2
#define NAME_SHOW_LEN 20

namespace duckdb {

std::unique_ptr<Connection> con;
ClientContext *context = NULL;

string short_name(const string &name_arg) {
  string ret;
  string name = name_arg;
  if (name[0] == '\n') {
    name = name.substr(1);
  }
  if (name.size() > NAME_SHOW_LEN) {
    ret = name.substr(0, NAME_SHOW_LEN / 2) + "..." +
          name.substr(name.size() - NAME_SHOW_LEN / 2);
    // replace ' with `
    for (int i = 0; i < ret.size(); i++) {
      if (ret[i] == '\'') {
        ret[i] = '`';
      }
    }
    return ret;
  }
  ret = name;
  // replace ' with `
  for (int i = 0; i < ret.size(); i++) {
    if (ret[i] == '\'') {
      ret[i] = '`';
    }
  }
  return ret;
}

string reformat_sql_string(const string &str) {
  string ret;
  ret = str;
  while (ret[0] == '\n') {
    ret = ret.substr(1);
  }
  for (int i = 0; i < ret.size(); i++) {
    if (ret[i] == '\'') {
      ret[i] = '`';
    }
  }
  return ret;
}

string repeat_run(const string &sql, int warmup_times, int times) {
  for (int i = 0; i < warmup_times; i++) {
    std::cout << "warmup " << i << std::endl;
    auto result = con->Query(sql);
    if (result->HasError()) {
      std::string err = result->GetError();
      std::cout << sql << std::endl;
      std::cout << err << std::endl;
      return err;
    }
  }
  std::chrono::time_point<std::chrono::steady_clock> start;
  std::chrono::time_point<std::chrono::steady_clock> end;
  // double sum = 0; // in ms
  std::vector<double> times_list;
  for (int i = 0; i < times; i++) {
    std::cout << "run " << i << std::endl;
    start = std::chrono::steady_clock::now();
    auto result = con->Query(sql);
    end = std::chrono::steady_clock::now();
    if (result->HasError()) {
      std::string err = result->GetError();
      std::cout << sql << std::endl;
      std::cout << err << std::endl;
      return err;
    }
    // sum += std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
    //            .count();
    times_list.push_back(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count());
  }
  // return std::to_string(sum / times);
  string res;
  for (int i = 0; i < times_list.size(); i++) {
    res += std::to_string(times_list[i]);
    if (i != times_list.size() - 1) {
      res += " ";
    }
  }
  std::cout << res << std::endl;
  return res;
}

inline string ProfileMain(string test_dir, string preparation_file,
                          string workload_file) {
  if (test_dir.back() != '/') {
    test_dir = test_dir + "/";
  }
  // do the preparation work
  std::cout << "Preparing" << std::endl;

  auto file_name = test_dir + preparation_file;
  std::ifstream t(file_name);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    std::string err = "Input file is empty or does not exist: " + file_name;
    return "select '" + err + "' as 'Preparation Failed.';";
  }
  std::string sql = buffer.str();
  std::cout << short_name(sql) << std::endl;
  auto result = con->Query(sql);
  if (result->HasError()) {
    std::string err = result->GetError();
    return "select '" + err + "' as 'Preparation Failed.';";
  }
  std::cout << "Preparation Done" << std::endl;

  // do the workload
  file_name = test_dir + workload_file;
  std::ifstream t2(file_name);
  std::ostringstream buffer2;
  buffer2 << t2.rdbuf();
  if (buffer2.str().empty()) {
    std::string err = "Input file is empty or does not exist: " + file_name;
    // replace ' with `
    return "select '" + reformat_sql_string(err) + "' as 'Workload Failed.';";
  }
  std::cout << "Doing workloads" << std::endl;
  string running_result_table;
  // split the workload into multiple queries by ';'
  std::string workload = buffer2.str();
  int count = 1;
  while (!workload.empty()) {
    auto pos = workload.find(';');
    if (pos == std::string::npos) {
      break;
    }
    std::string sql = workload.substr(0, pos + 1);
    workload = workload.substr(pos + 1);
    std::cout << short_name(sql) << std::endl;
    auto result = repeat_run(sql, WARMUP_TIMES, REPEAT_TIMES);
    result = reformat_sql_string(result);
    sql = reformat_sql_string(sql);
    running_result_table +=
        "('" + std::to_string(count) + "','" + sql + "','" + result + "'),";
    count++;
  }
  running_result_table =
      running_result_table.substr(0, running_result_table.size() - 1);
  std::cout << "Workloads Done" << std::endl;
  // std::cout<<running_result_table<<std::endl;
  return "select col0 as id, col1 as description, col2 as "
         "\"avg_time(ms)/error\" from (values" +
         running_result_table + ");";
}

inline string ProfileCustomPragmaFun(ClientContext &context,
                                     const FunctionParameters &parameters) {
  std::string test_dir = parameters.values[0].GetValue<string>();
  std::string preparation_file = parameters.values[1].GetValue<string>();
  std::string workload_file = parameters.values[2].GetValue<string>();
  return ProfileMain(test_dir, preparation_file, workload_file);
}

inline string ProfilePragmaFun(ClientContext &context,
                               const FunctionParameters &parameters) {
  std::string test_dir = parameters.values[0].GetValue<string>();
  return ProfileMain(test_dir, PREPARATION_FILE, WORKLOAD_FILE);
}

/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */

static void LoadInternal(DatabaseInstance &instance) {
  con = make_uniq<Connection>(instance);
  context = con->context.get();

  auto profile_pragma_function = PragmaFunction::PragmaCall(
      "profile", ProfilePragmaFun, {LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, profile_pragma_function);
  auto profile_custom_pragma_function = PragmaFunction::PragmaCall(
      "profile_custom", ProfileCustomPragmaFun,
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
  ExtensionUtil::RegisterFunction(instance, profile_custom_pragma_function);
}

void Udf1Extension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string Udf1Extension::Name() { return "profiler"; }

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void profiler_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}
DUCKDB_EXTENSION_API const char *profiler_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
