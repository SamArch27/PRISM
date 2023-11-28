/**
 * @file file.cpp
 * Utility function to manipulate files and compile them.
 */

#include "file.hpp"
#include "utils.hpp"
#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
using namespace std;

static std::string
    filePath(__FILE__); // Get the path of the current source file
static std::filesystem::path path(filePath);
static std::string current_dir = path.parent_path().string();

std::string exec(const char *cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

void create_dir_from_dir(const string &new_dir, const string &template_dir) {
  // remove the old directory in new_dir
  string cmd = "rm -rf " + new_dir;
  cout << exec(cmd.c_str()) << endl;
  cmd = "cp -r " + template_dir + " " + new_dir;
  cout << exec(cmd.c_str()) << endl;
}

void insert_def_and_reg(const string &defs, const string &regs, int udf_count) {
  create_dir_from_dir(current_dir + "/" + UDF_EXTENSION_OUTPUT_DIR,
                      current_dir + "/" + UDF_EXTENSION_TEMPLATE_DIR);
  string template_file =
      current_dir + "/" + UDF_EXTENSION_OUTPUT_DIR + "src/udf1_extension.cpp";
  // change the udf1_extension.cpp file
  std::ifstream t(template_file);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    ERROR("Template file is empty or does not exist: " + template_file);
  }
  string template_str = buffer.str();
  // search for the string "/* ==== Unique identifier to indicate change
  // required: 9340jfsa034 ==== */"
  size_t search_start = 0;
  string change_start_str = "/* ==== Unique identifier to indicate change "
                            "required start: 9340jfsa034 ==== */\n";
  string change_end_str = "/* ==== Unique identifier to indicate change "
                          "required end: 9340jfsa034 ==== */\n";
  size_t change_start, change_end;
  try {
    while ((change_start = template_str.find(change_start_str, search_start)) !=
           string::npos) {
      change_end = template_str.find(change_end_str, change_start);
      cout << "changing: "
           << template_str.substr(change_start, change_end - change_start)
           << endl;
      size_t udf_start = template_str.find("udf", change_start);
      ASSERT(udf_start != string::npos,
             "Expect to find word udf in: " +
                 template_str.substr(change_start, change_end - change_start));
      template_str =
          template_str.replace(udf_start, 4, "udf" + std::to_string(udf_count));
      search_start = change_end;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }

  // search for the string "/* ==== Unique identifier to indicate insertion
  // point start: 04rj39jds934 ==== */"
  string start_str = "/* ==== Unique identifier to indicate insertion point "
                     "start: 04rj39jds934 ==== */\n";
  string end_str = "/* ==== Unique identifier to indicate insertion point end: "
                   "04rj39jds934 ==== */\n";
  size_t start_pos = template_str.find(start_str);
  size_t end_pos = template_str.find(end_str);
  if (start_pos == string::npos || end_pos == string::npos) {
    ERROR("Cannot find the insertion point in the template file: " +
          template_file);
  }
  // insert the definitions
  string new_str = template_str.substr(0, start_pos + start_str.length()) +
                   defs + template_str.substr(end_pos);

  // search for the string "/* ==== Unique identifier to indicate register
  // insertion point start: 04rj39jds934 ==== */"
  start_str = "/* ==== Unique identifier to indicate register insertion point "
              "start: 04rj39jds934 ==== */\n";
  end_str = "/* ==== Unique identifier to indicate register insertion point "
            "end: 04rj39jds934 ==== */\n";
  start_pos = new_str.find(start_str);
  end_pos = new_str.find(end_str);
  if (start_pos == string::npos || end_pos == string::npos) {
    ERROR("Cannot find the insertion point in the template file: " +
          template_file);
  }
  // insert the definitions
  new_str = new_str.substr(0, start_pos + start_str.length()) + regs +
            new_str.substr(end_pos);

  // write the new string to the template file
  std::ofstream out(template_file);
  if (out.fail()) {
    ERROR("Cannot open the template file for writing: " + template_file);
  }
  out << new_str;
  out.close();

  // change the CMakelists.txt file
  template_file =
      current_dir + "/" + UDF_EXTENSION_OUTPUT_DIR + "CMakeLists.txt";
  t = std::ifstream(template_file);
  buffer = std::ostringstream();
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    ERROR("Template file is empty or does not exist: " + template_file);
  }
  template_str = buffer.str();
  template_str = template_str.replace(template_str.find("udf"), 4,
                                      "udf" + std::to_string(udf_count));
  std::ofstream out2(template_file);
  if (out2.fail()) {
    ERROR("Cannot open the template file for writing: " + template_file);
  }
  out2 << template_str;
  out2.close();
}

void compile_udf(int udf_count) {
  string cmd = "cd " + current_dir + "/../" + ";make udfs >/dev/null 2>&1";
  cout << exec(cmd.c_str()) << endl;
}

void compile_udaf(int udf_count) {
  string cmd = "cd " + current_dir + "/../" + ";make udafs";
  cout << cmd << endl;
  cout << exec(cmd.c_str()) << endl;
}

void load_udf(duckdb::Connection &connection, int udf_count) {
  std::string install = "install '" + current_dir +
                        "/../build/udfs/extension/udf1/" + "udf" +
                        std::to_string(udf_count) + ".duckdb_extension'";
  std::string load = "load '" + current_dir + "/../build/udfs/extension/udf1/" +
                     "udf" + std::to_string(udf_count) + ".duckdb_extension'";
  cout << "Running: " << install << endl;
  auto res = connection.Query(install);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
  cout << "Running: " << load << endl;
  res = connection.Query(load);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
}

void load_udaf(duckdb::Connection &connection, int udf_count) {
  std::string install = "install '" + current_dir +
                        "/../build/udfs/extension/udf_agg/" +
                        "udf_agg.duckdb_extension'";
  std::string load = "load '" + current_dir +
                     "/../build/udfs/extension/udf_agg/" +
                     "udf_agg.duckdb_extension'";
  cout << "Running: " << install << endl;
  auto res = connection.Query(install);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
  cout << "Running: " << load << endl;
  res = connection.Query(load);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
}