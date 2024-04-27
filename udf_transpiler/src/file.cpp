/**
 * @file file.cpp
 * Utility function to manipulate files and compile them.
 */

#include "file.hpp"
#include "utils.hpp"
#include <array>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

static String filePath(__FILE__); // Get the path of the current source file
static std::filesystem::path path(filePath);
static String current_dir = path.parent_path().string();

String exec(const char *cmd) {
  std::array<char, 128> buffer;
  String result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

void create_dir_from_dir(const String &new_dir, const String &template_dir) {
  // remove the old directory in new_dir
  String cmd = "rm -rf " + new_dir;
  DEBUG_INFO(exec(cmd.c_str()));
  cmd = "cp -r " + template_dir + " " + new_dir;
  DEBUG_INFO(exec(cmd.c_str()));
}

void insertDefAndReg(const String &defs, const String &regs, int udfCount) {
  create_dir_from_dir(current_dir + "/" + UDF_EXTENSION_OUTPUT_DIR,
                      current_dir + "/" + UDF_EXTENSION_TEMPLATE_DIR);
  String template_file =
      current_dir + "/" + UDF_EXTENSION_OUTPUT_DIR + "src/udf1_extension.cpp";
  // change the udf1_extension.cpp file
  std::ifstream t(template_file);
  std::ostringstream buffer;
  buffer << t.rdbuf();
  if (buffer.str().empty()) {
    ERROR("Template file is empty or does not exist: " + template_file);
  }
  String template_str = buffer.str();
  // search for the String "/* ==== Unique identifier to indicate change
  // required: 9340jfsa034 ==== */"
  size_t search_start = 0;
  String change_start_str = "/* ==== Unique identifier to indicate change "
                            "required start: 9340jfsa034 ==== */\n";
  String change_end_str = "/* ==== Unique identifier to indicate change "
                          "required end: 9340jfsa034 ==== */\n";
  size_t change_start, change_end;
  try {
    while ((change_start = template_str.find(change_start_str, search_start)) !=
           String::npos) {
      change_end = template_str.find(change_end_str, change_start);
      // COUT << "changing: "
      //      << template_str.substr(change_start, change_end - change_start)
      //      << ENDL;
      size_t udf_start = template_str.find("udf", change_start);
      ASSERT(udf_start != String::npos,
             "Expect to find word udf in: " +
                 template_str.substr(change_start, change_end - change_start));
      template_str =
          template_str.replace(udf_start, 4, "udf" + std::to_string(udfCount));
      search_start = change_end;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }

  // search for the String "/* ==== Unique identifier to indicate insertion
  // point start: 04rj39jds934 ==== */"
  String start_str = "/* ==== Unique identifier to indicate insertion point "
                     "start: 04rj39jds934 ==== */\n";
  String end_str = "/* ==== Unique identifier to indicate insertion point end: "
                   "04rj39jds934 ==== */\n";
  size_t start_pos = template_str.find(start_str);
  size_t end_pos = template_str.find(end_str);
  if (start_pos == String::npos || end_pos == String::npos) {
    ERROR("Cannot find the insertion point in the template file: " +
          template_file);
  }
  // insert the definitions
  String new_str = template_str.substr(0, start_pos + start_str.length()) +
                   defs + template_str.substr(end_pos);

  // search for the String "/* ==== Unique identifier to indicate register
  // insertion point start: 04rj39jds934 ==== */"
  start_str = "/* ==== Unique identifier to indicate register insertion point "
              "start: 04rj39jds934 ==== */\n";
  end_str = "/* ==== Unique identifier to indicate register insertion point "
            "end: 04rj39jds934 ==== */\n";
  start_pos = new_str.find(start_str);
  end_pos = new_str.find(end_str);
  if (start_pos == String::npos || end_pos == String::npos) {
    ERROR("Cannot find the insertion point in the template file: " +
          template_file);
  }
  // insert the definitions
  new_str = new_str.substr(0, start_pos + start_str.length()) + regs +
            new_str.substr(end_pos);

  // write the new String to the template file
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
                                      "udf" + std::to_string(udfCount));
  std::ofstream out2(template_file);
  if (out2.fail()) {
    ERROR("Cannot open the template file for writing: " + template_file);
  }
  out2 << template_str;
  out2.close();
}

void compileUDF() {
  // String cmd = "cd " + current_dir + "/../" + ";make udfs >/dev/null 2>&1";
  String cmd = "cd " + current_dir + "/../" + ";make udfs";
  DEBUG_INFO(exec(cmd.c_str()));
}

/**
 * will load the udf*.duckdb_extension file that is last created
 */
void loadUDF(duckdb::Connection &connection) {
  // find the current udfCount
  String cmd = "cd " + current_dir +
               "/../build/udfs/extension/udf1/;ls -t | grep -o "
               "'^udf.*\\.duckdb_extension$'";
  String filename = exec(cmd.c_str());
  filename = filename.substr(0, filename.find("\n"));
  String install = "install '" + current_dir +
                   "/../build/udfs/extension/udf1/" + filename + "'";
  String load = "load '" + current_dir + "/../build/udfs/extension/udf1/" +
                filename + "'";
  auto res = connection.Query(install);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
  res = connection.Query(load);
  if (res->HasError()) {
    EXCEPTION(res->GetError());
  }
}

void drawGraph(const String &dot, String name) {
  // // create a hidden file in GRAPH_OUTPUT_DIR
  // String filename = current_dir + "/" + GRAPH_OUTPUT_DIR + name + ".dot";
  // std::ofstream out(filename);
  // if (out.fail()) {
  //   ERROR("Cannot open the file for writing: " + filename);
  // }
  // out << dot;
  // out.close();

  // // run the dot command
  // String cmd = "dot -Tpdf -O " + filename;
  // DEBUG_INFO(exec(cmd.c_str()));
}