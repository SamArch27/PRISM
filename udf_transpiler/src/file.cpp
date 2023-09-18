/**
 * @file file.cpp
 * Utility function to manipulate files and compile them.
*/

#include "file.hpp"
#include "utils.hpp"
#include <sstream>
#include <iostream>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
using namespace std;

std::string filePath(__FILE__); // Get the path of the current source file
std::filesystem::path path(filePath);
std::string current_dir = path.parent_path().string();

void insert_def_and_reg(const string & defs, const string & regs){
    string template_file = current_dir + "/../udf_template/src/udf1_extension.cpp";
    // read the template file
    std::ifstream t(template_file);
    std::ostringstream buffer;
    buffer << t.rdbuf();
    if(buffer.str().empty()){
        ERROR("Template file is empty or does not exist: " + template_file);
    }
    string template_str = buffer.str();
    // search for the string "/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */"
    string start_str = "/* ==== Unique identifier to indicate insertion point start: 04rj39jds934 ==== */\n";
    string end_str = "/* ==== Unique identifier to indicate insertion point end: 04rj39jds934 ==== */\n";
    size_t start_pos = template_str.find(start_str);
    size_t end_pos = template_str.find(end_str);
    if(start_pos == string::npos || end_pos == string::npos){
        ERROR("Cannot find the insertion point in the template file: " + template_file);
    }
    // insert the definitions
    string new_str = template_str.substr(0, start_pos + start_str.length()) + defs + template_str.substr(end_pos);

    // search for the string "/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */"
    start_str = "/* ==== Unique identifier to indicate register insertion point start: 04rj39jds934 ==== */\n";
    end_str = "/* ==== Unique identifier to indicate register insertion point end: 04rj39jds934 ==== */\n";
    start_pos = new_str.find(start_str);
    end_pos = new_str.find(end_str);
    if(start_pos == string::npos || end_pos == string::npos){
        ERROR("Cannot find the insertion point in the template file: " + template_file);
    }
    // insert the definitions
    new_str = new_str.substr(0, start_pos + start_str.length()) + regs + new_str.substr(end_pos);

    // write the new string to the template file
    std::ofstream out(template_file);
    if(out.fail()){
        ERROR("Cannot open the template file for writing: " + template_file);
    }
    out << new_str;
    out.close();
}

std::string exec(const char* cmd) {
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

void compile_udf(){
    string cmd = "cd " + current_dir + "/../" + ";make udf1";
    cout<<exec(cmd.c_str())<<endl;
}

void load_udf(duckdb::Connection &connection){
    std::string install = "install '" + current_dir + "/../build/udfs/extension/udf1/udf1.duckdb_extension'";
    std::string load = "load '" + current_dir + "/../build/udfs/extension/udf1/udf1.duckdb_extension'";
    auto res = connection.Query(install);
    if(res->HasError()){
        EXCEPTION(res->GetError());
    }
    res = connection.Query(load);
    if(res->HasError()){
        EXCEPTION(res->GetError());
    }
}