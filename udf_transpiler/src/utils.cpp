#include "utils.hpp"
#include <yaml-cpp/yaml.h>

template <>
std::string vec_join(std::vector<std::string> &vec, std::string sep){
    std::string result = "";
    for(auto &item : vec){
        result += item + sep;
    }
    return result.substr(0, result.size() - sep.size());
}

template <>
std::string list_join(std::list<std::string> &any_list, std::string sep){
    std::string result = "";
    for(auto &item : any_list){
        result += item + sep;
    }
    return result.substr(0, result.size() - sep.size());
}

void remove_spaces(std::string& str) {
    str.erase(remove(str.begin(), str.end(), ' '), str.end());
}

// todo
bool is_number(){
    return false;
}

// /**
//  * @brief check if an expression is a constant such as 2 or 'string' or variable
// */
// bool is_const_or_var(string &expr, FunctionInfo &funtion_info, UDF_Type &expected_type, string &result){
//     if((expr.starts_with("\"") and expr.ends_with("\"")) or (expr.starts_with("'") and expr.ends_with("'"))){
//         ASSERT("")
//         result = expr;
//         return true;
//     }
//     else if(){

//     }
// }

/**
 * @brief Resolve a type name to a C++ type name and a type size.
 */
void UDF_Type::resolve_type(string type_name, const string &udf_str){
    // std::cout<<type_name<<std::endl;
    // std::cout<<std::stoi("#28#")<<std::endl;
    if (type_name.starts_with('#'))
    {
        if (udf_str.size() == 0)
        {
            ERROR("UDF string is empty");
        }
        int type_start = type_name.find('#');
        type_start = std::stoi(type_name.substr(type_start + 1));
        // map varchar() as varchar
        std::regex type_pattern("(\\w+(\\(\\d+, ?\\d+\\))?)", std::regex_constants::icase);
        std::smatch tmp_types;
        std::regex_search(udf_str.begin() + type_start, udf_str.end(), tmp_types, type_pattern);
        string real_type_name = tmp_types[0];
        return resolve_type(real_type_name, udf_str);
    }
    remove_spaces(type_name);
    std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::toupper);
    if (type_name.starts_with("DECIMAL"))
    {
        int width, scale;
        get_decimal_width_scale(type_name, width, scale);
        duckdb_type = type_name;
        this->width = width;
        this->scale = scale;
        return;
    }
    else if(type_name.starts_with("VARCHAR")){
        return resolve_type("VARCHAR", udf_str);
    }
    if (alias_to_duckdb_type.count(type_name))
    {
        duckdb_type = alias_to_duckdb_type.at(type_name);
    }
    else
    {
        ERROR(fmt::format("Unknown type: {}", type_name));
    }
}

void UDF_Type::get_decimal_width_scale(string &duckdb_type, int &width, int &scale){
    std::regex decimal_pattern("DECIMAL\\((\\d+),(\\d+)\\)", std::regex_constants::icase);
    std::smatch decimal_match;
    std::regex_search(duckdb_type, decimal_match, decimal_pattern);
    if(decimal_match.size() == 3){
        width = std::stoi(decimal_match[1]);
        scale = std::stoi(decimal_match[2]);
    }
    else{
        width = DEFAULT_WIDTH;
        scale = DEFAULT_SCALE;
    }
    return;
}

string UDF_Type::get_decimal_int_type(int width, int scale){
    ASSERT(width > 0, "Width of decimal should be > 0.");
    if(width <= 4)
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

const string UDF_Type::get_cpp_type() const
{
    ASSERT(is_unknown() == false, "Cannot get cpp type from UNKNOWN duckdb type.");
    if(duckdb_type.starts_with("DECIMAL")){
        return UDF_Type::get_decimal_int_type(width, scale);
    }
    return duckdb_to_cpp_type.at(duckdb_type);
}

const string UDF_Type::get_duckdb_type() const
{
    if(duckdb_type.starts_with("DECIMAL")){
        return fmt::format("DECIMAL({}, {})", width, scale);
    }
    return duckdb_type;
}

const string UDF_Type::get_duckdb_logical_type() const
{
    return "LogicalType::" + get_duckdb_type();
}

string UDF_Type::create_duckdb_value(const string &ret_name, const string &cpp_value){
    const vector<string> numeric = {"BOOLEAN", "TINYINT", "SMALLINT", "DATE", "TIME", "INTEGER", "BIGINT", "TIMESTAMP", "FLOAT", "DOUBLE", "DECIMAL"};
    const vector<string> blob = {"VARCHAR", "BLOB"};
    if(duckdb_type.starts_with("DECIMAL")){
        return fmt::format("{} = Value::DECIMAL({}, {}, {})", ret_name, cpp_value, width, scale);
    }
    else if(std::find(numeric.begin(), numeric.end(), duckdb_type) != numeric.end()){
        return fmt::format("{} = Value::{}({})", ret_name, duckdb_type, cpp_value);
    }
    else if(std::find(blob.begin(), blob.end(), duckdb_type) != blob.end()){
        return fmt::format("{0} = Value({1});\n{0}.GetTypeMutable() = LogicalType::{2}", ret_name, cpp_value, duckdb_type);
    }
    else{
        ERROR(fmt::format("Cannot create duckdb value from type {}", duckdb_type));
    }
}

YAMLConfig::YAMLConfig(){
    query = YAML::LoadFile("templates/query.yaml");
    function = YAML::LoadFile("templates/function.yaml");
    control = YAML::LoadFile("templates/control.yaml");
}

/**
 * lowerize variable names
*/
std::string get_var_name(const std::string &var_name){
    std::string result = var_name;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}
