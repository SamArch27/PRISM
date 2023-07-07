#include "utils.hpp"

string vec_join(vector<string> &vec, const string &del){
    return vec.empty() ? "" : /* leave early if there are no items in the list */
            std::accumulate( /* otherwise, accumulate */
                ++vec.begin(), vec.end(), /* the range 2nd to after-last */
                *vec.begin(), /* and start accumulating with the first item */
                [&](auto &&a, auto &&b) -> auto& { a += del; a += b; return a; });
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
string UDF_Type::resolve_type(string &type_name, const string &udf_str){
    // std::cout<<type_name<<std::endl;
    // std::cout<<std::stoi("#28#")<<std::endl;
    if (type_name.starts_with('#'))
    {
        if (udf_str.size() == 0)
        {
            throw std::runtime_error("UDF string is empty");
        }
        int type_start = type_name.find('#');
        // int type_end = type_name.find('#', type_start);
        type_start = std::stoi(type_name.substr(type_start + 1));
        int type_end = type_start;
        while (isalpha(udf_str[type_end]))
        {
            type_end++;
        }
        string real_type_name = udf_str.substr(type_start, type_end - type_start);
        return resolve_type(real_type_name, udf_str);
    }
    remove_spaces(type_name);
    std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::toupper);
    if (type_name.starts_with("DECIMAL"))
    {
        std::regex type_pattern("^DECIMAL ?(\\(\\d+,\\d+\\))", std::regex_constants::icase);
        std::smatch tmp_types;
        std::regex_search(type_name, tmp_types, type_pattern);
        if (tmp_types.size() > 0)
        {
            return type_name;
        }
    }
    if (alias_to_duckdb_type.contains(type_name))
    {
        return alias_to_duckdb_type.at(type_name);
    }
    else
    {
        throw runtime_error(fmt::format("Unknown type: {}", type_name));
    }
}

void UDF_Type::get_decimal_width_scale(string &duckdb_type, int &width, int &scale){
    std::regex decimal_pattern("DECIMAL\\((\\d+),(\\d+)\\)", std::regex_constants::icase);
    std::smatch decimal_match;
    std::regex_search(duckdb_type, decimal_match, decimal_pattern);
    ASSERT(decimal_match.size() == 3, "Decimal type is not of the correct format.");    
    width = std::stoi(decimal_match[1]);
    scale = std::stoi(decimal_match[2]);
    // std::cout << width << scale << '\n';
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
        throw std::runtime_error("Width larger than 38.");
}

string UDF_Type::get_cpp_type()
{
    ASSERT(is_unknown() == false, "Cannot get cpp type from UNKNOWN duckdb type.");
    // std::cout << duckdb_type << "_____" << '\n';
    if(duckdb_type.starts_with("DECIMAL(")){
        int width, scale;
        UDF_Type::get_decimal_width_scale(duckdb_type, width, scale);
        return UDF_Type::get_decimal_int_type(width, scale);
    }
    return duckdb_to_cpp_type.at(duckdb_type);
}