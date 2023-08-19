#include "utils.hpp"

std::string vec_join(std::vector<std::string> &vec, std::string sep){
    std::string result = "";
    for(auto &item : vec){
        result += item + sep;
    }
    return result.substr(0, result.size() - sep.size());
}