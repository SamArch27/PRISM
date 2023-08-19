#pragma once
#include <string>
#include <vector>
#include <iostream>

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::exit(EXIT_FAILURE); \
        } \
    } while (false)

template <typename T>
std::string vec_join(std::vector<T> &vec, std::string sep){
    std::string result = "";
    for(auto &item : vec){
        result += std::to_string(item) + sep;
    }
    return result.substr(0, result.size() - sep.size());
}

std::string vec_join(std::vector<std::string> &vec, std::string sep);
