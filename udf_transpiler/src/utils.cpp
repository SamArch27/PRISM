#include "utils.hpp"
#include <filesystem>
#include <yaml-cpp/yaml.h>

template <>
std::string vec_join(const std::vector<std::string> &vec, std::string sep) {
  std::string result = "";
  for (auto &item : vec) {
    result += item + sep;
  }
  return result.substr(0, result.size() - sep.size());
}

template <>
std::string list_join(std::list<std::string> &any_list, std::string sep) {
  std::string result = "";
  for (auto &item : any_list) {
    result += item + sep;
  }
  return result.substr(0, result.size() - sep.size());
}

std::string toLower(const std::string &str) {
  std::string lower = str;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  return lower;
}

std::string toUpper(const std::string &str) {
  std::string upper = str;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  return upper;
}

std::string removeSpaces(const std::string &str) {
  std::regex spaceRegex("\\s+");
  return std::regex_replace(str, spaceRegex, "");
}

std::vector<std::string> extractMatches(const std::string &text,
                                        const char *pattern,
                                        std::size_t group) {
  std::vector<std::string> res;
  auto regex = std::regex(pattern, std::regex_constants::icase);
  std::smatch matched;
  std::string str = text;
  while (std::regex_search(str, matched, regex)) {
    res.push_back(matched[group]);
    str = matched.suffix();
  }
  return res;
}

YAMLConfig::YAMLConfig() {
  std::string filePath(__FILE__); // Get the path of the current source file
  std::filesystem::path path(filePath);
  std::string current_dir = path.parent_path().string();
  query = YAML::LoadFile(current_dir + "/../templates/query.yaml");
  function = YAML::LoadFile(current_dir + "/../templates/function.yaml");
  control = YAML::LoadFile(current_dir + "/../templates/control.yaml");
  aggify = YAML::LoadFile(current_dir + "/../templates/aggify.yaml");
}
