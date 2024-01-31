#include "utils.hpp"
#include <filesystem>
#include <yaml-cpp/yaml.h>

template <>
String vector_join(const Vec<String> &vec, String sep) {
  String result = "";
  for (auto &item : vec) {
    result += item + sep;
  }
  return result.substr(0, result.size() - sep.size());
}

template <>
String list_join(std::list<String> &any_list, String sep) {
  String result = "";
  for (auto &item : any_list) {
    result += item + sep;
  }
  return result.substr(0, result.size() - sep.size());
}

String toLower(const String &str) {
  String lower = str;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  return lower;
}

String toUpper(const String &str) {
  String upper = str;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  return upper;
}

String removeSpaces(const String &str) {
  std::regex spaceRegex("\\s+");
  return std::regex_replace(str, spaceRegex, "");
}

Vec<String> extractMatches(const String &text,
                                        const char *pattern,
                                        std::size_t group) {
  Vec<String> res;
  auto regex = std::regex(pattern, std::regex_constants::icase);
  std::smatch matched;
  String str = text;
  while (std::regex_search(str, matched, regex)) {
    res.push_back(matched[group]);
    str = matched.suffix();
  }
  return res;
}

YAMLConfig::YAMLConfig() {
  String filePath(__FILE__); // Get the path of the current source file
  std::filesystem::path path(filePath);
  String current_dir = path.parent_path().string();
  query = YAML::LoadFile(current_dir + "/../templates/query.yaml");
  function = YAML::LoadFile(current_dir + "/../templates/function.yaml");
  control = YAML::LoadFile(current_dir + "/../templates/control.yaml");
  aggify = YAML::LoadFile(current_dir + "/../templates/aggify.yaml");
}
