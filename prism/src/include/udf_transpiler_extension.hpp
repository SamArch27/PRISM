#pragma once

#include "duckdb.hpp"

namespace duckdb {

extern std::string dbPlatform;
extern std::unordered_map<std::string, bool> optimizerPassOnMap;

class UdfTranspilerExtension : public Extension {
private:
  duckdb::DuckDB *db = NULL;

public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckdb
