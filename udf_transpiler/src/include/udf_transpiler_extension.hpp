#pragma once

#include "duckdb.hpp"

namespace duckdb {

class UdfTranspilerExtension : public Extension {
private:
  duckdb::DuckDB *db = NULL;

public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckdb
