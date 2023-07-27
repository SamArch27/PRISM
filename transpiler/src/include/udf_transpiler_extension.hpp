#pragma once

#include "duckdb.hpp"

namespace duckdb {

class Udf_transpilerExtension : public Extension {
private:
	duckdb::DuckDB& db;
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
