#pragma once

#include "duckdb.hpp"

namespace duckdb {

class Udf1Extension : public Extension {
public:
	// duckdb::DuckDB* db = NULL;
	// std::unique_ptr<Connection> con;
	// ClientContext *context = NULL;
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
