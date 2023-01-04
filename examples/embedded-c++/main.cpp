#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB duckDb("/Users/a/github/duckdb/data.db");

	Connection connection(duckDb);

	connection.Query("CREATE TABLE integers(i INTEGER)");
	connection.Query("INSERT INTO integers VALUES (3)");
	auto result = connection.Query("SELECT * FROM integers");
	result->Print();
}
