#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeVar(duckdb_libpgquery::PGRangeVar *pgRangeVar) {
	auto result = make_unique<BaseTableRef>();

	result->alias = TransformAlias(pgRangeVar->alias, result->column_name_alias);
	if (pgRangeVar->relname) {
		result->table_name = pgRangeVar->relname;
	}
	if (pgRangeVar->schemaname) {
		result->schema_name = pgRangeVar->schemaname;
	}
	if (pgRangeVar->sample) {
		result->sample = TransformSampleOptions(pgRangeVar->sample);
	}
	result->query_location = pgRangeVar->location;
	return move(result);
}

QualifiedName Transformer::TransformQualifiedName(duckdb_libpgquery::PGRangeVar *root) {
	QualifiedName qname;
	if (root->relname) {
		qname.name = root->relname;
	} else {
		qname.name = string();
	}
	if (root->schemaname) {
		qname.schema = root->schemaname;
	} else {
		qname.schema = INVALID_SCHEMA;
	}
	return qname;
}

} // namespace duckdb
