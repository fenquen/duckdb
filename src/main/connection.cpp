#include "duckdb/main/connection.hpp"

#include "duckdb/execution/operator/persistent/parallel_csv_reader.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

    Connection::Connection(DatabaseInstance &databaseInstance) : clientContext(make_shared<ClientContext>(databaseInstance.shared_from_this())) {
        ConnectionManager::Get(databaseInstance).AddConnection(*clientContext);

#ifdef DEBUG
        EnableProfiling();
        clientContext->config.emit_profiler_output = false;
#endif

    }

    Connection::Connection(DuckDB &database) : Connection(*database.databaseInstance) {
    }

    Connection::~Connection() {
        ConnectionManager::Get(*clientContext->databaseInstance).RemoveConnection(*clientContext);
    }

    string Connection::GetProfilingInformation(ProfilerPrintFormat format) {
        auto &profiler = QueryProfiler::Get(*clientContext);
        if (format == ProfilerPrintFormat::JSON) {
            return profiler.ToJSON();
        } else {
            return profiler.QueryTreeToString();
        }
    }

    void Connection::Interrupt() {
        clientContext->Interrupt();
    }

    void Connection::EnableProfiling() {
        clientContext->EnableProfiling();
    }

    void Connection::DisableProfiling() {
        clientContext->DisableProfiling();
    }

    void Connection::EnableQueryVerification() {
        ClientConfig::GetConfig(*clientContext).query_verification_enabled = true;
    }

    void Connection::DisableQueryVerification() {
        ClientConfig::GetConfig(*clientContext).query_verification_enabled = false;
    }

    void Connection::ForceParallelism() {
        ClientConfig::GetConfig(*clientContext).verify_parallelism = true;
    }

    unique_ptr<QueryResult> Connection::SendQuery(const string &query) {
        return clientContext->Query(query, true);
    }

    unique_ptr<MaterializedQueryResult> Connection::Query(const string &querySql) {
        auto result = clientContext->Query(querySql, false);
        D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
        return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
    }

    unique_ptr<MaterializedQueryResult> Connection::Query(unique_ptr<SQLStatement> statement) {
        auto result = clientContext->Query(move(statement), false);
        D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
        return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
    }

    unique_ptr<PendingQueryResult> Connection::PendingQuery(const string &query, bool allow_stream_result) {
        return clientContext->PendingQuery(query, allow_stream_result);
    }

    unique_ptr<PendingQueryResult>
    Connection::PendingQuery(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
        return clientContext->PendingQuery(move(statement), allow_stream_result);
    }

    unique_ptr<PreparedStatement> Connection::Prepare(const string &query) {
        return clientContext->Prepare(query);
    }

    unique_ptr<PreparedStatement> Connection::Prepare(unique_ptr<SQLStatement> statement) {
        return clientContext->Prepare(move(statement));
    }

    unique_ptr<QueryResult> Connection::QueryParamsRecursive(const string &query, vector<Value> &values) {
        auto statement = Prepare(query);
        if (statement->HasError()) {
            return make_unique<MaterializedQueryResult>(statement->error);
        }
        return statement->Execute(values, false);
    }

    unique_ptr<TableDescription> Connection::TableInfo(const string &table_name) {
        return TableInfo(DEFAULT_SCHEMA, table_name);
    }

    unique_ptr<TableDescription> Connection::TableInfo(const string &schema_name, const string &table_name) {
        return clientContext->TableInfo(schema_name, table_name);
    }

    vector<unique_ptr<SQLStatement>> Connection::ExtractStatements(const string &query) {
        return clientContext->ParseStatements(query);
    }

    unique_ptr<LogicalOperator> Connection::ExtractPlan(const string &query) {
        return clientContext->ExtractPlan(query);
    }

    void Connection::Append(TableDescription &description, DataChunk &chunk) {
        if (chunk.size() == 0) {
            return;
        }
        ColumnDataCollection collection(Allocator::Get(*clientContext), chunk.GetTypes());
        collection.Append(chunk);
        Append(description, collection);
    }

    void Connection::Append(TableDescription &description, ColumnDataCollection &collection) {
        clientContext->Append(description, collection);
    }

    shared_ptr<Relation> Connection::Table(const string &table_name) {
        return Table(DEFAULT_SCHEMA, table_name);
    }

    shared_ptr<Relation> Connection::Table(const string &schema_name, const string &table_name) {
        auto table_info = TableInfo(schema_name, table_name);
        if (!table_info) {
            throw Exception("Table does not exist!");
        }
        return make_shared<TableRelation>(clientContext, move(table_info));
    }

    shared_ptr<Relation> Connection::View(const string &tname) {
        return View(DEFAULT_SCHEMA, tname);
    }

    shared_ptr<Relation> Connection::View(const string &schema_name, const string &table_name) {
        return make_shared<ViewRelation>(clientContext, schema_name, table_name);
    }

    shared_ptr<Relation> Connection::TableFunction(const string &fname) {
        vector<Value> values;
        named_parameter_map_t named_parameters;
        return TableFunction(fname, values, named_parameters);
    }

    shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values,
                                                   const named_parameter_map_t &named_parameters) {
        return make_shared<TableFunctionRelation>(clientContext, fname, values, named_parameters);
    }

    shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values) {
        return make_shared<TableFunctionRelation>(clientContext, fname, values);
    }

    shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values) {
        vector<string> column_names;
        return Values(values, column_names);
    }

    shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values, const vector<string> &column_names,
                                            const string &alias) {
        return make_shared<ValueRelation>(clientContext, values, column_names, alias);
    }

    shared_ptr<Relation> Connection::Values(const string &values) {
        vector<string> column_names;
        return Values(values, column_names);
    }

    shared_ptr<Relation>
    Connection::Values(const string &values, const vector<string> &column_names, const string &alias) {
        return make_shared<ValueRelation>(clientContext, values, column_names, alias);
    }

    shared_ptr<Relation> Connection::ReadCSV(const string &csv_file) {
        BufferedCSVReaderOptions options;
        options.file_path = csv_file;
        options.auto_detect = true;
        BufferedCSVReader reader(*clientContext, options);
        vector<ColumnDefinition> column_list;
        for (idx_t i = 0; i < reader.sql_types.size(); i++) {
            column_list.emplace_back(reader.col_names[i], reader.sql_types[i]);
        }
        return make_shared<ReadCSVRelation>(clientContext, csv_file, move(column_list), true);
    }

    shared_ptr<Relation> Connection::ReadCSV(const string &csv_file, const vector<string> &columns) {
        // parse columns
        vector<ColumnDefinition> column_list;
        for (auto &column: columns) {
            auto col_list = Parser::ParseColumnList(column, clientContext->GetParserOptions());
            if (col_list.LogicalColumnCount() != 1) {
                throw ParserException("Expected a single column definition");
            }
            column_list.push_back(move(col_list.GetColumnMutable(LogicalIndex(0))));
        }
        return make_shared<ReadCSVRelation>(clientContext, csv_file, move(column_list));
    }

    unordered_set<string> Connection::GetTableNames(const string &query) {
        return clientContext->GetTableNames(query);
    }

    shared_ptr<Relation> Connection::RelationFromQuery(const string &query, const string &alias, const string &error) {
        return RelationFromQuery(QueryRelation::ParseStatement(*clientContext, query, error), alias);
    }

    shared_ptr<Relation> Connection::RelationFromQuery(unique_ptr<SelectStatement> select_stmt, const string &alias) {
        return make_shared<QueryRelation>(clientContext, move(select_stmt), alias);
    }

    void Connection::BeginTransaction() {
        auto result = Query("BEGIN TRANSACTION"); // 该sql对应了1个PGNode 是PGRawStmt 内部的话又有1个PGNode 是PGTransactionStmt
        if (result->HasError()) {
            result->ThrowError();
        }
    }

    void Connection::Commit() {
        auto result = Query("COMMIT");
        if (result->HasError()) {
            result->ThrowError();
        }
    }

    void Connection::Rollback() {
        auto result = Query("ROLLBACK");
        if (result->HasError()) {
            result->ThrowError();
        }
    }

    void Connection::SetAutoCommit(bool auto_commit) {
        clientContext->transactionContext.SetAutoCommit(auto_commit);
    }

    bool Connection::IsAutoCommit() {
        return clientContext->transactionContext.IsAutoCommit();
    }

    bool Connection::HasActiveTransaction() {
        return clientContext->transactionContext.HasActiveTransaction();
    }

} // namespace duckdb
