#include "duckdb/main/database.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/replacement_opens.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/error_manager.hpp"

#ifndef DUCKDB_NO_THREADS

#include "duckdb/common/thread.hpp"

#endif

namespace duckdb {

    DBConfig::DBConfig() {
        compression_functions = make_unique<CompressionFunctionSet>();
        replacement_opens.push_back(ExtensionPrefixReplacementOpen());
        cast_functions = make_unique<CastFunctionSet>();
        error_manager = make_unique<ErrorManager>();
    }

    DBConfig::DBConfig(std::unordered_map<string, string> &config_dict, bool read_only) {
        compression_functions = make_unique<CompressionFunctionSet>();
        if (read_only) {
            dbConfigOptions.access_mode = AccessMode::READ_ONLY;
        }
        for (auto &kv: config_dict) {
            string key = kv.first;
            string val = kv.second;
            auto config_property = DBConfig::GetOptionByName(key);
            if (!config_property) {
                throw InvalidInputException("Unrecognized configuration property \"%s\"", key);
            }
            auto opt_val = Value(val);
            DBConfig::SetOption(*config_property, opt_val);
        }
    }

    DBConfig::~DBConfig() {
    }

    DatabaseInstance::DatabaseInstance() {
    }

    DatabaseInstance::~DatabaseInstance() {
        if (Exception::UncaughtException()) {
            return;
        }

        // shutting down: attempt to checkpoint the database
        // but only if we are not cleaning up as part of an exception unwind
        try {
            auto &storage = StorageManager::GetStorageManager(*this);
            if (!storage.InMemory()) {
                auto &config = storage.databaseInstance.dbConfig;
                if (!config.dbConfigOptions.checkpoint_on_shutdown) {
                    return;
                }
                storage.CreateCheckpoint(true);
            }
        } catch (...) {
        }
    }

    BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
        return *db.GetStorageManager().buffer_manager;
    }

    DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
        return *context.databaseInstance;
    }

    StorageManager &StorageManager::GetStorageManager(DatabaseInstance &db) {
        return db.GetStorageManager();
    }

    Catalog &Catalog::GetCatalog(DatabaseInstance &db) {
        return db.GetCatalog();
    }

    FileSystem &FileSystem::GetFileSystem(DatabaseInstance &db) {
        return db.GetFileSystem();
    }

    DBConfig &DBConfig::GetConfig(DatabaseInstance &db) {
        return db.dbConfig;
    }

    ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
        return context.config;
    }

    const DBConfig &DBConfig::GetConfig(const DatabaseInstance &db) {
        return db.dbConfig;
    }

    const ClientConfig &ClientConfig::GetConfig(const ClientContext &context) {
        return context.config;
    }

    TransactionManager &TransactionManager::Get(ClientContext &context) {
        return TransactionManager::Get(DatabaseInstance::GetDatabase(context));
    }

    TransactionManager &TransactionManager::Get(DatabaseInstance &db) {
        return db.GetTransactionManager();
    }

    ConnectionManager &ConnectionManager::Get(DatabaseInstance &databaseInstance) {
        return databaseInstance.GetConnectionManager();
    }

    ConnectionManager &ConnectionManager::Get(ClientContext &context) {
        return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
    }

    void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
        DBConfig default_config;
        DBConfig *config_ptr = &default_config;
        if (user_config) {
            config_ptr = user_config;
        }

        if (config_ptr->dbConfigOptions.temporary_directory.empty() && database_path) {
            // no directory specified: use default temp path
            config_ptr->dbConfigOptions.temporary_directory = string(database_path) + ".tmp";

            // special treatment for in-memory mode
            if (strcmp(database_path, ":memory:") == 0) {
                config_ptr->dbConfigOptions.temporary_directory = ".tmp";
            }
        }

        if (database_path) {
            config_ptr->dbConfigOptions.database_path = database_path;
        } else {
            config_ptr->dbConfigOptions.database_path.clear();
        }

        for (auto &open: config_ptr->replacement_opens) {
            if (open.pre_func) {
                open.data = open.pre_func(*config_ptr, open.static_data.get());
                if (open.data) {
                    break;
                }
            }
        }

        Configure(*config_ptr);

        if (user_config && !user_config->dbConfigOptions.use_temporary_directory) {
            // temporary directories explicitly disabled
            dbConfig.dbConfigOptions.temporary_directory = string();
        }

        // TODO: Support an extension here, to generate different storage managers
        // depending on the DB path structure/prefix.
        const string dbPath = dbConfig.dbConfigOptions.database_path;
        storageManager = make_unique<SingleFileStorageManager>(*this,
                                                               dbPath,
                                                               dbConfig.dbConfigOptions.access_mode == AccessMode::READ_ONLY);

        catalog = make_unique<Catalog>(*this);
        transaction_manager = make_unique<TransactionManager>(*this);
        scheduler = make_unique<TaskScheduler>(*this);
        object_cache = make_unique<ObjectCache>();
        connection_manager = make_unique<ConnectionManager>();

        // initialize the database
        storageManager->Initialize();

        // only increase thread count after storage init because we get races on catalog otherwise
        scheduler->SetThreads(dbConfig.dbConfigOptions.maximum_threads);

        for (auto &open: dbConfig.replacement_opens) {
            if (open.post_func && open.data) {
                open.post_func(*this, open.data.get());
                break;
            }
        }
    }

    DuckDB::DuckDB(const char *path,
                   DBConfig *new_config) : databaseInstance(make_shared<DatabaseInstance>()) {
        databaseInstance->Initialize(path, new_config);
        if (databaseInstance->dbConfig.dbConfigOptions.load_extensions) {
            ExtensionHelper::LoadAllExtensions(*this);
        }
    }

    DuckDB::DuckDB(const string &path,
                   DBConfig *config) : DuckDB(path.c_str(), config) {
    }

    DuckDB::DuckDB(DatabaseInstance &instance_p) : databaseInstance(instance_p.shared_from_this()) {
    }

    DuckDB::~DuckDB() {
    }

    StorageManager &DatabaseInstance::GetStorageManager() {
        return *storageManager;
    }

    Catalog &DatabaseInstance::GetCatalog() {
        return *catalog;
    }

    TransactionManager &DatabaseInstance::GetTransactionManager() {
        return *transaction_manager;
    }

    TaskScheduler &DatabaseInstance::GetScheduler() {
        return *scheduler;
    }

    ObjectCache &DatabaseInstance::GetObjectCache() {
        return *object_cache;
    }

    FileSystem &DatabaseInstance::GetFileSystem() {
        return *dbConfig.file_system;
    }

    ConnectionManager &DatabaseInstance::GetConnectionManager() {
        return *connection_manager;
    }

    FileSystem &DuckDB::GetFileSystem() {
        return databaseInstance->GetFileSystem();
    }

    Allocator &Allocator::Get(ClientContext &context) {
        return Allocator::Get(*context.databaseInstance);
    }

    Allocator &Allocator::Get(DatabaseInstance &db) {
        return *db.dbConfig.allocator;
    }

    void DatabaseInstance::Configure(DBConfig &new_config) {
        dbConfig.dbConfigOptions = new_config.dbConfigOptions;
        if (dbConfig.dbConfigOptions.access_mode == AccessMode::UNDEFINED) {
            dbConfig.dbConfigOptions.access_mode = AccessMode::READ_WRITE;
        }

        if (new_config.file_system) {
            dbConfig.file_system = move(new_config.file_system);
        } else {
            dbConfig.file_system = make_unique<VirtualFileSystem>();
        }

        if (dbConfig.dbConfigOptions.maximum_memory == (idx_t) -1) {
            auto memory = FileSystem::GetAvailableMemory();
            if (memory != DConstants::INVALID_INDEX) {
                dbConfig.dbConfigOptions.maximum_memory = memory * 8 / 10;
            }
        }

        if (new_config.dbConfigOptions.maximum_threads == (idx_t) -1) {
#ifndef DUCKDB_NO_THREADS
            dbConfig.dbConfigOptions.maximum_threads = std::thread::hardware_concurrency();
#else
            config.options.maximum_threads = 1;
#endif
        }
        dbConfig.allocator = move(new_config.allocator);
        if (!dbConfig.allocator) {
            dbConfig.allocator = make_unique<Allocator>();
        }

        dbConfig.replacement_scans = move(new_config.replacement_scans);
        dbConfig.replacement_opens = move(new_config.replacement_opens);
        dbConfig.parser_extensions = move(new_config.parser_extensions);

        dbConfig.error_manager = move(new_config.error_manager);
        if (!dbConfig.error_manager) {
            dbConfig.error_manager = make_unique<ErrorManager>();
        }

        if (!dbConfig.default_allocator) {
            dbConfig.default_allocator = Allocator::DefaultAllocatorReference();
        }
    }

    DBConfig &DBConfig::GetConfig(ClientContext &context) {
        return context.databaseInstance->dbConfig;
    }

    const DBConfig &DBConfig::GetConfig(const ClientContext &context) {
        return context.databaseInstance->dbConfig;
    }

    idx_t DatabaseInstance::NumberOfThreads() {
        return scheduler->NumberOfThreads();
    }

    const unordered_set<std::string> &DatabaseInstance::LoadedExtensions() {
        return loaded_extensions;
    }

    idx_t DuckDB::NumberOfThreads() {
        return databaseInstance->NumberOfThreads();
    }

    bool DuckDB::ExtensionIsLoaded(const std::string &name) {
        return databaseInstance->loaded_extensions.find(name) != databaseInstance->loaded_extensions.end();
    }

    void DatabaseInstance::SetExtensionLoaded(const std::string &name) {
        loaded_extensions.insert(name);
    }

    bool DatabaseInstance::TryGetCurrentSetting(const std::string &key, Value &result) {
        // check the session values
        auto &db_config = DBConfig::GetConfig(*this);
        const auto &global_config_map = db_config.dbConfigOptions.set_variables;

        auto global_value = global_config_map.find(key);
        bool found_global_value = global_value != global_config_map.end();
        if (!found_global_value) {
            return false;
        }
        result = global_value->second;
        return true;
    }

    string ClientConfig::ExtractTimezone() const {
        auto entry = set_variables.find("TimeZone");
        if (entry == set_variables.end()) {
            return "UTC";
        } else {
            return entry->second.GetValue<std::string>();
        }
    }

    ValidChecker &DatabaseInstance::GetValidChecker() {
        return db_validity;
    }

    ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
        return db.GetValidChecker();
    }

} // namespace duckdb
