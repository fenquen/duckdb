//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

    class ClientContext;

    class Transaction;

    class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
    class TransactionContext {
    public:
        TransactionContext(TransactionManager &transactionManager,
                           ClientContext &clientContext) : transactionManager(transactionManager),
                                                           clientContext(clientContext),
                                                           auto_commit(true),
                                                           currentTransaction(nullptr) {
        }

        ~TransactionContext();

        Transaction &ActiveTransaction() {
            D_ASSERT(currentTransaction);
            return *currentTransaction;
        }

        bool HasActiveTransaction() {
            return currentTransaction != nullptr;
        }

        void RecordQuery(string query);

        void BeginTransaction();

        void Commit();

        void Rollback();

        void ClearTransaction();

        void SetAutoCommit(bool value);

        bool IsAutoCommit() {
            return auto_commit;
        }

    private:
        TransactionManager &transactionManager;

        ClientContext &clientContext;

        bool auto_commit;

        Transaction *currentTransaction;

        TransactionContext(const TransactionContext &) = delete;
    };

} // namespace duckdb
