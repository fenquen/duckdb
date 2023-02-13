#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

TransactionContext::~TransactionContext() {
	if (currentTransaction) {
		try {
			Rollback();
		} catch (...) {
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (currentTransaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}

    currentTransaction = transactionManager.StartTransaction(clientContext);
}

void TransactionContext::Commit() {
	if (!currentTransaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	auto transaction = currentTransaction;
	SetAutoCommit(true);
    currentTransaction = nullptr;
	string error = transactionManager.CommitTransaction(clientContext, transaction);
	if (!error.empty()) {
		throw TransactionException("Failed to commit: %s", error);
	}
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !currentTransaction) {
		BeginTransaction();
	}
}

void TransactionContext::Rollback() {
	if (!currentTransaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	auto transaction = currentTransaction;
	ClearTransaction();
	transactionManager.RollbackTransaction(transaction);
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
    currentTransaction = nullptr;
}

} // namespace duckdb
