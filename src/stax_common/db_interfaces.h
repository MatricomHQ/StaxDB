#pragma once

#include "stax_common/common_types.hpp"
#include <memory>

class Transaction;
class StaxTree;

class IDatabaseServices {
public:
    virtual ~IDatabaseServices() = default;
    virtual TxnID get_next_txn_id() = 0;
    virtual void update_last_committed_txn_id(TxnID id) = 0;
    virtual TxnID get_last_committed_txn_id() const = 0;
};

class ITransactionFactory {
public:
    virtual ~ITransactionFactory() = default;

    virtual std::unique_ptr<Transaction> create_transaction(
        size_t thread_id,
        uint32_t collection_idx,
        IDatabaseServices* db_services,
        StaxTree& tree,
        TxnID explicit_read_snapshot_id = 0,
        TxnID explicit_commit_id = 0
    ) = 0;
};


extern ITransactionFactory* global_transaction_factory;



void set_global_transaction_factory(ITransactionFactory* factory);