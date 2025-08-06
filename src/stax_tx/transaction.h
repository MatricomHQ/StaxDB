
#pragma once

#include "stax_common/common_types.hpp"



struct TxnContext {
    TxnID txn_id;
    TxnID read_snapshot_id;
    size_t thread_id;
};



struct TransactionBatch {
    int64_t logical_item_count_delta = 0;
    int64_t live_record_bytes_delta = 0;
};