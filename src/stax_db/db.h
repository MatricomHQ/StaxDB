#pragma once

#include <string>
#include <memory>
#include <filesystem>
#include <optional>
#include <string_view>
#include <mutex>
#include <vector>
#include <utility>
#include <thread>
#include <atomic>

#include "stax_common/os_platform_tools.h"
#include "stax_db/arena_structs.h"
#include "stax_core/value_store.hpp"
#include "stax_core/stax_tree.hpp"
#include "stax_common/common_types.hpp"
#include "stax_common/spin_locks.h"
#include "stax_common/db_interfaces.h"
#include "stax_tx/transaction.h"
#include "stax_common/constants.h"

class Database;
class DBCursor;
class Collection;
class NodeAllocator;

namespace StaxStats
{
    class DatabaseStatisticsCollector;
}

class HybridTimestampGenerator
{
public:
    HybridTimestampGenerator();
    TxnID get_next_id();

private:
    std::atomic<TxnID> last_generated_id_;

    struct ThreadTxnIDGenerator
    {
        TxnID current_local_id_;
        TxnID local_id_end_;
        ThreadTxnIDGenerator() : current_local_id_(0), local_id_end_(0) {}
    };
    static thread_local ThreadTxnIDGenerator tls_generator_;
    static constexpr size_t BATCH_SIZE = 1000;
};

struct DbGeneration
{
    std::filesystem::path path;
    uint8_t *mmap_base = nullptr;
    size_t mmap_size = 0;
    OsFileHandleType file_handle = INVALID_OS_FILE_HANDLE;
    OsFileHandleType lock_file_handle = INVALID_OS_FILE_HANDLE;
    FileHeader *file_header = nullptr;

    std::unique_ptr<NodeAllocator> internal_node_allocator;

    std::vector<std::unique_ptr<Collection>> owned_collections;
    std::vector<std::unique_ptr<CollectionRecordAllocator>> owned_record_allocators;

    ~DbGeneration();
    void unmap_and_close();
    CollectionEntry &get_collection_entry_ref(uint32_t idx) const;
};

class Collection
{
public:
    uint32_t get_id() const { return collection_idx_; }
    StaxTree &get_critbit_tree() { return *critbit_tree_; }
    const StaxTree &get_critbit_tree() const { return *critbit_tree_; }

    Collection(Database *parent_db, DbGeneration *owning_generation, uint32_t collection_idx, CollectionRecordAllocator &record_allocator);

    TxnContext begin_transaction_context(size_t thread_id, bool is_read_only = false);
    void commit(const TxnContext &ctx, TransactionBatch &batch);
    void abort(const TxnContext &ctx);

    void insert(const TxnContext &ctx, TransactionBatch &batch, std::string_view key, std::string_view value);
    void remove(const TxnContext &ctx, TransactionBatch &batch, std::string_view key);
    std::optional<RecordData> get(const TxnContext &ctx, std::string_view key);

    void insert_sync_direct(std::string_view key, std::string_view value, size_t thread_id);
    void remove_sync_direct(std::string_view key, size_t thread_id);

    std::unique_ptr<DBCursor> seek(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key = std::nullopt);
    std::unique_ptr<DBCursor> seek_first(const TxnContext &ctx, std::optional<std::string_view> end_key = std::nullopt);
    std::unique_ptr<DBCursor> seek_raw(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key = std::nullopt);

private:
    friend class Database;
    friend class DBCursor;
    friend class MergedCursorImpl;

    Database *parent_db_;
    DbGeneration *owning_generation_;
    uint32_t collection_idx_;

    std::unique_ptr<StaxTree> critbit_tree_;
    CollectionRecordAllocator *record_allocator_;
};

enum class DurabilityLevel
{
    NoSync,
    SyncOnCommit
};

class Database
{
public:
    ~Database();

    static std::unique_ptr<Database> create_new(const std::filesystem::path &db_directory, size_t num_threads, DurabilityLevel level = DurabilityLevel::NoSync, const std::filesystem::path &file_name = "data.stax");
    static std::unique_ptr<Database> open_existing(const std::filesystem::path &db_directory, size_t num_threads, DurabilityLevel level = DurabilityLevel::NoSync);
    static void drop(const std::filesystem::path& db_directory);

    uint32_t get_collection(std::string_view name);
    Collection &get_collection_by_idx(uint32_t collection_idx);

    Collection *get_ofv_collection();
    Collection *get_fvo_collection();

    TxnContext begin_transaction_context(size_t thread_id, bool is_read_only = false);
    void commit(const TxnContext &ctx, uint32_t collection_idx, const TransactionBatch &batch);
    void abort(const TxnContext &ctx);

    const std::filesystem::path &get_db_path() const;
    size_t get_num_configured_threads() const { return num_threads_; }
    DurabilityLevel get_durability_level() const { return durability_level_; }

    void dump_state(std::ostream &os) const;

    static void compact(const std::filesystem::path &db_directory, size_t num_threads, bool flatten = false);

    StaxStats::DatabaseStatisticsCollector get_statistics_collector();

    void update_last_committed_txn_id(TxnID id);
    TxnID get_last_committed_txn_id() const;
    TxnID get_next_txn_id();

public:
    Database(const std::filesystem::path &base_dir, size_t num_threads, DurabilityLevel level);

    const std::vector<std::unique_ptr<DbGeneration>> &get_generations() const { return generations_; }
    SpinLock &get_generations_lock() { return generations_lock_; }
    DbGeneration *get_active_generation() { return generations_.empty() ? nullptr : generations_.front().get(); }

private:
    friend class Collection;
    friend class StaxStats::DatabaseStatisticsCollector;
    friend class MergedCursorImpl;
    friend class DBCursor;
    friend class NodeAllocator;
    friend class CollectionRecordAllocator;

    static uint64_t hash_name(std::string_view name);

    std::unique_ptr<HybridTimestampGenerator> timestamp_generator_;
    std::filesystem::path base_directory_;
    size_t num_threads_;
    DurabilityLevel durability_level_;

    std::vector<std::unique_ptr<DbGeneration>> generations_;
    SpinLock generations_lock_;

    void open_generation(const std::filesystem::path &db_directory, const std::filesystem::path &file_name, bool is_new);
};