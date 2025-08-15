#include "stax_db/db.h"
#include "stax_common/os_file_extensions.h"
#include "stax_tx/transaction.h"
#include "stax_graph/graph_engine.h"
#include "stax_db/statistics.h"
#include <stdexcept>
#include <new>
#include <filesystem>
#include <utility>
#include <iostream>
#include <functional>
#include <chrono>
#include <mutex>
#include <algorithm>
#include <unordered_map>

#include "stax_common/roaring.h"
#include "stax_core/node_allocator.hpp"

DbGeneration::~DbGeneration()
{
    unmap_and_close();
}

void DbGeneration::unmap_and_close()
{
    owned_collections.clear();
    internal_node_allocator.reset();

    if (file_header && mmap_base)
    {
        OSFileExtensions::flush_file_range_raw(mmap_base, mmap_size);
    }
    if (mmap_base)
    {
        OSFileExtensions::unmap_file_raw(mmap_base, mmap_size);
        mmap_base = nullptr;
    }
    if (file_handle != INVALID_OS_FILE_HANDLE)
    {
        OSFileExtensions::close_file(file_handle);
        file_handle = INVALID_OS_FILE_HANDLE;
    }

    if (lock_file_handle != INVALID_OS_FILE_HANDLE)
    {
        OSFileExtensions::unlock_file(lock_file_handle);
        lock_file_handle = INVALID_OS_FILE_HANDLE;
    }
}

CollectionEntry &DbGeneration::get_collection_entry_ref(uint32_t idx) const
{
    if (idx >= file_header->collection_array_capacity)
    {
        throw std::out_of_range("Collection index out of bounds for on-disk array.");
    }
    return *reinterpret_cast<CollectionEntry *>(mmap_base + file_header->collection_array_offset + (idx * sizeof(CollectionEntry)));
}

thread_local HybridTimestampGenerator::ThreadTxnIDGenerator HybridTimestampGenerator::tls_generator_;

HybridTimestampGenerator::HybridTimestampGenerator() : last_generated_id_(0) {}

TxnID HybridTimestampGenerator::get_next_id()
{
    TxnID batch_start_id;
    TxnID next_id;

    if (tls_generator_.current_local_id_ < tls_generator_.local_id_end_)
    {
        next_id = tls_generator_.current_local_id_;
        tls_generator_.current_local_id_++;
        return next_id;
    }
    else
    {
        uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

        TxnID expected_global_id = last_generated_id_.load(std::memory_order_acquire);

        while (true)
        {
            TxnID new_proposed_global_id;
            TxnID expected_ts_part = expected_global_id >> 16;

            if (now_ms > expected_ts_part)
            {
                new_proposed_global_id = (now_ms << 16);
            }
            else
            {
                new_proposed_global_id = expected_global_id + 1;
            }

            batch_start_id = new_proposed_global_id;
            if (last_generated_id_.compare_exchange_weak(expected_global_id, batch_start_id + BATCH_SIZE, std::memory_order_release, std::memory_order_acquire))
            {
                tls_generator_.current_local_id_ = batch_start_id;
                tls_generator_.local_id_end_ = batch_start_id + BATCH_SIZE;

                next_id = tls_generator_.current_local_id_;
                tls_generator_.current_local_id_++;
                return next_id;
            }
            now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
        }
    }
}

Database::Database(const std::filesystem::path &base_dir, size_t num_threads, DurabilityLevel level)
    : timestamp_generator_(std::make_unique<HybridTimestampGenerator>()),
      base_directory_(base_dir),
      num_threads_(num_threads),
      durability_level_(level)
{
}

Database::~Database()
{
    UniqueSpinLockGuard lock(generations_lock_);
    generations_.clear();
}

uint64_t Database::hash_name(std::string_view name)
{
    uint64_t hash = 14695981039346656037ULL;
    for (char c : name)
    {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

void Database::update_last_committed_txn_id(TxnID id)
{
    if (generations_.empty())
        return;
    DbGeneration &active_gen = *generations_.front();
    if (active_gen.file_header)
    {
        TxnID observed_max_id = active_gen.file_header->last_committed_txn_id.load(std::memory_order_acquire);
        while (id > observed_max_id)
        {
            if (active_gen.file_header->last_committed_txn_id.compare_exchange_weak(observed_max_id, id,
                                                                                    std::memory_order_release,
                                                                                    std::memory_order_acquire))
            {
                break;
            }
        }
    }
}

TxnID Database::get_last_committed_txn_id() const
{
    if (generations_.empty())
        return 0;
    const DbGeneration &active_gen = *generations_.front();
    if (!active_gen.file_header)
        return 0;
    return active_gen.file_header->last_committed_txn_id.load(std::memory_order_acquire);
}

TxnID Database::get_next_txn_id()
{
    return timestamp_generator_->get_next_id();
}

std::unique_ptr<Database> Database::create_new(const std::filesystem::path &db_directory, size_t num_threads, DurabilityLevel level, const std::filesystem::path &file_name)
{
    auto db = std::make_unique<Database>(db_directory, num_threads, level);
    if (!std::filesystem::exists(db_directory))
    {
        std::filesystem::create_directories(db_directory);
    }
    db->open_generation(db_directory, file_name, true);
    return db;
}

std::unique_ptr<Database> Database::open_existing(const std::filesystem::path &db_directory, size_t num_threads, DurabilityLevel level)
{
    auto db = std::make_unique<Database>(db_directory, num_threads, level);

    std::vector<std::filesystem::path> gen_paths;
    if (std::filesystem::exists(db_directory / "data.stax"))
    {
        gen_paths.push_back(db_directory / "data.stax");
    }
    for (int i = 0;; ++i)
    {
        std::filesystem::path old_gen_path = db_directory / ("data.stax_g" + std::to_string(i));
        if (std::filesystem::exists(old_gen_path))
        {
            gen_paths.push_back(old_gen_path);
        }
        else
        {
            break;
        }
    }

    if (gen_paths.empty())
    {
        db->open_generation(db_directory, "data.stax", true);
        return db;
    }

    std::sort(gen_paths.begin(), gen_paths.end());

    for (const auto &path : gen_paths)
    {
        db->open_generation(db_directory, path.filename(), false);
    }

    if (db->generations_.size() > 1)
    {
        std::cerr << "Warning: Multiple database generations found. A previous compaction may have been interrupted." << std::endl;
    }

    return db;
}

void Database::drop(const std::filesystem::path& db_directory) {
    if (!std::filesystem::exists(db_directory) || !std::filesystem::is_directory(db_directory)) {
        return; 
    }

    std::error_code ec;
    std::filesystem::remove_all(db_directory, ec);
    if (ec) {
        throw std::runtime_error("Failed to drop database directory '" + db_directory.string() + "': " + ec.message());
    }
}

void Database::open_generation(const std::filesystem::path &db_directory, const std::filesystem::path &file_name, bool is_new)
{
    auto gen = std::make_unique<DbGeneration>();
    gen->path = db_directory / file_name;

    std::filesystem::path lock_path = gen->path;
    lock_path += ".lock";
    gen->lock_file_handle = OSFileExtensions::lock_file(lock_path);
    if (gen->lock_file_handle == INVALID_OS_FILE_HANDLE)
    {
        throw std::runtime_error("Failed to acquire lock for database file: " + lock_path.string());
    }

    bool file_actually_exists = std::filesystem::exists(gen->path);
    is_new = !file_actually_exists;

    if (is_new)
    {
        gen->mmap_size = DB_MAX_VIRTUAL_SIZE;
        gen->file_handle = OSFileExtensions::open_file_for_writing(gen->path);
        if (gen->file_handle == INVALID_OS_FILE_HANDLE)
        {
            throw std::runtime_error("Failed to create database file at " + gen->path.string());
        }

        std::string err = OSFileExtensions::extend_file_raw(gen->file_handle, gen->mmap_size);
        if (!err.empty())
        {
            OSFileExtensions::close_file(gen->file_handle);
            throw std::runtime_error("Failed to extend database file: " + err);
        }
    }
    else
    {
        try
        {
            gen->mmap_size = std::filesystem::file_size(gen->path);
        }
        catch (const std::filesystem::filesystem_error &e)
        {
            throw std::runtime_error(std::string("Failed to get file size for '") + gen->path.string() + "': " + e.what());
        }
        if (gen->mmap_size < sizeof(FileHeader))
        {
            throw std::runtime_error("Cannot open empty or corrupt file.");
        }
        gen->file_handle = OSFileExtensions::open_file_for_reading_writing(gen->path);
        if (gen->file_handle == INVALID_OS_FILE_HANDLE)
        {
            throw std::runtime_error("Failed to open database file.");
        }
    }

    auto map_result = OSFileExtensions::map_file_raw(gen->file_handle, 0, gen->mmap_size, true);
    gen->mmap_base = static_cast<uint8_t *>(map_result.first);
    if (!gen->mmap_base)
    {
        OSFileExtensions::close_file(gen->file_handle);
        throw std::runtime_error("Failed to map database file: " + map_result.second);
    }

    gen->file_header = reinterpret_cast<FileHeader *>(gen->mmap_base);

    if (is_new)
    {
        new (gen->file_header) FileHeader();
        gen->file_header->magic = 0xDEADBEEFCAFEBABE;
        gen->file_header->version = 12;
        gen->file_header->file_size = sizeof(FileHeader);
        gen->file_header->last_committed_txn_id.store(0);

        const size_t collection_metadata_region_size = MAX_COLLECTIONS_PER_DB_INITIAL * sizeof(CollectionEntry);

        gen->file_header->collection_array_offset = sizeof(FileHeader);
        gen->file_header->collection_array_count.store(0);
        gen->file_header->collection_array_capacity = MAX_COLLECTIONS_PER_DB_INITIAL;

        gen->file_header->global_alloc_offset.store(gen->file_header->collection_array_offset + collection_metadata_region_size);
    }
    else
    {
        if (gen->file_header->magic != 0xDEADBEEFCAFEBABE)
        {
            throw std::runtime_error("Invalid database file format.");
        }
        if (gen->file_header->version < 12)
        {
            throw std::runtime_error("Database file is from an older, incompatible version.");
        }
    }

    gen->internal_node_allocator = std::make_unique<StaxAllocator>(gen->file_header, gen->mmap_base);

    uint32_t active_collection_count = gen->file_header->collection_array_count.load(std::memory_order_acquire);
    uint32_t array_capacity = gen->file_header->collection_array_capacity;

    gen->owned_collections.reserve(array_capacity);

    for (uint32_t i = 0; i < active_collection_count; ++i)
    {
        gen->owned_collections.emplace_back(
            std::make_unique<Collection>(this, gen.get(), i));
    }

    UniqueSpinLockGuard lock(generations_lock_);
    generations_.push_back(std::move(gen));
}

uint32_t Database::get_collection(std::string_view name)
{
    UniqueSpinLockGuard lock(generations_lock_);
    if (generations_.empty())
    {
        throw std::runtime_error("Database is not open.");
    }
    DbGeneration &active_gen = *generations_.front();

    uint32_t name_hash_val = static_cast<uint32_t>(hash_name(name));

    for (;;)
    {
        uint32_t observed_count = active_gen.file_header->collection_array_count.load(std::memory_order_acquire);

        for (uint32_t i = 0; i < observed_count; ++i)
        {
            if (active_gen.get_collection_entry_ref(i).name_hash == name_hash_val)
            {
                if (i < active_gen.owned_collections.size() && active_gen.owned_collections[i])
                {
                    return i;
                }
            }
        }

        if (observed_count >= active_gen.file_header->collection_array_capacity)
        {
            throw std::runtime_error("Pre-allocated collection metadata region is full.");
        }

        if (observed_count >= active_gen.owned_collections.size())
        {
            active_gen.owned_collections.resize(observed_count + 1);
        }

        uint32_t expected_count_for_cas = observed_count;
        if (active_gen.file_header->collection_array_count.compare_exchange_weak(expected_count_for_cas, observed_count + 1))
        {
            uint32_t new_index = observed_count;
            CollectionEntry &new_entry = active_gen.get_collection_entry_ref(new_index);

            new_entry.name_hash = name_hash_val;
            new_entry.root_node_ptr.store(0, std::memory_order_relaxed);
            new_entry.logical_item_count.store(0, std::memory_order_relaxed);
            new_entry.live_record_bytes.store(0, std::memory_order_relaxed);
            new_entry.object_id_counter.store(1, std::memory_order_relaxed);

            active_gen.owned_collections[new_index] = std::make_unique<Collection>(this, &active_gen, new_index);

            return new_index;
        }
    }
}

Collection &Database::get_collection_by_idx(uint32_t collection_idx)
{
    if (generations_.empty())
    {
        throw std::runtime_error("Database is not open.");
    }
    DbGeneration &active_gen = *generations_.front();
    if (collection_idx >= active_gen.owned_collections.size() || !active_gen.owned_collections[collection_idx])
    {
        throw std::out_of_range("Collection index out of valid range or collection not initialized.");
    }
    return *active_gen.owned_collections[collection_idx];
}

Collection *Database::get_ofv_collection()
{
    uint32_t idx = get_collection("graph_ofv");
    return &get_collection_by_idx(idx);
}

Collection *Database::get_fvo_collection()
{
    uint32_t idx = get_collection("graph_fvo");
    return &get_collection_by_idx(idx);
}

const std::filesystem::path &Database::get_db_path() const
{
    if (generations_.empty())
    {
        static const std::filesystem::path empty_path;
        return empty_path;
    }
    return generations_.front()->path;
}

StaxStats::DatabaseStatisticsCollector Database::get_statistics_collector()
{
    return StaxStats::DatabaseStatisticsCollector(this);
}

TxnContext Database::begin_transaction_context(size_t thread_id, bool is_read_only)
{
    if (is_read_only)
    {
        return {0, get_last_committed_txn_id(), thread_id};
    }
    else
    {
        TxnID new_id = get_next_txn_id();
        return {new_id, new_id, thread_id};
    }
}

void Database::commit(const TxnContext &ctx, uint32_t collection_idx)
{
    if (ctx.txn_id == 0)
        return;

    DbGeneration *active_gen = get_active_generation();
    if (!active_gen)
        return;

    update_last_committed_txn_id(ctx.txn_id);

    if (durability_level_ == DurabilityLevel::SyncOnCommit)
    {
        if (active_gen->mmap_base)
        {
            std::string err = OSFileExtensions::flush_file_range_raw(active_gen->mmap_base, active_gen->mmap_size);
            if (!err.empty())
            {
                throw std::runtime_error("FATAL: Failed to flush data to disk during durable commit: " + err);
            }
        }
    }
}

void Database::abort(const TxnContext &ctx)
{
}

StaxAllocator* Database::get_global_allocator() {
    if (generations_.empty()) {
        return nullptr;
    }
    return generations_.front()->internal_node_allocator.get();
}

Collection::Collection(Database *parent_db, DbGeneration *owning_generation, uint32_t collection_idx)
    : parent_db_(parent_db), owning_generation_(owning_generation), collection_idx_(collection_idx)
{
    CollectionEntry &entry = owning_generation_->get_collection_entry_ref(collection_idx);

    critbit_tree_ = std::make_unique<StaxTree16>(
        *owning_generation_->internal_node_allocator,
        entry.root_node_ptr);
}

TxnContext Collection::begin_transaction_context(size_t thread_id, bool is_read_only)
{
    return parent_db_->begin_transaction_context(thread_id, is_read_only);
}

void Collection::commit(const TxnContext &ctx)
{
    parent_db_->commit(ctx, collection_idx_);
}

void Collection::abort(const TxnContext &ctx)
{
    parent_db_->abort(ctx);
}

void Collection::insert(const TxnContext &ctx, std::string_view key, std::string_view value)
{
    if (ctx.txn_id == 0)
        throw std::runtime_error("Cannot perform writes in a read-only transaction context.");

    ThreadLocalAllocator local_alloc(*parent_db_->get_global_allocator());
    critbit_tree_->insert(local_alloc, ctx, key, value, false);
}

void Collection::remove(const TxnContext &ctx, std::string_view key)
{
    if (ctx.txn_id == 0)
        throw std::runtime_error("Cannot perform writes in a read-only transaction context.");

    ThreadLocalAllocator local_alloc(*parent_db_->get_global_allocator());
    critbit_tree_->remove(local_alloc, ctx, key);
}

StaxRecord* Collection::get(const TxnContext &ctx, std::string_view key)
{
    // The new MVCC model means we only need to check the active generation's tree.
    // The tree itself handles visibility of versions.
    return critbit_tree_->get(ctx, key);
}

void Collection::insert_sync_direct(std::string_view key, std::string_view value, size_t thread_id)
{
    TxnContext ctx = parent_db_->begin_transaction_context(thread_id, false);
    ThreadLocalAllocator local_alloc(*parent_db_->get_global_allocator());
    critbit_tree_->insert(local_alloc, ctx, key, value, false);
    parent_db_->commit(ctx, collection_idx_);
}

void Collection::remove_sync_direct(std::string_view key, size_t thread_id)
{
    TxnContext ctx = parent_db_->begin_transaction_context(thread_id, false);
    ThreadLocalAllocator local_alloc(*parent_db_->get_global_allocator());
    critbit_tree_->remove(local_alloc, ctx, key);
    parent_db_->commit(ctx, collection_idx_);
}

std::vector<StaxRecord*> Collection::range(const TxnContext &ctx, std::string_view prefix)
{
    return critbit_tree_->range(ctx, prefix);
}

std::vector<StaxRecord*> Collection::range(const TxnContext &ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts)
{
    return critbit_tree_->range(ctx, prefix, start_ts, end_ts);
}