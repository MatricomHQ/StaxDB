#include "stax_db/db.h"
#include "stax_common/os_file_extensions.h"
#include "stax_tx/transaction.h"
#include "stax_tx/db_cursor.hpp"
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

DbGeneration::~DbGeneration()
{
    unmap_and_close();
}

void DbGeneration::unmap_and_close()
{
    owned_collections.clear();
    stax_allocator.reset();

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

// =================================================================================================
// --- New Cursor Implementation ---
// =================================================================================================
DBCursor::DBCursor() : ctx_(inert_context) {}

DBCursor::~DBCursor() = default;

DBCursor::DBCursor(StaxTree16* tree, const TxnContext& ctx, std::string_view start_key, std::optional<std::string_view> end_key)
    : tree_(tree),
      ctx_(ctx),
      is_valid_(false),
      current_record_(nullptr),
      start_key_buffer_(start_key),
      start_key_view_(start_key_buffer_)
{
    if (end_key) {
        end_key_buffer_ = *end_key;
        end_key_view_ = end_key_buffer_;
        has_end_key_ = true;
    }
    find_initial_leaf();
}

bool DBCursor::is_valid() const {
    return is_valid_;
}

std::string_view DBCursor::key() const {
    return is_valid_ ? current_record_->get_key() : std::string_view{};
}

DataView DBCursor::value() const {
    if (!is_valid_) return {};
    return DataView(current_record_->get_value_data(), current_record_->value_len);
}

void DBCursor::next() {
    if (!is_valid_) return;
    is_valid_ = false;
    current_record_ = nullptr;
    advance_to_next_valid();
}

bool DBCursor::is_visible(StaxRecord* record) {
    while(record) {
        if (record->txn_id <= ctx_.read_snapshot_id) {
            return !record->is_deleted;
        }
        record = tree_->get_allocator().get_ptr<StaxRecord>(record->prev_version_offset);
    }
    return false;
}

void DBCursor::find_initial_leaf() {
    uint64_t root_ptr = tree_->get_root_ptr().load(std::memory_order_acquire);
    if (root_ptr == 0) {
        is_valid_ = false;
        return;
    }
    path_stack_.push({root_ptr, 0});
    advance_to_next_valid();
}

void DBCursor::advance_to_next_valid() {
    while (!path_stack_.empty()) {
        uint64_t current_ptr = path_stack_.top().first;
        int& child_idx = path_stack_.top().second;

        if (StaxTree16::is_leaf(current_ptr)) {
            path_stack_.pop();
            StaxRecord* record = tree_->get_allocator().get_ptr<StaxRecord>(StaxTree16::get_offset(current_ptr));

            if (record->get_key() < start_key_view_) {
                continue;
            }

            if (has_end_key_ && record->get_key() >= end_key_view_) {
                // Since we are traversing in order, we can stop.
                is_valid_ = false;
                return;
            }

            if (is_visible(record)) {
                is_valid_ = true;
                current_record_ = record;
                return;
            }
        } else { // Internal node
            if (child_idx < 16) {
                InternalNode* node = tree_->get_allocator().get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
                uint64_t child_ptr = node->children[child_idx].load(std::memory_order_relaxed);
                child_idx++;
                if (child_ptr != 0) {
                    path_stack_.push({child_ptr, 0});
                }
            } else {
                path_stack_.pop();
            }
        }
    }
    is_valid_ = false;
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
    thread_local_allocators_.resize(num_threads);
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

ThreadLocalAllocator& Database::get_thread_local_allocator(size_t thread_id) {
    if (thread_id >= thread_local_allocators_.size() || !thread_local_allocators_[thread_id]) {
        throw std::out_of_range("Thread local allocator not initialized for thread ID " + std::to_string(thread_id));
    }
    return *thread_local_allocators_[thread_id];
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

    gen->stax_allocator = std::make_unique<StaxAllocator>(gen->file_header, gen->mmap_base);

    if (generations_.empty()) { // Only initialize TLABs for the first generation
        for(size_t i = 0; i < num_threads_; ++i) {
            thread_local_allocators_[i] = std::make_unique<ThreadLocalAllocator>(*gen->stax_allocator);
        }
    }


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

void Database::compact(const std::filesystem::path &db_directory, size_t num_threads, bool flatten)
{
    std::cout << "Starting compaction process for directory: " << db_directory
              << " (Flatten: " << (flatten ? "Yes" : "No") << ")" << std::endl;

    auto source_db = Database::open_existing(db_directory, num_threads);
    if (!source_db || source_db->generations_.empty())
    {
        throw std::runtime_error("Compaction failed: Could not open source database or it is empty.");
    }

    std::filesystem::path compacted_file_name = "data.stax.compact";
    std::filesystem::path compacted_path = db_directory / compacted_file_name;
    if (std::filesystem::exists(compacted_path))
    {
        std::filesystem::remove(compacted_path);
    }
    auto compacted_db = Database::create_new(db_directory, num_threads, DurabilityLevel::NoSync, compacted_file_name);
    if (!compacted_db)
    {
        throw std::runtime_error("Failed to create temporary database for compaction.");
    }

    uint32_t source_collection_count = source_db->generations_.front()->file_header->collection_array_count.load(std::memory_order_acquire);


    for (uint32_t i = 0; i < source_collection_count; ++i)
    {
        uint32_t collection_name_hash_from_source = source_db->generations_.front()->get_collection_entry_ref(i).name_hash;
        std::string collection_name_placeholder = "collection_hash_" + std::to_string(collection_name_hash_from_source);

        Collection &source_collection = *source_db->generations_.front()->owned_collections[i];
        uint32_t dest_collection_idx = compacted_db->get_collection(collection_name_placeholder);
        Collection &dest_collection = compacted_db->get_collection_by_idx(dest_collection_idx);

        TxnContext compaction_read_ctx = source_db->begin_transaction_context(0, true);
        TxnContext compaction_write_ctx = compacted_db->begin_transaction_context(0, false);
        TransactionBatch write_batch;

        if (flatten)
        {
            std::unordered_map<std::string, StaxRecord*> latest_versions;
            for (auto cursor = source_collection.seek_first(compaction_read_ctx); cursor->is_valid(); cursor->next())
            {
                latest_versions[std::string(cursor->key())] = cursor->get_current_record();
            }
            for (const auto &pair : latest_versions)
            {
                if (!pair.second->is_deleted)
                {
                    dest_collection.insert(compaction_write_ctx, write_batch, pair.first, std::string_view(pair.second->get_value_data(), pair.second->value_len));
                }
            }
        }
        else
        {
            for (auto cursor = source_collection.seek_first(compaction_read_ctx); cursor->is_valid(); cursor->next())
            {
                dest_collection.insert(compaction_write_ctx, write_batch, cursor->key(), static_cast<std::string_view>(cursor->value()));
            }
        }
        compacted_db->commit(compaction_write_ctx, dest_collection_idx, write_batch);
    }

    TxnID final_compacted_db_txn_id = compacted_db->get_last_committed_txn_id();
    DbGeneration *compacted_gen = compacted_db->get_active_generation();
    if (compacted_gen && compacted_gen->file_header)
    {
        compacted_gen->file_header->last_committed_txn_id.store(final_compacted_db_txn_id, std::memory_order_release);
    }

    compacted_db.reset();
    source_db.reset();

    std::filesystem::path original_path = db_directory / "data.stax";
    std::filesystem::path temp_path = db_directory / "data.stax.tmp";

    std::error_code ec;
    std::filesystem::rename(original_path, temp_path, ec);
    if (ec)
        throw std::runtime_error("Failed to rename original DB file to .tmp: " + ec.message());

    std::filesystem::rename(compacted_path, original_path, ec);
    if (ec)
    {
        std::filesystem::rename(temp_path, original_path);
        throw std::runtime_error("FATAL: Failed to rename compacted DB file. Original DB may be at .tmp path. Error: " + ec.message());
    }

    std::filesystem::remove(temp_path, ec);
    if (ec)
        std::cerr << "Warning: Failed to clean up temporary file '" << temp_path << "': " << ec.message() << std::endl;
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

void Database::commit(const TxnContext &ctx, uint32_t collection_idx, const TransactionBatch &batch)
{
    if (ctx.txn_id == 0)
        return;

    DbGeneration *active_gen = get_active_generation();
    if (!active_gen)
        return;

    CollectionEntry &entry = active_gen->get_collection_entry_ref(collection_idx);
    if (batch.logical_item_count_delta != 0)
    {
        entry.logical_item_count.fetch_add(batch.logical_item_count_delta, std::memory_order_relaxed);
    }
    if (batch.live_record_bytes_delta != 0)
    {
        entry.live_record_bytes.fetch_add(batch.live_record_bytes_delta, std::memory_order_relaxed);
    }

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

Collection::Collection(Database *parent_db, DbGeneration *owning_generation, uint32_t collection_idx)
    : parent_db_(parent_db), owning_generation_(owning_generation), collection_idx_(collection_idx)
{
    CollectionEntry &entry = owning_generation_->get_collection_entry_ref(collection_idx);

    tree_ = std::make_unique<StaxTree16>(
        *owning_generation_->stax_allocator,
        entry.root_node_ptr);
}

TxnContext Collection::begin_transaction_context(size_t thread_id, bool is_read_only)
{
    return parent_db_->begin_transaction_context(thread_id, is_read_only);
}

void Collection::commit(const TxnContext &ctx, TransactionBatch &batch)
{
    parent_db_->commit(ctx, collection_idx_, batch);
}

void Collection::abort(const TxnContext &ctx)
{
    parent_db_->abort(ctx);
}

void Collection::insert(const TxnContext &ctx, TransactionBatch &batch, std::string_view key, std::string_view value)
{
    if (ctx.txn_id == 0)
        throw std::runtime_error("Cannot perform writes in a read-only transaction context.");

    ThreadLocalAllocator& local_alloc = parent_db_->get_thread_local_allocator(ctx.thread_id);
    tree_->insert(local_alloc, ctx, key, value, false);

    batch.logical_item_count_delta++;
    // TODO: This size is not quite right, but it's a placeholder.
    batch.live_record_bytes_delta += (key.length() + value.length() + sizeof(StaxRecord));
}

void Collection::remove(const TxnContext &ctx, TransactionBatch &batch, std::string_view key)
{
    if (ctx.txn_id == 0)
        throw std::runtime_error("Cannot perform writes in a read-only transaction context.");
    ThreadLocalAllocator& local_alloc = parent_db_->get_thread_local_allocator(ctx.thread_id);
    tree_->remove(local_alloc, ctx, key);
    batch.logical_item_count_delta--;
}

std::optional<RecordData> Collection::get(const TxnContext &ctx, std::string_view key)
{
    for (const auto &gen_ptr : parent_db_->get_generations())
    {
        if (collection_idx_ < gen_ptr->owned_collections.size() && gen_ptr->owned_collections[collection_idx_])
        {
            auto result = gen_ptr->owned_collections[collection_idx_]->get_tree().get(ctx, key);
            if (result.has_value())
            {
                return result;
            }
        }
    }
    return std::nullopt;
}

void Collection::insert_sync_direct(std::string_view key, std::string_view value, size_t thread_id)
{
    TxnContext ctx = parent_db_->begin_transaction_context(thread_id, false);
    TransactionBatch batch;
    ThreadLocalAllocator& local_alloc = parent_db_->get_thread_local_allocator(thread_id);
    tree_->insert(local_alloc, ctx, key, value, false);
    batch.logical_item_count_delta++;
    batch.live_record_bytes_delta += (key.length() + value.length() + sizeof(StaxRecord));
    parent_db_->commit(ctx, collection_idx_, batch);
}

void Collection::remove_sync_direct(std::string_view key, size_t thread_id)
{
    TxnContext ctx = parent_db_->begin_transaction_context(thread_id, false);
    TransactionBatch batch;
    ThreadLocalAllocator& local_alloc = parent_db_->get_thread_local_allocator(thread_id);
    tree_->remove(local_alloc, ctx, key);
    batch.logical_item_count_delta--;
    parent_db_->commit(ctx, collection_idx_, batch);
}

std::unique_ptr<DBCursor> Collection::seek(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(&get_tree(), ctx, start_key, end_key);
}

std::unique_ptr<DBCursor> Collection::seek_first(const TxnContext &ctx, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(&get_tree(), ctx, "", end_key);
}

std::unique_ptr<DBCursor> Collection::seek_raw(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(&this->get_tree(), ctx, start_key, end_key);
}