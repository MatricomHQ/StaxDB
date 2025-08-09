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
#include "stax_core/node_allocator.hpp"

DbGeneration::~DbGeneration()
{
    unmap_and_close();
}

void DbGeneration::unmap_and_close()
{
    owned_collections.clear();
    owned_record_allocators.clear();
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

MergedCursorImpl::MergedCursorImpl(Database *db, const TxnContext &ctx, uint32_t collection_idx, std::string_view start_key_view, std::optional<std::string_view> end_key)
    : db_(db), ctx_(ctx)
{
    if (end_key)
    {
        has_end_key_ = true;
        end_key_buffer_ = std::string(*end_key);
        end_key_view_ = end_key_buffer_;
    }

    const auto &generations = db_->get_generations();
    for (size_t i = 0; i < generations.size(); ++i)
    {
        if (collection_idx < generations[i]->owned_collections.size() && generations[i]->owned_collections[collection_idx])
        {
            Collection &col = *generations[i]->owned_collections[collection_idx];
            DBCursor generation_cursor(db, ctx, &col.get_critbit_tree(), start_key_view, end_key, false);
            if (generation_cursor.is_valid())
            {
                pq_.push({std::move(generation_cursor), i});
            }
        }
    }
    advance();
}

void MergedCursorImpl::advance()
{
    while (true)
    {
        if (pq_.empty())
        {
            is_valid_ = false;
            return;
        }

        MergeCursorState top_state = std::move(const_cast<MergeCursorState &>(pq_.top()));
        pq_.pop();
        std::string_view candidate_key = top_state.cursor.key();

        if (has_end_key_ && candidate_key >= end_key_view_)
        {
            is_valid_ = false;
            return;
        }

        std::vector<MergeCursorState> candidate_versions;
        candidate_versions.push_back(std::move(top_state));

        while (!pq_.empty() && pq_.top().cursor.key() == candidate_key)
        {
            candidate_versions.push_back(std::move(const_cast<MergeCursorState &>(pq_.top())));
            pq_.pop();
        }

        RecordData best_visible_record;
        TxnID best_visible_txn_id = 0;

        for (auto &candidate : candidate_versions)
        {
            StaxTree *tree_ptr = candidate.cursor.tree_;
            RecordData current_record_to_check = candidate.cursor.current_record_data_;

            while (true)
            {
                if (current_record_to_check.txn_id <= ctx_.read_snapshot_id)
                {
                    if (current_record_to_check.txn_id > best_visible_txn_id)
                    {
                        best_visible_txn_id = current_record_to_check.txn_id;
                        best_visible_record = current_record_to_check;
                    }
                    break;
                }
                if (current_record_to_check.prev_version_rel_offset == CollectionRecordAllocator::NIL_RECORD_OFFSET)
                {
                    break;
                }
                current_record_to_check = tree_ptr->get_record_data_by_offset(current_record_to_check.prev_version_rel_offset);
            }

            candidate.cursor.next();
            if (candidate.cursor.is_valid())
            {
                pq_.push(std::move(candidate));
            }
        }

        if (best_visible_txn_id > 0 && !best_visible_record.is_deleted)
        {
            is_valid_ = true;
            last_key_buffer_.assign(best_visible_record.key_view());
            last_key_view_ = last_key_buffer_;
            current_record_data_ = best_visible_record;
            return;
        }
    }
}

DBCursor::DBCursor() : impl_(nullptr), ctx_(inert_context) {}

DBCursor::DBCursor(Database *db, const TxnContext &ctx, uint32_t collection_idx, std::string_view start_key, std::optional<std::string_view> end_key)
    : impl_(std::make_unique<MergedCursorImpl>(db, ctx, collection_idx, start_key, end_key)), ctx_(ctx) {}

DBCursor::DBCursor(Database *db, const TxnContext &ctx, StaxTree *tree, std::optional<std::string_view> end_key, bool raw_mode)
    : db_(db), ctx_(ctx), tree_(tree), is_valid_(false), raw_mode_(raw_mode)
{
    if (end_key)
    {
        has_end_key_ = true;
        end_key_buffer_ = std::string(*end_key);
        end_key_view_ = end_key_buffer_;
    }
    tree_->seek("", path_stack_);
    validate_current_leaf();
    if (is_valid_ && has_end_key_ && key() >= end_key_view_)
    {
        is_valid_ = false;
    }
}

DBCursor::DBCursor(Database *db, const TxnContext &ctx, StaxTree *tree, std::string_view start_key, std::optional<std::string_view> end_key, bool raw_mode)
    : db_(db), ctx_(ctx), tree_(tree), is_valid_(false), raw_mode_(raw_mode)
{
    if (end_key)
    {
        has_end_key_ = true;
        end_key_buffer_ = std::string(*end_key);
        end_key_view_ = end_key_buffer_;
    }
    tree_->seek(start_key, path_stack_);

    validate_current_leaf();

    while (!is_valid_ && !path_stack_.empty())
    {
        next();
    }

    if (is_valid_)
    {
        if (has_end_key_ && key() >= end_key_view_)
        {
            is_valid_ = false;
            return;
        }
    }
}

DBCursor::~DBCursor() = default;

bool DBCursor::is_valid() const
{
    if (impl_)
        return impl_->is_valid_;
    return is_valid_;
}

std::string_view DBCursor::key() const
{
    if (impl_)
        return impl_->last_key_view_;
    return std::string_view(current_key_ptr_, current_key_len_);
}

DataView DBCursor::value() const
{
    if (impl_)
        return impl_->is_valid_ ? DataView(impl_->current_record_data_.value_ptr, impl_->current_record_data_.value_len) : DataView{};
    if (!is_valid_)
        return {};
    return DataView(current_record_data_.value_ptr, current_record_data_.value_len);
}

void DBCursor::advance_to_next_physical_leaf()
{
    if (path_stack_.empty())
    {
        is_valid_ = false;
        return;
    }
    uint64_t current_leaf_pointer = path_stack_.top();
    path_stack_.pop();

    uint64_t next_subtree_root = NIL_POINTER;
    while (!path_stack_.empty())
    {
        uint64_t parent_pointer = path_stack_.top();

        uint64_t left_child_val = tree_->internal_node_allocator_.get_left_child_ptr(parent_pointer).load(std::memory_order_acquire);

        if (left_child_val == current_leaf_pointer)
        {
            next_subtree_root = tree_->internal_node_allocator_.get_right_child_ptr(parent_pointer).load(std::memory_order_acquire);
            break;
        }
        current_leaf_pointer = parent_pointer;
        path_stack_.pop();
    }

    if (next_subtree_root != NIL_POINTER)
    {
        uint64_t pointer_to_push = next_subtree_root;
        while (pointer_to_push != NIL_POINTER)
        {
            path_stack_.push(pointer_to_push);
            if (pointer_to_push & POINTER_TAG_BIT)
                break;
            pointer_to_push = tree_->internal_node_allocator_.get_left_child_ptr(pointer_to_push).load(std::memory_order_acquire);
        }
    }
}

void DBCursor::next()
{
    if (impl_)
    {
        impl_->advance();
        return;
    }

    while (true)
    {
        advance_to_next_physical_leaf();

        if (path_stack_.empty())
        {
            is_valid_ = false;
            return;
        }

        validate_current_leaf();

        if (is_valid_)
        {
            if (has_end_key_ && key() >= end_key_view_)
            {
                is_valid_ = false;
                return;
            }
            return;
        }
    }
}

void DBCursor::validate_current_leaf()
{
    if (path_stack_.empty())
    {
        is_valid_ = false;
        return;
    }
    uint64_t current_pointer = path_stack_.top();

    if (!(current_pointer & POINTER_TAG_BIT))
    {
        is_valid_ = false;
        return;
    }

    uint32_t record_relative_offset = current_pointer & POINTER_INDEX_MASK;

    if (raw_mode_)
    {

        current_record_data_ = tree_->record_allocator_.get_record_data(record_relative_offset);
        current_key_ptr_ = current_record_data_.key_ptr;
        current_key_len_ = current_record_data_.key_len;
        is_valid_ = !current_record_data_.is_deleted;
        return;
    }

    uint32_t version_relative_offset_for_mvcc = record_relative_offset;
    while (version_relative_offset_for_mvcc != CollectionRecordAllocator::NIL_RECORD_OFFSET)
    {
        RecordData record = tree_->record_allocator_.get_record_data(version_relative_offset_for_mvcc);
        if (record.txn_id <= ctx_.read_snapshot_id)
        {
            current_record_data_ = record;
            current_key_ptr_ = record.key_ptr;
            current_key_len_ = record.key_len;
            is_valid_ = !current_record_data_.is_deleted;
            return;
        }
        version_relative_offset_for_mvcc = record.prev_version_rel_offset;
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

    gen->internal_node_allocator = std::make_unique<NodeAllocator>(gen->file_header, gen->mmap_base);

    uint32_t active_collection_count = gen->file_header->collection_array_count.load(std::memory_order_acquire);
    uint32_t array_capacity = gen->file_header->collection_array_capacity;

    gen->owned_collections.reserve(array_capacity);
    gen->owned_record_allocators.reserve(array_capacity);

    for (uint32_t i = 0; i < active_collection_count; ++i)
    {
        gen->owned_record_allocators.emplace_back(
            std::make_unique<CollectionRecordAllocator>(gen->file_header, gen->mmap_base, num_threads_));
        gen->owned_collections.emplace_back(
            std::make_unique<Collection>(this, gen.get(), i, *gen->owned_record_allocators[i]));
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
            active_gen.owned_record_allocators.resize(observed_count + 1);
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

            active_gen.owned_record_allocators[new_index] = std::make_unique<CollectionRecordAllocator>(active_gen.file_header, active_gen.mmap_base, num_threads_);
            active_gen.owned_collections[new_index] = std::make_unique<Collection>(this, &active_gen, new_index, *active_gen.owned_record_allocators[new_index]);

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
    TxnID compaction_snapshot_id = source_db->get_last_committed_txn_id();

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
            std::unordered_map<std::string, RecordData> latest_versions;
            for (auto cursor = source_collection.seek_first(compaction_read_ctx); cursor->is_valid(); cursor->next())
            {
                latest_versions[std::string(cursor->key())] = cursor->current_record_data_;
            }
            for (const auto &pair : latest_versions)
            {
                if (!pair.second.is_deleted)
                {
                    dest_collection.insert(compaction_write_ctx, write_batch, pair.first, pair.second.value_view());
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

Collection::Collection(Database *parent_db, DbGeneration *owning_generation, uint32_t collection_idx, CollectionRecordAllocator &record_allocator)
    : parent_db_(parent_db), owning_generation_(owning_generation), collection_idx_(collection_idx), record_allocator_(&record_allocator)
{
    CollectionEntry &entry = owning_generation_->get_collection_entry_ref(collection_idx);

    critbit_tree_ = std::make_unique<StaxTree>(
        *owning_generation_->internal_node_allocator,
        *record_allocator_,
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
    critbit_tree_->insert(ctx, key, value);
    batch.logical_item_count_delta++;
    batch.live_record_bytes_delta += (key.length() + value.length() + CollectionRecordAllocator::HEADER_SIZE);
}

void Collection::remove(const TxnContext &ctx, TransactionBatch &batch, std::string_view key)
{
    if (ctx.txn_id == 0)
        throw std::runtime_error("Cannot perform writes in a read-only transaction context.");
    critbit_tree_->remove(ctx, key);
    batch.logical_item_count_delta--;
}

std::optional<RecordData> Collection::get(const TxnContext &ctx, std::string_view key)
{
    for (const auto &gen_ptr : parent_db_->get_generations())
    {
        if (collection_idx_ < gen_ptr->owned_collections.size() && gen_ptr->owned_collections[collection_idx_])
        {
            auto result = gen_ptr->owned_collections[collection_idx_]->get_critbit_tree().get(ctx, key);
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
    critbit_tree_->insert(ctx, key, value);
    batch.logical_item_count_delta++;
    batch.live_record_bytes_delta += (key.length() + value.length() + CollectionRecordAllocator::HEADER_SIZE);
    parent_db_->commit(ctx, collection_idx_, batch);
}

void Collection::remove_sync_direct(std::string_view key, size_t thread_id)
{
    TxnContext ctx = parent_db_->begin_transaction_context(thread_id, false);
    TransactionBatch batch;
    critbit_tree_->remove(ctx, key);
    batch.logical_item_count_delta--;
    parent_db_->commit(ctx, collection_idx_, batch);
}

std::unique_ptr<DBCursor> Collection::seek(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(parent_db_, ctx, collection_idx_, start_key, end_key);
}

std::unique_ptr<DBCursor> Collection::seek_first(const TxnContext &ctx, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(parent_db_, ctx, collection_idx_, "", end_key);
}

std::unique_ptr<DBCursor> Collection::seek_raw(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key)
{
    return std::make_unique<DBCursor>(parent_db_, ctx, &this->get_critbit_tree(), start_key, end_key, true);
}

uint64_t CollectionRecordAllocator::allocate_data_chunk(size_t size_bytes, size_t alignment) {
    if (!file_header_) {
        throw std::runtime_error("Cannot allocate chunk: file header is null.");
    }

    if ((alignment & (alignment - 1)) != 0)
    {
        throw std::invalid_argument("Alignment must be a power of two.");
    }

    const uint64_t alignment_mask = alignment - 1;
    uint64_t current_offset = file_header_->global_alloc_offset.load(std::memory_order_acquire);

    while (true)
    {
        uint64_t aligned_offset = (current_offset + alignment_mask) & ~alignment_mask;
        uint64_t next_offset = aligned_offset + size_bytes;

        if (next_offset > DB_MAX_VIRTUAL_SIZE)
        {
            throw std::runtime_error("Database out of space during aligned chunk allocation.");
        }

        if (file_header_->global_alloc_offset.compare_exchange_weak(current_offset, next_offset, std::memory_order_release, std::memory_order_acquire))
        {
            return aligned_offset;
        }
    }
}


CollectionRecordAllocator::CollectionRecordAllocator(FileHeader* file_header, uint8_t* mmap_base_addr, size_t num_threads_configured_for_db) noexcept
    : file_header_(file_header), mmap_base_addr_(mmap_base_addr), num_threads_configured_for_db_(num_threads_configured_for_db)
{
    for (size_t i = 0; i < MAX_CONCURRENT_THREADS; ++i)
    {
        thread_tlabs_[i].start_ptr = nullptr;
        thread_tlabs_[i].end_ptr = nullptr;
        thread_tlabs_[i].current_offset_in_tlab.store(0, std::memory_order_relaxed);
    }
}

void CollectionRecordAllocator::allocate_new_tlab(size_t thread_id, size_t requested_record_size)
{
    if (thread_id >= num_threads_configured_for_db_)
    {
        throw std::out_of_range("Thread ID exceeds configured number of threads for CollectionRecordAllocator.");
    }

    size_t chunk_size = std::max(static_cast<size_t>(RECORD_ALLOCATOR_CHUNK_SIZE), requested_record_size);
    chunk_size = (chunk_size + OFFSET_GRANULARITY - 1) & ~(static_cast<size_t>(OFFSET_GRANULARITY - 1));

    uint64_t chunk_start_offset = allocate_data_chunk(chunk_size);

    ThreadLocalBuffer &tlab = thread_tlabs_[thread_id];
    tlab.start_ptr = mmap_base_addr_ + chunk_start_offset;
    tlab.end_ptr = tlab.start_ptr + chunk_size;
    tlab.current_offset_in_tlab.store(0, std::memory_order_relaxed);
}

void *CollectionRecordAllocator::reserve_record_space(size_t thread_id, size_t key_len, size_t value_len, uint32_t &out_record_rel_offset)
{
    if (key_len > MAX_KEY_VALUE_LENGTH || value_len > MAX_KEY_VALUE_LENGTH)
    {
        throw std::overflow_error("Key or value length exceeds maximum allowed (65535 bytes).");
    }

    if (thread_id >= num_threads_configured_for_db_)
    {
        throw std::out_of_range("Thread ID exceeds configured number of threads in CollectionRecordAllocator::reserve_record_space.");
    }

    const size_t total_record_size = get_allocated_record_size(key_len, value_len);

    for (int i = 0; i < 2; ++i)
    {
        ThreadLocalBuffer &tlab = thread_tlabs_[thread_id];

        if (tlab.start_ptr && (tlab.start_ptr + tlab.current_offset_in_tlab.load(std::memory_order_relaxed) + total_record_size <= tlab.end_ptr))
        {
            uint64_t allocated_offset_in_tlab = tlab.current_offset_in_tlab.fetch_add(total_record_size, std::memory_order_relaxed);
            uint64_t absolute_record_addr = reinterpret_cast<uint64_t>(tlab.start_ptr + allocated_offset_in_tlab);
            uint64_t byte_offset_from_base = absolute_record_addr - reinterpret_cast<uint64_t>(mmap_base_addr_);

            out_record_rel_offset = static_cast<uint32_t>(byte_offset_from_base / OFFSET_GRANULARITY);
            return reinterpret_cast<void *>(absolute_record_addr);
        }
        allocate_new_tlab(thread_id, total_record_size);
    }

    throw std::runtime_error("CollectionRecordAllocator: Persistent out of space after attempting to get a new chunk.");
}