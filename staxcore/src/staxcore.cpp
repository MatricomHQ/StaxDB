#include "staxcore.h"
#include "stax_internal.h"

#include <string>
#include <memory>
#include <new> // For placement new
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstdlib>
#include <algorithm> // For std::sort

// =================================================================================================
// --- StaxTree16 Method Implementations ---
// =================================================================================================
STAX_ALWAYS_INLINE bool StaxTree16::is_leaf(uint64_t ptr) { return (ptr & LEAF_TAG) != 0; }
STAX_ALWAYS_INLINE uint64_t StaxTree16::get_leaf_offset(uint64_t ptr) { return ptr & ~LEAF_TAG; }
STAX_ALWAYS_INLINE uint64_t StaxTree16::get_internal_offset(uint64_t ptr) { return ptr & OFFSET_MASK; }
STAX_ALWAYS_INLINE uint64_t StaxTree16::make_leaf_ptr(uint64_t offset) { return (offset & OFFSET_MASK) | LEAF_TAG; }
STAX_ALWAYS_INLINE uint32_t StaxTree16::get_test_idx(uint64_t ptr) { return ptr >> IDX_SHIFT; }
STAX_ALWAYS_INLINE uint64_t StaxTree16::make_internal_ptr(uint64_t offset, uint32_t idx) {
    if (idx >= (1 << 16)) throw std::runtime_error("Key too long");
    return (static_cast<uint64_t>(idx) << IDX_SHIFT) | (offset & OFFSET_MASK);
}
STAX_ALWAYS_INLINE int StaxTree16::get_child_idx(int nibble, uint32_t test_idx) const { return nibble ^ (test_idx & 0x0F); }
STAX_ALWAYS_INLINE int StaxTree16::get_nibble_from_child_idx(int child_idx, uint32_t test_idx) const { return child_idx ^ (test_idx & 0x0F); }
StaxTree16::StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref) : allocator_(allocator), root_ptr_(root_ref) {}
STAX_ALWAYS_INLINE int StaxTree16::get_nibble_at(std::string_view key, uint32_t nibble_idx) {
    const size_t byte_idx = nibble_idx >> 1;
    const bool is_in_bounds = byte_idx < key.length();
    const uint8_t byte = is_in_bounds ? key[byte_idx] : 0;
    const uint32_t shift_amount = (1 - (nibble_idx & 1)) * 4;
    return (byte >> shift_amount) & 0x0F;
}
int StaxTree16::find_first_differing_nibble(std::string_view k1, std::string_view k2) {
    const size_t len1 = k1.length();
    const size_t len2 = k2.length();
    const size_t min_len = std::min(len1, len2);
    size_t byte_idx = 0;

#if defined(WASM_BUILD)
    // Generic C++ implementation for WASM
#else
#if defined(__AVX512F__) && defined(__AVX512BW__)
    while (byte_idx + 64 <= min_len) {
        __m512i v1 = _mm512_loadu_si512(k1.data() + byte_idx);
        __m512i v2 = _mm512_loadu_si512(k2.data() + byte_idx);
        uint64_t mask = _mm512_cmpeq_epu8_mask(v1, v2);
        if (mask != 0xFFFFFFFFFFFFFFFF) {
            byte_idx += __builtin_ctzll(~mask);
            goto found_diff_byte;
        }
        byte_idx += 64;
    }
#elif defined(__AVX2__)
    while (byte_idx + 32 <= min_len) {
        __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(k1.data() + byte_idx));
        __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(k2.data() + byte_idx));
        int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2));
        if (static_cast<unsigned int>(mask) != 0xFFFFFFFF) {
            byte_idx += __builtin_ctz(~mask);
            goto found_diff_byte;
        }
        byte_idx += 32;
    }
#elif defined(__aarch64__)
    while (byte_idx + 16 <= min_len) {
        uint8x16_t v1 = vld1q_u8(reinterpret_cast<const uint8_t*>(k1.data() + byte_idx));
        uint8x16_t v2 = vld1q_u8(reinterpret_cast<const uint8_t*>(k2.data() + byte_idx));
        uint8x16_t equal_mask = vceqq_u8(v1, v2);
        uint64x2_t mask_parts = vreinterpretq_u64_u8(equal_mask);
        if ((vgetq_lane_u64(mask_parts, 0) & vgetq_lane_u64(mask_parts, 1)) != 0xFFFFFFFFFFFFFFFF) {
            break;
        }
        byte_idx += 16;
    }
#endif
#endif // WASM_BUILD

    while (byte_idx + 8 <= min_len) {
        uint64_t val1, val2;
        memcpy(&val1, k1.data() + byte_idx, 8);
        memcpy(&val2, k2.data() + byte_idx, 8);
        if (val1 != val2) break;
        byte_idx += 8;
    }
    for (; byte_idx < min_len; ++byte_idx) {
        if (k1[byte_idx] != k2[byte_idx]) goto found_diff_byte;
    }
    if (len1 != len2) return byte_idx * 2;
    return -1;
found_diff_byte:
    uint8_t b1 = k1[byte_idx];
    uint8_t b2 = k2[byte_idx];
    return (b1 >> 4) != (b2 >> 4) ? byte_idx * 2 : byte_idx * 2 + 1;
}

void StaxTree16::range_scan(const TxnContext& ctx, std::string_view start_key, std::string_view end_key, std::vector<StaxRecord*>& results) const {
    if (start_key > end_key) {
        return;
    }
    struct ScanFrame {
        uint64_t node_ptr;
        bool lower_bound_tight;
        bool upper_bound_tight;
    };
    constexpr int MAX_SCAN_DEPTH = 2048;
    ScanFrame stack[MAX_SCAN_DEPTH];
    int stack_top = -1;
    uint64_t root = root_ptr_.load(std::memory_order_acquire);
    if (root != 0) {
        assert(stack_top + 1 < MAX_SCAN_DEPTH);
        stack[++stack_top] = {root, true, true};
    }
    while (stack_top != -1) {
        ScanFrame frame = stack[stack_top--];
        if (is_leaf(frame.node_ptr)) {
            StaxRecord* record = get_visible_record(allocator_.get_ptr<StaxRecord>(get_leaf_offset(frame.node_ptr)), ctx);
            if (record) {
                std::string_view key = record->get_key();
                if ((!frame.lower_bound_tight || key >= start_key) && (!frame.upper_bound_tight || key <= end_key)) {
                    results.push_back(record);
                }
            }
            continue;
        }
        InternalNode* node = allocator_.get_ptr<InternalNode>(get_internal_offset(frame.node_ptr));
        uint32_t test_idx = get_test_idx(frame.node_ptr);
        int start_nibble = frame.lower_bound_tight ? get_nibble_at(start_key, test_idx) : 0;
        int end_nibble = frame.upper_bound_tight ? get_nibble_at(end_key, test_idx) : 15;
        for (int nibble = end_nibble; nibble >= start_nibble; --nibble) {
            uint64_t child_ptr = node->children[get_child_idx(nibble, test_idx)].load(std::memory_order_acquire);
            if (!child_ptr) {
                continue;
            }
            bool next_lower_tight = frame.lower_bound_tight && (nibble == start_nibble);
            bool next_upper_tight = frame.upper_bound_tight && (nibble == end_nibble);
            assert(stack_top + 1 < MAX_SCAN_DEPTH);
            stack[++stack_top] = {child_ptr, next_lower_tight, next_upper_tight};
        }
    }
}
void StaxTree16::insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete) {
    while (true) {
        std::atomic<uint64_t>* parent_ptr_loc = &root_ptr_;
        uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);

        while (current_ptr != 0 && !is_leaf(current_ptr)) {
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_internal_offset(current_ptr));
            uint32_t test_idx = get_test_idx(current_ptr);

            StaxRecord* rep_record = allocator_.get_ptr<StaxRecord>(node->representative_leaf_offset);
            std::string_view rep_key = rep_record->get_key();
            int d_idx = find_first_differing_nibble(key, rep_key);

            if (static_cast<uint32_t>(d_idx) < test_idx) {
                break;
            }
            int nibble = get_nibble_at(key, test_idx);
            parent_ptr_loc = &node->children[get_child_idx(nibble, test_idx)];
            current_ptr = parent_ptr_loc->load(std::memory_order_acquire);
        }

        uint64_t expected_ptr = current_ptr;

        if (current_ptr == 0) {
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
        } else if (is_leaf(current_ptr)) {
            uint64_t leaf_offset = get_leaf_offset(current_ptr);
            StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(leaf_offset);
            if (existing_record->get_key() == key) {
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, leaf_offset);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
            } else {
                std::string_view existing_key = existing_record->get_key();
                int d_idx = find_first_differing_nibble(key, existing_key);
                uint64_t new_internal_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
                InternalNode* new_node = new (allocator_.get_ptr<InternalNode>(new_internal_node_offset)) InternalNode();
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                int new_key_nibble = get_nibble_at(key, d_idx);
                int existing_key_nibble = get_nibble_at(existing_key, d_idx);
                new_node->children[get_child_idx(new_key_nibble, d_idx)].store(new_leaf_ptr, std::memory_order_relaxed);
                new_node->children[get_child_idx(existing_key_nibble, d_idx)].store(current_ptr, std::memory_order_relaxed);
                new_node->representative_leaf_offset = new_record_offset;
                uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
                if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
            }
        } else { // split internal node
            StaxRecord* rep_record = allocator_.get_ptr<StaxRecord>(allocator_.get_ptr<InternalNode>(get_internal_offset(current_ptr))->representative_leaf_offset);
            std::string_view rep_key = rep_record->get_key();
            int d_idx = find_first_differing_nibble(key, rep_key);

            uint64_t new_internal_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
            InternalNode* new_node = new (allocator_.get_ptr<InternalNode>(new_internal_node_offset)) InternalNode();
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            int new_key_nibble = get_nibble_at(key, d_idx);
            int existing_key_nibble = get_nibble_at(rep_key, d_idx);
            new_node->children[get_child_idx(new_key_nibble, d_idx)].store(new_leaf_ptr, std::memory_order_relaxed);
            new_node->children[get_child_idx(existing_key_nibble, d_idx)].store(current_ptr, std::memory_order_relaxed);
            new_node->representative_leaf_offset = new_record_offset;
            uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
            if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
        }
        STAX_PAUSE();
    }
}
uint64_t StaxTree16::allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
    size_t total_size = sizeof(StaxRecord) + key.length() + value.length();
    uint64_t offset = local_alloc.allocate(total_size);
    StaxRecord* new_rec = allocator_.get_ptr<StaxRecord>(offset);
    new_rec->key_len = key.length(); new_rec->value_len = value.length(); new_rec->txn_id = ctx.txn_id;
    new_rec->prev_version_offset = prev_version_offset; new_rec->is_deleted = is_delete;
    memcpy(new_rec->get_key_data(), key.data(), key.length());
    if(!value.empty()) memcpy(new_rec->get_value_data(), value.data(), value.length());
    return offset;
}
void StaxTree16::remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
    insert(local_alloc, ctx, key, "", true);
}
StaxRecord* StaxTree16::get(const TxnContext &ctx, std::string_view key, size_t key_len) const {
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    while (current_ptr != 0) {
        if (is_leaf(current_ptr)) {
            StaxRecord* record_head = allocator_.get_ptr<StaxRecord>(get_leaf_offset(current_ptr));
            if (record_head->get_key() == key) {
                return get_visible_record(record_head, ctx);
            }
            return nullptr;
        } else {
            uint32_t test_idx = get_test_idx(current_ptr);
            int nibble = get_nibble_at(key, test_idx);
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_internal_offset(current_ptr));
            current_ptr = node->children[get_child_idx(nibble, test_idx)].load(std::memory_order_acquire);
        }
    }
    return nullptr;
}

// --- Internal C++ Implementation ---

class StaxDBImpl {
public:
    StaxDBImpl(const stax_config_t& config) : config_(config), next_txn_id_(1) {}

    ~StaxDBImpl() {
        if (mmap_ptr_) {
            if (config_.storage_type == STAX_STORE_IN_MEMORY) {
                free(mmap_ptr_);
            } else {
                munmap(mmap_ptr_, config_.db_size);
            }
        }
        if (fd_ != -1) {
            close(fd_);
        }
    }

    stax_status_t open() {
        try {
            if (config_.storage_type == STAX_STORE_FILE) {
                int flags = O_RDWR | O_CREAT;
                fd_ = ::open(config_.filepath, flags, S_IRUSR | S_IWUSR);
                if (fd_ == -1) return STAX_ERROR_FILE_OPEN_FAILED;
                if (::ftruncate(fd_, config_.db_size) == -1) return STAX_ERROR_FILE_TRUNCATE_FAILED;
                int prot = PROT_READ | (config_.read_only ? 0 : PROT_WRITE);
                mmap_ptr_ = ::mmap(nullptr, config_.db_size, prot, MAP_SHARED, fd_, 0);
                if (mmap_ptr_ == MAP_FAILED) return STAX_ERROR_MMAP_FAILED;
            } else if (config_.storage_type == STAX_STORE_ANONYMOUS) {
                int prot = PROT_READ | (config_.read_only ? 0 : PROT_WRITE);
                mmap_ptr_ = ::mmap(nullptr, config_.db_size, prot, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
                if (mmap_ptr_ == MAP_FAILED) return STAX_ERROR_MMAP_FAILED;
            } else {
                mmap_ptr_ = malloc(config_.db_size);
                if (!mmap_ptr_) return STAX_ERROR_OUT_OF_MEMORY;
            }

            bool is_new_db = true; // Assuming new DB for simplicity.
            file_header_ = new (mmap_ptr_) FileHeader();
            if (is_new_db) {
                file_header_->global_alloc_offset.store(sizeof(FileHeader), std::memory_order_relaxed);
                file_header_->root_ptr.store(0, std::memory_order_relaxed);
            }

            global_allocator_ = std::make_unique<StaxAllocator>(file_header_, static_cast<uint8_t*>(mmap_ptr_));
            local_allocator_ = std::make_unique<ThreadLocalAllocator>(*global_allocator_);
            tree_ = std::make_unique<StaxTree16>(*global_allocator_, file_header_->root_ptr);

        } catch (const std::exception&) {
            return STAX_ERROR_GENERIC;
        }
        return STAX_OK;
    }

    stax_status_t put(const stax_slice_t* key, const stax_slice_t* value) {
        if (config_.read_only) return STAX_ERROR_READ_ONLY;
        try {
            tree_->insert(*local_allocator_, get_txn_context(),
                std::string_view(static_cast<const char*>(key->data), key->size),
                std::string_view(static_cast<const char*>(value->data), value->size), false);
        } catch (...) { return STAX_ERROR_GENERIC; }
        return STAX_OK;
    }

    stax_status_t get(const stax_slice_t* key, stax_slice_t* value) {
        try {
            StaxRecord* record = tree_->get(get_read_txn_context(),
                std::string_view(static_cast<const char*>(key->data), key->size), key->size);

            if (!record) {
                value->data = nullptr;
                value->size = 0;
                return STAX_ERROR_NOT_FOUND;
            }
            void* val_buf = malloc(record->value_len);
            if (!val_buf) return STAX_ERROR_OUT_OF_MEMORY;
            memcpy(val_buf, record->get_value_data(), record->value_len);
            value->data = val_buf;
            value->size = record->value_len;
        } catch (...) { return STAX_ERROR_GENERIC; }
        return STAX_OK;
    }

    stax_status_t del(const stax_slice_t* key) {
        if (config_.read_only) return STAX_ERROR_READ_ONLY;
        try {
            tree_->insert(*local_allocator_, get_txn_context(),
                std::string_view(static_cast<const char*>(key->data), key->size), "", true);
        } catch (...) { return STAX_ERROR_GENERIC; }
        return STAX_OK;
    }

    StaxTree16* get_tree() { return tree_.get(); }
    stax_config_t get_config() const { return config_; }
    TxnContext get_read_txn_context() {
        return {0, std::numeric_limits<uint64_t>::max(), 0};
    }

    void get_all_leaf_heads(std::vector<StaxRecord*>& results) {
        struct ScanFrame { uint64_t node_ptr; };
        std::vector<ScanFrame> stack;
        uint64_t root = file_header_->root_ptr.load(std::memory_order_acquire);
        if (root != 0) stack.push_back({root});
        while (!stack.empty()) {
            ScanFrame frame = stack.back();
            stack.pop_back();
            if (StaxTree16::is_leaf(frame.node_ptr)) {
                results.push_back(global_allocator_->get_ptr<StaxRecord>(StaxTree16::get_leaf_offset(frame.node_ptr)));
                continue;
            }
            InternalNode* node = global_allocator_->get_ptr<InternalNode>(StaxTree16::get_internal_offset(frame.node_ptr));
            for (int i = 0; i < 16; ++i) {
                uint64_t child_ptr = node->children[i].load(std::memory_order_acquire);
                if (child_ptr) stack.push_back({child_ptr});
            }
        }
    }

private:
    TxnContext get_txn_context() {
        return {0, std::numeric_limits<uint64_t>::max(), next_txn_id_.fetch_add(1)};
    }
    stax_config_t config_;
    int fd_ = -1;
    void* mmap_ptr_ = nullptr;
    FileHeader* file_header_ = nullptr;
    std::unique_ptr<StaxAllocator> global_allocator_;
    std::unique_ptr<ThreadLocalAllocator> local_allocator_;
    std::unique_ptr<StaxTree16> tree_;
    std::atomic<TxnID> next_txn_id_;
};

class StaxCursorImpl {
public:
    StaxCursorImpl(StaxDBImpl* db_impl) : db_impl_(db_impl), current_pos_(0) {}
    void scan(std::string_view start_key, std::string_view end_key) {
        records_.clear();
        current_pos_ = 0;
        db_impl_->get_tree()->range_scan(db_impl_->get_read_txn_context(), start_key, end_key, records_);
    }
    void seek_first() { scan("", std::string(256, '\xFF')); }
    void seek(std::string_view key) { scan(key, std::string(256, '\xFF')); }
    bool is_valid() const { return current_pos_ < records_.size(); }
    void next() { if (is_valid()) current_pos_++; }
    void get(stax_slice_t* key, stax_slice_t* value) {
        if (!is_valid()) {
            key->data = nullptr; key->size = 0;
            value->data = nullptr; value->size = 0;
            return;
        }
        StaxRecord* record = records_[current_pos_];
        key->data = record->get_key_data();
        key->size = record->key_len;
        value->data = record->get_value_data();
        value->size = record->value_len;
    }
private:
    StaxDBImpl* db_impl_;
    std::vector<StaxRecord*> records_;
    size_t current_pos_;
};

struct stax_db_t { StaxDBImpl* impl; };
struct stax_cursor_t { StaxCursorImpl* impl; };

extern "C" {

const char* stax_strerror(stax_status_t status) {
    switch (status) {
        case STAX_OK: return "Success";
        case STAX_ERROR_NOT_FOUND: return "Not found";
        case STAX_ERROR_GENERIC: return "A generic, unspecified error occurred";
        case STAX_ERROR_INVALID_ARG: return "Invalid argument provided to function";
        case STAX_ERROR_OUT_OF_MEMORY: return "Could not allocate memory";
        case STAX_ERROR_READ_ONLY: return "Database is in read-only mode";
        case STAX_ERROR_MMAP_FAILED: return "Memory mapping failed";
        case STAX_ERROR_FILE_OPEN_FAILED: return "Could not open file";
        case STAX_ERROR_FILE_TRUNCATE_FAILED: return "Could not truncate file";
        case STAX_ERROR_FILE_RENAME_FAILED: return "Could not rename file";
        case STAX_ERROR_DB_OPEN_FAILED: return "Failed to open database handle";
        case STAX_ERROR_COMPACTION_FAILED: return "Database compaction failed";
        case STAX_ERROR_FLATTEN_FAILED: return "Database flatten failed";
        case STAX_ERROR_NOT_APPLICABLE: return "Operation not applicable in the current mode";
        default: return "Unknown error code";
    }
}

stax_config_t stax_get_default_config() {
    stax_config_t config;
#if defined(WASM_BUILD)
    config.storage_type = STAX_STORE_IN_MEMORY;
#else
    config.storage_type = STAX_STORE_FILE;
#endif
    config.filepath = "./stax.db";
    config.db_size = 10ULL * 1024 * 1024 * 1024;
    config.thread_arena_size = 32 * 1024;
    config.read_only = false;
    return config;
}

stax_status_t stax_db_open(const stax_config_t* config, stax_db_t** db) {
    if (!config || !db) return STAX_ERROR_INVALID_ARG;
    stax_db_t* db_handle = (stax_db_t*)malloc(sizeof(stax_db_t));
    if (!db_handle) return STAX_ERROR_OUT_OF_MEMORY;
    db_handle->impl = new (std::nothrow) StaxDBImpl(*config);
    if (!db_handle->impl) {
        free(db_handle);
        return STAX_ERROR_OUT_OF_MEMORY;
    }
    stax_status_t rc = db_handle->impl->open();
    if (rc != STAX_OK) {
        delete db_handle->impl;
        free(db_handle);
        return rc;
    }
    *db = db_handle;
    return STAX_OK;
}

void stax_db_close(stax_db_t* db) {
    if (db) {
        delete db->impl;
        free(db);
    }
}

stax_status_t stax_put(stax_db_t* db, const stax_slice_t* key, const stax_slice_t* value) {
    if (!db || !db->impl) return STAX_ERROR_INVALID_ARG;
    return db->impl->put(key, value);
}

stax_status_t stax_get(stax_db_t* db, const stax_slice_t* key, stax_slice_t* value) {
    if (!db || !db->impl || !value) return STAX_ERROR_INVALID_ARG;
    return db->impl->get(key, value);
}

stax_status_t stax_delete(stax_db_t* db, const stax_slice_t* key) {
    if (!db || !db->impl) return STAX_ERROR_INVALID_ARG;
    return db->impl->del(key);
}

void stax_free_slice(stax_slice_t* value) {
    if (value && value->data) {
        free((void*)value->data);
        value->data = nullptr;
        value->size = 0;
    }
}

stax_status_t stax_compact(stax_db_t* db) {
    if (!db || !db->impl) return STAX_ERROR_INVALID_ARG;
    stax_config_t config = db->impl->get_config();
    if (config.storage_type != STAX_STORE_FILE) return STAX_ERROR_NOT_APPLICABLE;
    std::string temp_path = std::string(config.filepath) + ".compact";
    stax_config_t new_config = config;
    new_config.filepath = temp_path.c_str();
    stax_db_t* new_db = nullptr;
    stax_status_t rc = stax_db_open(&new_config, &new_db);
    if (rc != STAX_OK) return STAX_ERROR_COMPACTION_FAILED;
    stax_cursor_t* cursor = nullptr;
    rc = stax_cursor_create(db, &cursor);
    if (rc != STAX_OK) {
        stax_db_close(new_db);
        unlink(temp_path.c_str());
        return STAX_ERROR_COMPACTION_FAILED;
    }
    for (stax_cursor_seek_first(cursor); stax_cursor_valid(cursor); stax_cursor_next(cursor)) {
        stax_slice_t key, value;
        stax_cursor_get(cursor, &key, &value);
        stax_put(new_db, &key, &value);
    }
    stax_cursor_destroy(cursor);
    std::string old_path = config.filepath;
    stax_db_close(new_db);
    if (rename(temp_path.c_str(), old_path.c_str()) != 0) {
        unlink(temp_path.c_str());
        return STAX_ERROR_FILE_RENAME_FAILED;
    }
    return STAX_OK;
}

stax_status_t stax_flatten(stax_db_t* db) {
    // NOTE: In this implementation, flatten is identical to compact.
    // A true flatten would require traversing version chains, but the current
    // range_scan only returns the latest visible version of a key,
    // which is what compact also does.
    return stax_compact(db);
}

stax_status_t stax_cursor_create(stax_db_t* db, stax_cursor_t** cursor) {
    if (!db || !db->impl || !cursor) return STAX_ERROR_INVALID_ARG;
    stax_cursor_t* c = (stax_cursor_t*)malloc(sizeof(stax_cursor_t));
    if (!c) return STAX_ERROR_OUT_OF_MEMORY;
    c->impl = new (std::nothrow) StaxCursorImpl(db->impl);
    if (!c->impl) {
        free(c);
        return STAX_ERROR_OUT_OF_MEMORY;
    }
    *cursor = c;
    return STAX_OK;
}

void stax_cursor_destroy(stax_cursor_t* cursor) {
    if (cursor) {
        delete cursor->impl;
        free(cursor);
    }
}

bool stax_cursor_valid(stax_cursor_t* cursor) {
    if (!cursor || !cursor->impl) return false;
    return cursor->impl->is_valid();
}

void stax_cursor_seek_first(stax_cursor_t* cursor) {
    if (!cursor || !cursor->impl) return;
    cursor->impl->seek_first();
}

void stax_cursor_seek(stax_cursor_t* cursor, const stax_slice_t* key) {
    if (!cursor || !cursor->impl) return;
    cursor->impl->seek(std::string_view(static_cast<const char*>(key->data), key->size));
}

void stax_cursor_next(stax_cursor_t* cursor) {
    if (!cursor || !cursor->impl) return;
    cursor->impl->next();
}

void stax_cursor_get(stax_cursor_t* cursor, stax_slice_t* key, stax_slice_t* value) {
    if (!cursor || !cursor->impl) return;
    cursor->impl->get(key, value);
}

} // extern "C"
