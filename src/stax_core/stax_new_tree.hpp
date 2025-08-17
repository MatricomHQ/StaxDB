#pragma once

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <stack>
#include <cstring>
#include <optional>
#include <algorithm>
#include <utility>

#include "stax_common/constants.h"
#include "stax_tx/transaction.h"
#include "stax_db/arena_structs.h"

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#include <immintrin.h>
#if defined(_MSC_VER)
#include <intrin.h> // For _BitScanForward
#endif
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif

// =================================================================================================
// --- StaxAllocator (Unified mmap Allocator) ---
// =================================================================================================
class StaxAllocator {
private:
    FileHeader* file_header_ = nullptr;
    uint8_t* mmap_base_addr_ = nullptr;

public:
    StaxAllocator(FileHeader* file_header, uint8_t* mmap_base_addr)
        : file_header_(file_header), mmap_base_addr_(mmap_base_addr) {}

    uint64_t allocate(size_t size, size_t alignment = 8) {
        if (!file_header_) {
            throw std::runtime_error("Cannot allocate chunk: file header is null.");
        }
        if ((alignment & (alignment - 1)) != 0) {
            throw std::invalid_argument("Alignment must be a power of two.");
        }
        const uint64_t alignment_mask = alignment - 1;
        uint64_t current_offset = file_header_->global_alloc_offset.load(std::memory_order_relaxed);
        while (true) {
            uint64_t aligned_offset = (current_offset + alignment_mask) & ~alignment_mask;
            uint64_t next_offset = aligned_offset + size;
            if (next_offset > DB_MAX_VIRTUAL_SIZE) {
                throw std::runtime_error("Database out of space.");
            }
            if (file_header_->global_alloc_offset.compare_exchange_weak(current_offset, next_offset, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return aligned_offset;
            }
        }
    }

    void deallocate(uint64_t offset, size_t size) {
        // No-op
    }

    template<typename T>
    T* get_ptr(uint64_t offset) const {
        if (offset == 0) return nullptr;
        return reinterpret_cast<T*>(mmap_base_addr_ + offset);
    }
};

// --- ThreadLocalAllocator ---
class ThreadLocalAllocator {
private:
    StaxAllocator& global_allocator_;
    uint64_t arena_offset_ = 0;
    uint64_t current_alloc_ptr_ = 0;
    uint64_t arena_end_ptr_ = 0;

    static constexpr size_t ARENA_SIZE = 64 * 1024; // 64KB arenas

    void request_new_arena() {
        arena_offset_ = global_allocator_.allocate(ARENA_SIZE, ARENA_SIZE); // Align arenas
        current_alloc_ptr_ = arena_offset_;
        arena_end_ptr_ = arena_offset_ + ARENA_SIZE;
    }

public:
    ThreadLocalAllocator(StaxAllocator& global_allocator) : global_allocator_(global_allocator) {
        request_new_arena();
    }

    uint64_t allocate(size_t size, size_t alignment = 8) {
        const uint64_t alignment_mask = alignment - 1;
        uint64_t aligned_ptr = (current_alloc_ptr_ + alignment_mask) & ~alignment_mask;

        if (aligned_ptr + size > arena_end_ptr_) {
            if (size > ARENA_SIZE) {
                return global_allocator_.allocate(size, alignment);
            }
            request_new_arena();
            aligned_ptr = (current_alloc_ptr_ + alignment_mask) & ~alignment_mask;
            if (aligned_ptr + size > arena_end_ptr_) {
                return global_allocator_.allocate(size, alignment);
            }
        }

        current_alloc_ptr_ = aligned_ptr + size;
        return aligned_ptr;
    }
};

// =================================================================================================
// --- Node and Record Structures ---
// =================================================================================================
struct StaxRecord {
    uint32_t key_len;
    uint32_t value_len;
    TxnID txn_id;
    uint64_t prev_version_offset;
    bool is_deleted;

    char* get_key_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord); }
    const char* get_key_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord); }
    char* get_value_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord) + key_len; }
    const char* get_value_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord) + key_len; }
    std::string_view get_key() const { return std::string_view(get_key_data(), key_len); }
    std::string_view get_value() const { return std::string_view(get_value_data(), value_len); }
};

struct InternalNode {
    uint64_t representative_leaf_offset;
    std::atomic<uint64_t> children[16];

    InternalNode() : representative_leaf_offset(0) {
        for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_release);
    }
};

// =================================================================================================
// --- StaxTree16 (Niblet Tree Implementation) ---
// =================================================================================================
class StaxTree16 {
private:
    StaxAllocator &allocator_;
    std::atomic<uint64_t> &root_ptr_;

    // Pointer Encoding Details (Original, Simple Version)
    static constexpr uint64_t LEAF_TAG = 1ULL;
    static constexpr uint64_t OFFSET_MASK = (1ULL << 48) - 1;
    static constexpr int IDX_SHIFT = 48;

public:
    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr) { return (ptr & LEAF_TAG) != 0; }
    static STAX_ALWAYS_INLINE uint64_t get_offset(uint64_t ptr) { return is_leaf(ptr) ? (ptr & ~LEAF_TAG) : (ptr & OFFSET_MASK); }
    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t offset) { return (offset & OFFSET_MASK) | LEAF_TAG; }
    static STAX_ALWAYS_INLINE uint32_t get_test_idx(uint64_t ptr) { return ptr >> IDX_SHIFT; }
    static STAX_ALWAYS_INLINE uint64_t make_internal_ptr(uint64_t offset, uint32_t idx) {
        if (idx >= (1 << 16)) throw std::runtime_error("Key too long");
        return (static_cast<uint64_t>(idx) << IDX_SHIFT) | (offset & OFFSET_MASK);
    }
    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx) {
        size_t byte_idx = nibble_idx >> 1;
        if (byte_idx >= key.length()) return 0;
        uint8_t byte = key[byte_idx];
        return (nibble_idx & 1) == 0 ? (byte >> 4) & 0x0F : byte & 0x0F;
    }


public:
    StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref);
    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);
    void insert_batch(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, const CoreKVPair* kv_pairs, size_t num_kvs, TransactionBatch& batch) {
        for (size_t i = 0; i < num_kvs; ++i) {
            insert(local_alloc, ctx, kv_pairs[i].key, kv_pairs[i].value, false);
        }
    }
    std::optional<RecordData> get(const TxnContext &ctx, std::string_view key) const;
    void multi_get_simd(const TxnContext& ctx, const std::vector<std::string_view>& keys, std::vector<std::optional<RecordData>>& results) const {
        results.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results[i] = get(ctx, keys[i]);
        }
    }
    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key);
    void range_scan(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key, std::vector<StaxRecord*>& results) const;
    StaxAllocator& get_allocator() { return allocator_; }
    std::atomic<uint64_t>& get_root_ptr() { return root_ptr_; }


public:
    StaxRecord* get_visible_record(StaxRecord* record_head, const TxnContext& ctx) const;
private:
    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset);
    static int find_first_differing_nibble(std::string_view k1, std::string_view k2);
};

// =================================================================================================
// --- StaxTree16 Method Implementations ---
// =================================================================================================

inline int StaxTree16::find_first_differing_nibble(std::string_view k1, std::string_view k2) {
    size_t len1 = k1.length();
    size_t len2 = k2.length();
    size_t min_len = std::min(len1, len2);
    size_t byte_idx = 0;

    // Compare 8 bytes at a time for as long as we can.
    while (byte_idx + 8 <= min_len) {
        uint64_t val1, val2;
        memcpy(&val1, k1.data() + byte_idx, 8);
        memcpy(&val2, k2.data() + byte_idx, 8);
        if (val1 != val2) {
            break; // Found a difference in this 8-byte chunk.
        }
        byte_idx += 8;
    }

    // Find the exact differing byte.
    for (; byte_idx < min_len; ++byte_idx) {
        if (k1[byte_idx] != k2[byte_idx]) {
            uint8_t b1 = k1[byte_idx];
            uint8_t b2 = k2[byte_idx];
            return (b1 >> 4) != (b2 >> 4) ? byte_idx * 2 : byte_idx * 2 + 1;
        }
    }

    // If one key is a prefix of the other.
    if (len1 != len2) {
        return byte_idx * 2;
    }

    return -1; // Keys are identical
}

inline StaxTree16::StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref)
    : allocator_(allocator), root_ptr_(root_ref) {}

inline void StaxTree16::insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete) {
restart_operation:
    std::atomic<uint64_t>* parent_ptr_loc = &root_ptr_;
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);

    while (true) {
        if (current_ptr == 0) {
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            uint64_t expected = 0;
            if (parent_ptr_loc->compare_exchange_strong(expected, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
            goto restart_operation;
        }

        if (is_leaf(current_ptr)) {
            uint64_t leaf_offset = get_offset(current_ptr);
            StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(leaf_offset);
            std::string_view existing_key = existing_record->get_key();

            if (existing_key == key) {
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, leaf_offset);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                if (parent_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
                goto restart_operation;
            }

            int d_idx = find_first_differing_nibble(key, existing_key);
            uint64_t new_internal_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
            InternalNode* new_node = allocator_.get_ptr<InternalNode>(new_internal_node_offset);
            new (new_node) InternalNode();

            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);

            int new_key_nibble = get_nibble_at(key, d_idx);
            int existing_key_nibble = get_nibble_at(existing_key, d_idx);

            new_node->children[new_key_nibble].store(new_leaf_ptr, std::memory_order_relaxed);
            new_node->children[existing_key_nibble].store(current_ptr, std::memory_order_relaxed);
            new_node->representative_leaf_offset = get_offset(new_leaf_ptr);

            uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
            if (parent_ptr_loc->compare_exchange_strong(current_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
            goto restart_operation;
        }

        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
        StaxRecord* rep_record = allocator_.get_ptr<StaxRecord>(node->representative_leaf_offset);
        std::string_view rep_key = rep_record->get_key();

        int d_idx = find_first_differing_nibble(key, rep_key);
        uint32_t test_idx = get_test_idx(current_ptr);

        if (d_idx < test_idx) {
            uint64_t new_internal_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
            InternalNode* new_node = allocator_.get_ptr<InternalNode>(new_internal_node_offset);
            new (new_node) InternalNode();

            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);

            int new_key_nibble = get_nibble_at(key, d_idx);
            int existing_key_nibble = get_nibble_at(rep_key, d_idx);

            new_node->children[new_key_nibble].store(new_leaf_ptr, std::memory_order_relaxed);
            new_node->children[existing_key_nibble].store(current_ptr, std::memory_order_relaxed);
            new_node->representative_leaf_offset = get_offset(new_leaf_ptr);

            uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
            if (parent_ptr_loc->compare_exchange_strong(current_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
            goto restart_operation;
        }

        int nibble = get_nibble_at(key, test_idx);
        parent_ptr_loc = &node->children[nibble];
        current_ptr = parent_ptr_loc->load(std::memory_order_acquire);
    }
}


inline uint64_t StaxTree16::allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
    size_t total_size = sizeof(StaxRecord) + key.length() + value.length();
    uint64_t offset = local_alloc.allocate(total_size);
    StaxRecord* new_rec = allocator_.get_ptr<StaxRecord>(offset);
    new_rec->key_len = key.length();
    new_rec->value_len = value.length();
    new_rec->txn_id = ctx.txn_id;
    new_rec->prev_version_offset = prev_version_offset;
    new_rec->is_deleted = is_delete;
    memcpy(new_rec->get_key_data(), key.data(), key.length());
    if(!value.empty()) memcpy(new_rec->get_value_data(), value.data(), value.length());
    return offset;
}

inline void StaxTree16::remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
    insert(local_alloc, ctx, key, "", true);
}

inline StaxRecord* StaxTree16::get_visible_record(StaxRecord* record_head, const TxnContext& ctx) const {
    StaxRecord* current_rec = record_head;
    while (current_rec != nullptr) {
        if (current_rec->txn_id <= ctx.read_snapshot_id) {
            if (current_rec->is_deleted) {
                return nullptr;
            }
            return current_rec;
        }
        current_rec = allocator_.get_ptr<StaxRecord>(current_rec->prev_version_offset);
    }
    return nullptr;
}

inline std::optional<RecordData> StaxTree16::get(const TxnContext &ctx, std::string_view key) const {
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    while (current_ptr != 0) {
        if (is_leaf(current_ptr)) {
            StaxRecord* record_head = allocator_.get_ptr<StaxRecord>(get_offset(current_ptr));
            if (record_head->get_key() == key) {
                StaxRecord* visible_record = get_visible_record(record_head, ctx);
                if (visible_record) {
                    return RecordData{
                        visible_record->get_key_data(),
                        visible_record->key_len,
                        visible_record->get_value_data(),
                        visible_record->value_len,
                        visible_record->is_deleted
                    };
                }
            }
            return std::nullopt;
        } else {
            uint32_t test_idx = get_test_idx(current_ptr);
            int nibble = get_nibble_at(key, test_idx);
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            current_ptr = node->children[nibble].load(std::memory_order_acquire);
        }
    }
    return std::nullopt;
}

inline void StaxTree16::range_scan(const TxnContext &ctx, std::string_view start_key, std::optional<std::string_view> end_key, std::vector<StaxRecord*>& results) const {
    if (end_key && start_key > *end_key) return;

    struct ScanState {
        uint64_t node_ptr;
        bool has_lower_bound;
        bool has_upper_bound;
    };

    std::vector<ScanState> stack;
    uint64_t root_ptr = root_ptr_.load(std::memory_order_acquire);
    if (root_ptr != 0) {
        stack.push_back({root_ptr, true, end_key.has_value()});
    }

    while(!stack.empty()) {
        ScanState current_state = stack.back();
        stack.pop_back();

        uint64_t node_ptr = current_state.node_ptr;
        bool has_lower_bound = current_state.has_lower_bound;
        bool has_upper_bound = current_state.has_upper_bound;

        if (is_leaf(node_ptr)) {
            StaxRecord* record_head = allocator_.get_ptr<StaxRecord>(get_offset(node_ptr));
            StaxRecord* visible_record = get_visible_record(record_head, ctx);
            if (visible_record) {
                std::string_view key = visible_record->get_key();
                if ((!has_lower_bound || key >= start_key) && (!has_upper_bound || key <= *end_key)) {
                    results.push_back(visible_record);
                }
            }
            continue;
        }

        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(node_ptr));
        uint32_t test_idx = get_test_idx(node_ptr);

        int start_nibble = has_lower_bound ? get_nibble_at(start_key, test_idx) : 0;
        int end_nibble = has_upper_bound ? get_nibble_at(*end_key, test_idx) : 15;

        for (int i = end_nibble; i >= start_nibble; --i) {
            uint64_t child_ptr = node->children[i].load(std::memory_order_acquire);
            if (!child_ptr) continue;

            bool new_has_lower_bound = has_lower_bound && (i == start_nibble);
            bool new_has_upper_bound = has_upper_bound && (i == end_nibble);

            stack.push_back({child_ptr, new_has_lower_bound, new_has_upper_bound});
        }
    }
}
