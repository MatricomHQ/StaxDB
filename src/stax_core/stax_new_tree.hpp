#pragma once

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <stack>
#include <cstring>

#include "stax_common/constants.h"
#include "stax_tx/transaction.h"
#include "stax_db/arena_structs.h"

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
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
    uint8_t *mmap_base_addr_ = nullptr;

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
        // No-op in this model
    }

    template<typename T>
    T* get_ptr(uint64_t offset) const {
        if (offset == 0) return nullptr;
        return reinterpret_cast<T*>(mmap_base_addr_ + offset);
    }
};

// =================================================================================================
// --- ThreadLocalAllocator ---
// =================================================================================================
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
            if (size > ARENA_SIZE) { // Large allocation
                return global_allocator_.allocate(size, alignment);
            }
            request_new_arena();
            aligned_ptr = (current_alloc_ptr_ + alignment_mask) & ~alignment_mask;
            if (aligned_ptr + size > arena_end_ptr_) {
                // This could happen if a small allocation doesn't fit in a fresh arena (due to alignment)
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
    // For alignment purposes, add padding
    char padding[7];


    char* get_key_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord); }
    const char* get_key_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord); }
    char* get_value_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord) + key_len; }
    const char* get_value_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord) + key_len; }
    std::string_view get_key() const { return std::string_view(get_key_data(), key_len); }
    std::string_view get_value() const { return std::string_view(get_value_data(), value_len); }
};

struct InternalNode {
    // test_nibble_idx is now encoded in the parent's pointer to this node.
    std::atomic<uint64_t> children[16];

    InternalNode() {
        for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_relaxed);
    }
};

// =================================================================================================
// --- Utility Functions ---
// =================================================================================================

// Converts a uint64_t to a big-endian byte string for lexicographical ordering.
inline std::string uint64_to_big_endian_str(uint64_t val) {
    std::string s(8, '\0');
    s[0] = (val >> 56) & 0xFF;
    s[1] = (val >> 48) & 0xFF;
    s[2] = (val >> 40) & 0xFF;
    s[3] = (val >> 32) & 0xFF;
    s[4] = (val >> 24) & 0xFF;
    s[5] = (val >> 16) & 0xFF;
    s[6] = (val >> 8) & 0xFF;
    s[7] = val & 0xFF;
    return s;
}

// Converts a big-endian byte string view back to a uint64_t.
inline uint64_t big_endian_str_to_uint64(std::string_view s) {
    if (s.length() != 8) {
        throw std::invalid_argument("String view must be 8 bytes long for uint64_t conversion.");
    }
    uint64_t val = 0;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[0])) << 56;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[1])) << 48;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[2])) << 40;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[3])) << 32;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[4])) << 24;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[5])) << 16;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[6])) << 8;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[7]));
    return val;
}

// =================================================================================================
// --- StaxTree16 (Niblet Tree Implementation) ---
// =================================================================================================
class StaxTree16
{
private:
    StaxAllocator &allocator_; // Still needed for get_ptr
    std::atomic<uint64_t> &root_ptr_;

public:
    StaxTree16(StaxAllocator &allocator,
             std::atomic<uint64_t> &root_ref)
    : allocator_(allocator),
      root_ptr_(root_ref) {}

    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false) {
        std::atomic<uint64_t>* current_ptr_loc = &root_ptr_;

        while(true) {
            uint64_t current_ptr = current_ptr_loc->load(std::memory_order_acquire);

            if (current_ptr == 0) {
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
                // No deallocate, let the arena allocator handle it
                continue;
            }

            if (is_leaf(current_ptr)) {
                uint64_t existing_record_offset = get_offset(current_ptr);
                StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(existing_record_offset);
                std::string_view existing_key = existing_record->get_key();

                if (existing_key == key) {
                    uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, existing_record_offset);
                    uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                     if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                        return;
                    }
                    continue;
                }

                int split_idx = find_first_differing_nibble(key, existing_key);

                uint64_t new_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
                InternalNode* new_node = allocator_.get_ptr<InternalNode>(new_node_offset);
                new (new_node) InternalNode();

                int nibble_new = get_nibble_at(key, split_idx);
                int nibble_old = get_nibble_at(existing_key, split_idx);

                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
                new_node->children[nibble_new].store(make_leaf_ptr(new_record_offset), std::memory_order_relaxed);
                new_node->children[nibble_old].store(current_ptr, std::memory_order_relaxed);

                uint64_t new_node_ptr = make_internal_ptr(new_node_offset, split_idx);
                if (current_ptr_loc->compare_exchange_strong(current_ptr, new_node_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }

            uint32_t test_idx = get_test_idx(current_ptr);
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            int nibble = get_nibble_at(key, test_idx);
            current_ptr_loc = &node->children[nibble];
        }
    }

    std::optional<RecordData> get(const TxnContext &ctx, std::string_view key) const {
        uint64_t current_ptr = root_ptr_.load(std::memory_order_relaxed);

        while (current_ptr != 0 && !is_leaf(current_ptr)) {
            uint32_t test_idx = get_test_idx(current_ptr);
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            int nibble = get_nibble_at(key, test_idx);
            current_ptr = node->children[nibble].load(std::memory_order_relaxed);
            #if defined(__x86_64__) || defined(__i386__)
                if (current_ptr != 0) _mm_prefetch(allocator_.get_ptr<void>(get_offset(current_ptr)), _MM_HINT_T0);
            #elif defined(__aarch64__)
                if (current_ptr != 0) __builtin_prefetch(allocator_.get_ptr<void>(get_offset(current_ptr)), 0, 0);
            #endif
        }

        if (current_ptr == 0) return std::nullopt;

        uint64_t record_offset = get_offset(current_ptr);
        StaxRecord* head_record = allocator_.get_ptr<StaxRecord>(record_offset);

        if (head_record == nullptr || head_record->get_key() != key) return std::nullopt;

        uint64_t current_version_offset = record_offset;
        while (current_version_offset != 0) {
            StaxRecord* record = allocator_.get_ptr<StaxRecord>(current_version_offset);
            if (record->txn_id <= ctx.read_snapshot_id) {
                if (record->is_deleted) return std::nullopt;
                return RecordData{
                    record->get_key_data(),
                    record->key_len,
                    record->get_value_data(),
                    record->value_len,
                    record->is_deleted
                };
            }
            current_version_offset = record->prev_version_offset;
        }
        return std::nullopt;
    }

    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
        insert(local_alloc, ctx, key, "", true);
    }

    void insert_batch(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, const CoreKVPair* kv_pairs, size_t num_kvs, TransactionBatch& batch) {
        for (size_t i = 0; i < num_kvs; ++i) {
            insert(local_alloc, ctx, kv_pairs[i].key, kv_pairs[i].value, false);
        }
    }

    void multi_get_simd(const TxnContext& ctx, const std::vector<std::string_view>& keys, std::vector<std::optional<RecordData>>& results) const {
        results.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results[i] = get(ctx, keys[i]);
        }
    }

public:
    // A pointer is either a leaf or an internal node.
    // We use the LSB to distinguish: 1 for leaf, 0 for internal.
    // For internal nodes, we encode the test_nibble_idx in the high bits.
    // This removes a memory fetch during traversal.
    static constexpr uint64_t LEAF_TAG = 1ULL;
    // We reserve 48 bits for the offset, which supports up to 256TB.
    static constexpr uint64_t OFFSET_MASK = (1ULL << 48) - 1;
    // We use the next 16 bits for the test_nibble_idx.
    static constexpr int IDX_SHIFT = 48;

    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr) { return (ptr & LEAF_TAG) != 0; }

    static STAX_ALWAYS_INLINE uint64_t get_offset(uint64_t ptr) {
        // This conditional will be compiled to an efficient cmov instruction.
        return is_leaf(ptr) ? (ptr & ~LEAF_TAG) : (ptr & OFFSET_MASK);
    }

    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t record_byte_offset) {
        // Ensure we don't have garbage in the high bits, then set the tag.
        return (record_byte_offset & OFFSET_MASK) | LEAF_TAG;
    }

    static STAX_ALWAYS_INLINE uint32_t get_test_idx(uint64_t ptr) {
        // This is only valid for internal node pointers.
        return ptr >> IDX_SHIFT;
    }

    static STAX_ALWAYS_INLINE uint64_t make_internal_ptr(uint64_t node_offset, uint32_t test_idx) {
        if (test_idx >= (1 << 16)) {
            // Keys longer than 32KB are not supported by this optimization.
            throw std::runtime_error("Key is too long to encode test_idx in pointer.");
        }
        // Ensure offset is clean, then OR in the test_idx at its shifted position.
        return (node_offset & OFFSET_MASK) | (static_cast<uint64_t>(test_idx) << IDX_SHIFT);
    }

    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx) {
        size_t byte_idx = nibble_idx >> 1; // Faster division by 2
        if (byte_idx >= key.length()) return 0;
        uint8_t byte = key[byte_idx];
        return (nibble_idx & 1) == 0 ? (byte >> 4) & 0x0F : byte & 0x0F; // Faster modulo 2
    }

    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
        size_t total_size = sizeof(StaxRecord) + key.length() + value.length();
        uint64_t offset = local_alloc.allocate(total_size, alignof(StaxRecord));

        StaxRecord* record = allocator_.get_ptr<StaxRecord>(offset);
        new (record) StaxRecord();
        record->key_len = key.length();
        record->value_len = value.length();
        record->txn_id = ctx.txn_id;
        record->prev_version_offset = prev_version_offset;
        record->is_deleted = is_delete;
        memcpy(record->get_key_data(), key.data(), key.length());
        memcpy(record->get_value_data(), value.data(), value.length());
        return offset;
    }

    static int find_first_differing_nibble(std::string_view k1, std::string_view k2) {
        const size_t len1 = k1.length();
        const size_t len2 = k2.length();
        const size_t min_len = std::min(len1, len2);
        size_t i = 0;
        if (min_len >= 8) {
            const uint64_t* p1 = reinterpret_cast<const uint64_t*>(k1.data());
            const uint64_t* p2 = reinterpret_cast<const uint64_t*>(k2.data());
            size_t limit = min_len / 8;
            for (; i < limit; ++i) {
                if (p1[i] != p2[i]) {
                    uint64_t xor_chunk = p1[i] ^ p2[i];
                    #if defined(_MSC_VER)
                        unsigned long differing_bit_idx;
                        _BitScanForward64(&differing_bit_idx, xor_chunk);
                    #else
                        long long differing_bit_idx = __builtin_ctzll(xor_chunk);
                    #endif
                    size_t byte_idx = i * 8 + (differing_bit_idx / 8);
                    uint8_t xor_val = k1[byte_idx] ^ k2[byte_idx];
                    return byte_idx * 2 + ((xor_val & 0xF0) == 0);
                }
            }
            i *= 8;
        }
        for (; i < min_len; ++i) {
            if (k1[i] != k2[i]) {
                uint8_t xor_val = k1[i] ^ k2[i];
                return i * 2 + ((xor_val & 0xF0) == 0);
            }
        }
        return (len1 == len2) ? -1 : min_len * 2;
    }

    StaxAllocator& get_allocator() { return allocator_; }
    std::atomic<uint64_t>& get_root_ptr() { return root_ptr_; }
};
