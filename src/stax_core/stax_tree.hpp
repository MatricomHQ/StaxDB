#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <vector>
#include <stack>
#include <memory>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#include "stax_core/node_allocator.hpp" // Now contains StaxAllocator
#include "stax_common/common_types.hpp"
#include "stax_tx/transaction.h"
#include "stax_common/binary_utils.h"

#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif

// =================================================================================================
// --- Node and Record Structures ---
// =================================================================================================

struct StaxRecord {
    uint32_t key_len;
    uint32_t value_len;
    TxnID txn_id;
    uint64_t prev_version_offset;
    bool is_deleted;

    char* get_key_data();
    const char* get_key_data() const;
    char* get_value_data();
    const char* get_value_data() const;
    std::string_view get_key() const;
};

struct InternalNode {
    uint32_t test_nibble_idx;
    std::atomic<uint64_t> children[16];

    InternalNode();
};


// =================================================================================================
// --- StaxTree16 (Niblet Tree Implementation) ---
// =================================================================================================

class StaxTree16 {
private:
    StaxAllocator &allocator_;
    std::atomic<uint64_t> &root_ptr_;

    static constexpr uint64_t LEAF_TAG = 1;
    static constexpr uint64_t POINTER_MASK = ~LEAF_TAG;

    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t get_offset(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t record_byte_offset);
    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx);

    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset);
    static int find_first_differing_nibble(std::string_view k1, std::string_view k2);

public:
    StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref);

    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);
    StaxRecord* get(const TxnContext &ctx, std::string_view key) const;
    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key);

    std::vector<StaxRecord*> range(const TxnContext &ctx, std::string_view prefix) const;
    std::vector<StaxRecord*> range(const TxnContext &ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts) const;
};