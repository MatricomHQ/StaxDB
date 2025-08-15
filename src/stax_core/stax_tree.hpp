#pragma once

#include <functional>
#include <vector>
#include <optional>
#include <algorithm>
#include <new>
#include <string_view>
#include <atomic>
#include <stack>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <limits>
#include <memory>
#include <iterator>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#include "stax_core/stax_new_tree.hpp"
#include "stax_common/common_types.hpp"
#include "stax_tx/transaction.h"

// Temporarily defining RecordData here to remove dependency on value_store.hpp
struct RecordData
{
    const char *key_ptr;
    size_t key_len;
    const char *value_ptr;
    size_t value_len;
    TxnID txn_id;
    uint64_t prev_version_offset; // Matched to StaxRecord's type
    bool is_deleted;

    RecordData() noexcept : key_ptr(nullptr), key_len(0), value_ptr(nullptr), value_len(0),
                            txn_id(0), prev_version_offset(0), is_deleted(false) {}

    std::string_view key_view() const noexcept { return std::string_view(key_ptr, key_len); }
    std::string_view value_view() const noexcept { return std::string_view(value_ptr, value_len); }
};


class StaxTree {
private:
    StaxAllocator &allocator_;
    std::atomic<uint64_t> &root_ptr_;

public:
    StaxTree(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref);

    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);
    std::optional<RecordData> get(const TxnContext &ctx, std::string_view key) const;
    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key);

    class Cursor {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = RecordData;
        using difference_type = std::ptrdiff_t;
        using pointer = const RecordData*;
        using reference = const RecordData&;

        Cursor& operator++();
        reference operator*() const;
        pointer operator->() const;
        bool operator!=(const Cursor& other) const;
        bool operator==(const Cursor& other) const;

        Cursor& begin();
        Cursor end();

    private:
        friend class StaxTree;
        Cursor(const StaxTree* tree, const TxnContext& ctx, std::string_view prefix, bool is_end = false);
        Cursor(const StaxTree* tree, const TxnContext& ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts, bool is_end = false);

        void advance();
        StaxRecord* get_visible_record(uint64_t head_record_offset) const;
        bool check_timestamp(std::string_view key) const;

        const StaxTree* tree_;
        TxnContext ctx_;
        std::string_view prefix_;
        std::optional<uint64_t> start_ts_;
        std::optional<uint64_t> end_ts_;

        std::stack<uint64_t> to_visit_;
        std::optional<RecordData> current_record_;
        bool is_end_sentinel_ = false;
    };

    Cursor range(const TxnContext& ctx, std::string_view prefix) const;
    Cursor range(const TxnContext& ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts) const;

    // Stubs for API compatibility
    void insert_batch(const TxnContext &ctx, const CoreKVPair *kv_pairs, size_t num_kvs, TransactionBatch &batch) {
        throw std::runtime_error("insert_batch not implemented in new tree");
    }
    void multi_get_simd(const TxnContext &ctx, const std::vector<std::string_view> &keys, std::vector<std::optional<RecordData>> &results) const {
         results.reserve(keys.size());
         for (const auto& key : keys) {
            results.push_back(get(ctx, key));
        }
    }

private:
    StaxRecord* get_internal(const TxnContext &ctx, std::string_view key) const;

    static constexpr uint64_t LEAF_TAG = 1;
    static constexpr uint64_t POINTER_MASK = ~LEAF_TAG;

    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr) { return (ptr & LEAF_TAG) != 0; }
    static STAX_ALWAYS_INLINE uint64_t get_offset(uint64_t ptr) { return ptr & POINTER_MASK; }
    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t record_byte_offset) { return record_byte_offset | LEAF_TAG; }

    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx) {
        size_t byte_idx = nibble_idx >> 1; // Faster division by 2
        if (byte_idx >= key.length()) return 0;
        uint8_t byte = key[byte_idx];
        return (nibble_idx & 1) == 0 ? (byte >> 4) & 0x0F : byte & 0x0F; // Faster modulo 2
    }
    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset);
    static int find_first_differing_nibble(std::string_view k1, std::string_view k2);
};