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

#include "stax_core/node_allocator.hpp"
#include "stax_core/value_store.hpp"
#include "stax_common/common_types.hpp"
#include "stax_common/constants.h"
#include "stax_tx/transaction.h"

constexpr uint64_t POINTER_TAG_BIT = 1ULL << 63;
constexpr uint64_t POINTER_INDEX_MASK = ~(POINTER_TAG_BIT);
constexpr uint64_t NIL_POINTER = 0;

#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif


class StaxTree
{
private:
    friend class DBCursor;

    NodeAllocator &internal_node_allocator_;
    CollectionRecordAllocator &record_allocator_;
    std::atomic<uint64_t> &root_ptr_;

    struct TraversalStep
    {
        uint64_t parent_node_idx;
        uint64_t child_ptr;
    };
    static_assert(sizeof(TraversalStep) == 16, "Compact TraversalStep must be 16 bytes");
    static constexpr uint64_t PARENT_IS_ROOT = (std::numeric_limits<uint64_t>::max)();

    class PathBuffer
    {
    private:
        std::unique_ptr<TraversalStep[]> buffer_;
        size_t capacity_;
        size_t size_;
        static constexpr size_t INITIAL_CAPACITY = 64;

    public:
        PathBuffer() : capacity_(INITIAL_CAPACITY), size_(0) { buffer_ = std::make_unique<TraversalStep[]>(INITIAL_CAPACITY); }
        PathBuffer(const PathBuffer &) = delete;
        PathBuffer &operator=(const PathBuffer &) = delete;

        TraversalStep *begin() { return buffer_.get(); }
        TraversalStep *end() { return buffer_.get() + size_; }

        bool push(const TraversalStep &step)
        {
            if (size_ >= capacity_)
            {
                grow();
                if (size_ >= capacity_)
                    return false;
            }
            buffer_[size_++] = step;
            return true;
        }
        void grow()
        {
            size_t new_capacity = capacity_ * 2;
            std::unique_ptr<TraversalStep[]> new_buffer = std::make_unique<TraversalStep[]>(new_capacity);
            memcpy(new_buffer.get(), buffer_.get(), size_ * sizeof(TraversalStep));
            buffer_ = std::move(new_buffer);
            capacity_ = new_capacity;
        }
        void reset() { size_ = 0; }
        size_t size() const { return size_; }
        TraversalStep &operator[](size_t index) { return buffer_[index]; }
        TraversalStep &back() { return buffer_[size_ - 1]; }
    };

    uint32_t find_critical_bit(const char *s1_data, size_t len1, const char *s2_data, size_t len2) const;
    std::atomic<uint64_t> *get_link_from_step(const TraversalStep &step);
    void find_leaf_nodes_recursive(uint64_t current_ptr, std::string_view prefix, std::vector<uint64_t> &leaf_nodes) const;
    
    static STAX_ALWAYS_INLINE bool get_bit(const char *s_data, size_t s_len, uint32_t bit_index) {
        size_t byte_idx = bit_index / 8;
        if (byte_idx >= s_len)
            return false;
        uint8_t mask = 0x80 >> (bit_index % 8);
        return (s_data[byte_idx] & mask) != 0;
    }

    static STAX_ALWAYS_INLINE int count_leading_zeros(uint32_t x) {
    #if defined(_MSC_VER)
        unsigned long index;
        if (_BitScanReverse(&index, x))
            return 31 - index;
        return 32;
    #else
        return __builtin_clz(x);
    #endif
    }

    static STAX_ALWAYS_INLINE int simd_memcmp(const char *s1, const char *s2, size_t n) {
        size_t offset = 0;

    #if defined(__AVX2__)
        for (; offset + 32 <= n; offset += 32)
        {
            __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s1 + offset));
            __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + offset));
            int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2));
            if (mask != (int)0xFFFFFFFF)
            {
                offset += _tzcnt_u32(~mask);
                return static_cast<int>(static_cast<unsigned char>(s1[offset])) -
                       static_cast<int>(static_cast<unsigned char>(s2[offset]));
            }
        }
    #endif
    #if defined(__SSE2__) || defined(_M_X64) || _M_IX86_FP >= 2
        for (; offset + 16 <= n; offset += 16)
        {
            __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(s1 + offset));
            __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(s2 + offset));
            int mask = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, v2));
            if (mask != 0xFFFF)
            {
    #if defined(_MSC_VER)
                unsigned long index;
                _BitScanForward(&index, ~mask);
                offset += index;
    #else
                offset += __builtin_ctz(~mask);
    #endif
                return static_cast<int>(static_cast<unsigned char>(s1[offset])) -
                       static_cast<int>(static_cast<unsigned char>(s2[offset]));
            }
        }
    #elif defined(__aarch64__)
        for (; offset + 16 <= n; offset += 16)
        {
            uint8x16_t v1 = vld1q_u8(reinterpret_cast<const uint8_t *>(s1 + offset));
            uint8x16_t v2 = vld1q_u8(reinterpret_cast<const uint8_t *>(s2 + offset));
            uint8x16_t v_eq = vceqq_u8(v1, v2);

            if (vgetq_lane_u64(vreinterpretq_u64_u8(v_eq), 0) != 0xFFFFFFFFFFFFFFFF ||
                vgetq_lane_u64(vreinterpretq_u64_u8(v_eq), 1) != 0xFFFFFFFFFFFFFFFF)
            {

                for (size_t i = 0; i < 16; ++i)
                {
                    if (s1[offset + i] != s2[offset + i])
                    {
                        return static_cast<int>(static_cast<unsigned char>(s1[offset + i])) -
                               static_cast<int>(static_cast<unsigned char>(s2[offset + i]));
                    }
                }
            }
        }
    #endif

        for (; offset < n; ++offset)
        {
            if (s1[offset] != s2[offset])
            {
                return static_cast<int>(static_cast<unsigned char>(s1[offset])) -
                       static_cast<int>(static_cast<unsigned char>(s2[offset]));
            }
        }
        return 0;
    }

public:
    StaxTree(NodeAllocator &internal_alloc,
             CollectionRecordAllocator &record_alloc,
             std::atomic<uint64_t> &root_ref);

    void insert(const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);

    void insert_batch(const TxnContext &ctx, const CoreKVPair *kv_pairs, size_t num_kvs, TransactionBatch &batch);
    std::optional<RecordData> get(const TxnContext &ctx, std::string_view key) const;
    void remove(const TxnContext &ctx, std::string_view key);
    void seek(std::string_view start_key, std::stack<uint64_t, std::vector<uint64_t>> &path_stack) const;
    void find_leaf_nodes_in_range(std::string_view prefix, std::vector<uint64_t> &leaf_nodes) const;
    void multi_get_simd(const TxnContext &ctx, const std::vector<std::string_view> &keys, std::vector<std::optional<RecordData>> &results) const;

    RecordData get_record_data_by_offset(uint32_t rel_offset) const
    {
        return record_allocator_.get_record_data(rel_offset);
    }
};