#pragma once

#include <cstdint>
#include <atomic>
#include <stdexcept>
#include <new>
#include <limits>
#include <vector>
#include <array>
#include <string>

#include "stax_common/constants.h"
#include "stax_common/common_types.hpp"

class Database;

#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif

class NodeAllocator
{
private:
    Database *parent_db_ = nullptr;
    uint8_t *mmap_base_addr_ = nullptr;

    static constexpr size_t NODES_PER_CHUNK = 512;
    static constexpr size_t CHUNK_ALIGNMENT = 16384;

    static constexpr size_t BIT_INDEX_ARRAY_BYTES = NODES_PER_CHUNK * sizeof(uint16_t);
    static constexpr size_t CHILD_PTR_ARRAY_BYTES = NODES_PER_CHUNK * sizeof(uint64_t);

    static constexpr size_t LEFT_CHILD_PTR_ARRAY_OFFSET = BIT_INDEX_ARRAY_BYTES;
    static constexpr size_t RIGHT_CHILD_PTR_ARRAY_OFFSET = LEFT_CHILD_PTR_ARRAY_OFFSET + CHILD_PTR_ARRAY_BYTES;
    static constexpr size_t CHUNK_USED_BYTES = RIGHT_CHILD_PTR_ARRAY_OFFSET + CHILD_PTR_ARRAY_BYTES;

    struct ThreadLocalChunk
    {
        uint64_t chunk_base_offset_from_mmap = 0;
        std::atomic<uint32_t> current_offset_nodes;
    };
    std::array<ThreadLocalChunk, MAX_CONCURRENT_THREADS> thread_chunks_;

    void request_new_chunk(size_t thread_id);

public:
    static thread_local std::vector<uint64_t> thread_local_free_list;
    static constexpr uint64_t NIL_INDEX = std::numeric_limits<uint64_t>::max();

    NodeAllocator(Database *parent_db, uint8_t *mmap_base_addr);

    uint64_t allocate(size_t thread_id);
    void deallocate(uint64_t node_handle);

    STAX_ALWAYS_INLINE uint16_t get_bit_index(uint64_t node_handle) const
    {
        const uint64_t chunk_base_offset = node_handle & ~(CHUNK_ALIGNMENT - 1);
        const uint64_t offset_in_chunk = node_handle & (CHUNK_ALIGNMENT - 1);
        const uint32_t node_index = static_cast<uint32_t>((offset_in_chunk - LEFT_CHILD_PTR_ARRAY_OFFSET) >> 3);
        const uint64_t bit_index_offset = chunk_base_offset + (node_index * sizeof(uint16_t));
        return *reinterpret_cast<uint16_t *>(mmap_base_addr_ + bit_index_offset);
    }

    STAX_ALWAYS_INLINE void set_bit_index(uint64_t node_handle, uint32_t val)
    {
        const uint64_t chunk_base_offset = node_handle & ~(CHUNK_ALIGNMENT - 1);
        const uint64_t offset_in_chunk = node_handle & (CHUNK_ALIGNMENT - 1);
        const uint32_t node_index = static_cast<uint32_t>((offset_in_chunk - LEFT_CHILD_PTR_ARRAY_OFFSET) >> 3);
        const uint64_t bit_index_offset = chunk_base_offset + (node_index * sizeof(uint16_t));
        *reinterpret_cast<uint16_t *>(mmap_base_addr_ + bit_index_offset) = static_cast<uint16_t>(val);
    }

    STAX_ALWAYS_INLINE std::atomic<uint64_t> &get_left_child_ptr(uint64_t node_handle)
    {
        return *reinterpret_cast<std::atomic<uint64_t> *>(mmap_base_addr_ + node_handle);
    }

    STAX_ALWAYS_INLINE std::atomic<uint64_t> &get_right_child_ptr(uint64_t node_handle)
    {
        return *reinterpret_cast<std::atomic<uint64_t> *>(mmap_base_addr_ + node_handle + CHILD_PTR_ARRAY_BYTES);
    }

    STAX_ALWAYS_INLINE const uint16_t *get_bit_index_ptr(uint64_t node_handle) const
    {
        const uint64_t chunk_base_offset = node_handle & ~(CHUNK_ALIGNMENT - 1);
        const uint64_t offset_in_chunk = node_handle & (CHUNK_ALIGNMENT - 1);
        const uint32_t node_index = static_cast<uint32_t>((offset_in_chunk - LEFT_CHILD_PTR_ARRAY_OFFSET) >> 3);
        const uint64_t bit_index_offset = chunk_base_offset + (node_index * sizeof(uint16_t));
        return reinterpret_cast<const uint16_t *>(mmap_base_addr_ + bit_index_offset);
    }

    size_t get_total_occupied_size() const;
};