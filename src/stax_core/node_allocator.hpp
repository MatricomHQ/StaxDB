
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
#include "stax_db/arena_structs.h" 


class Database; 


#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif


template <typename T> 
class NodeAllocator {
private:
    Database* parent_db_ = nullptr;
    uint8_t* mmap_base_addr_ = nullptr;

    
    struct ThreadLocalChunk {
        uint8_t* start_ptr = nullptr; 
        size_t size_bytes = 0;
        std::atomic<uint64_t> current_offset; 
    };
    std::array<ThreadLocalChunk, MAX_CONCURRENT_THREADS> thread_chunks_;

    
    void request_new_chunk(size_t thread_id);
    
    
    static constexpr size_t align_up(size_t size, size_t alignment) {
        return (size + alignment - 1) & ~(alignment - 1);
    }
    
public:
    static thread_local std::vector<uint64_t> thread_local_free_list; 
    static constexpr uint64_t NIL_INDEX = std::numeric_limits<uint64_t>::max(); 
    
    
    NodeAllocator(Database* parent_db, uint8_t* mmap_base_addr);
    
    
    uint64_t allocate(size_t thread_id); 
    
    void deallocate(uint64_t node_byte_offset); 
    
    
    STAX_ALWAYS_INLINE uint16_t get_bit_index(uint64_t node_byte_offset) const { 
        return reinterpret_cast<StaxTreeNode*>(mmap_base_addr_ + node_byte_offset)->bit_index;
    }
    STAX_ALWAYS_INLINE void set_bit_index(uint64_t node_byte_offset, uint32_t val) { 
        reinterpret_cast<StaxTreeNode*>(mmap_base_addr_ + node_byte_offset)->bit_index = static_cast<uint16_t>(val);
    }
    STAX_ALWAYS_INLINE std::atomic<uint64_t>& get_left_child_ptr(uint64_t node_byte_offset) { 
        return reinterpret_cast<StaxTreeNode*>(mmap_base_addr_ + node_byte_offset)->left_child_ptr;
    }
    STAX_ALWAYS_INLINE std::atomic<uint64_t>& get_right_child_ptr(uint64_t node_byte_offset) { 
        return reinterpret_cast<StaxTreeNode*>(mmap_base_addr_ + node_byte_offset)->right_child_ptr;
    }
    STAX_ALWAYS_INLINE const uint16_t* get_bit_index_ptr(uint64_t node_byte_offset) const { 
        return &(reinterpret_cast<const StaxTreeNode*>(mmap_base_addr_ + node_byte_offset)->bit_index);
    }

    size_t get_total_occupied_size() const; 
};

template <typename T>
thread_local std::vector<uint64_t> NodeAllocator<T>::thread_local_free_list;