#pragma once

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "stax_common/constants.h"
#include "stax_tx/transaction.h"
#include "stax_db/arena_structs.h"

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
    uint32_t test_nibble_idx;
    std::atomic<uint64_t> children[16];

    InternalNode() : test_nibble_idx(0) {
        for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_relaxed);
    }
};
