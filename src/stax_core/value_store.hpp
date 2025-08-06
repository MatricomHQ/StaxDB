#pragma once

#include <cstdint>
#include <atomic>
#include <stdexcept>
#include <optional>
#include <string_view>
#include <cstring>
#include <new>
#include <array>
#include <string>  
#include <vector>  
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


struct RecordData
{
    const char *key_ptr;
    size_t key_len;
    const char *value_ptr;
    size_t value_len;
    TxnID txn_id;
    uint32_t prev_version_rel_offset;
    bool is_deleted;

    RecordData() noexcept : key_ptr(nullptr), key_len(0), value_ptr(nullptr), value_len(0),
                            txn_id(0), prev_version_rel_offset(0), is_deleted(false) {}

    std::string_view key_view() const noexcept { return std::string_view(key_ptr, key_len); }
    std::string_view value_view() const noexcept { return std::string_view(value_ptr, value_len); }
};

class CollectionRecordAllocator
{
private:
    friend class StaxTree;
    
    Database* parent_db_ = nullptr;
    uint8_t* mmap_base_addr_ = nullptr;
    
    struct ThreadLocalBuffer
    {
        uint8_t *start_ptr = nullptr;
        uint8_t *end_ptr = nullptr;
        std::atomic<uint64_t> current_offset_in_tlab;
    };

    std::array<ThreadLocalBuffer, MAX_CONCURRENT_THREADS> thread_tlabs_;
    size_t num_threads_configured_for_db_;

    static constexpr uint32_t OFFSET_GRANULARITY = 8;
    
    static constexpr size_t FIXED_HEADER_SIZE = 24;
    static_assert(FIXED_HEADER_SIZE % OFFSET_GRANULARITY == 0, "Fixed header size must be a multiple of granularity for alignment.");
    
    void allocate_new_tlab(size_t thread_id, size_t requested_record_size);

public:
    static constexpr uint32_t NIL_RECORD_OFFSET = 0;
    static constexpr size_t HEADER_SIZE = FIXED_HEADER_SIZE;
    static constexpr uint32_t MAX_KEY_VALUE_LENGTH = std::numeric_limits<uint32_t>::max();
    static constexpr uint8_t FLAG_DELETED = 0x01;
    
    CollectionRecordAllocator(Database* parent_db, uint8_t* mmap_base_addr, size_t num_threads_configured_for_db) noexcept;

    static size_t get_allocated_record_size(size_t key_len, size_t value_len) noexcept {
        const size_t record_payload_size = key_len + value_len;
        return (HEADER_SIZE + record_payload_size + (OFFSET_GRANULARITY - 1)) & ~(static_cast<size_t>(OFFSET_GRANULARITY - 1));
    }

    void *reserve_record_space(size_t thread_id, size_t key_len, size_t value_len, uint32_t &out_record_rel_offset);

    
    STAX_ALWAYS_INLINE void finalize_record_header_and_data(void *record_base_ptr, size_t key_len, size_t value_len, bool is_delete, TxnID txn_id, uint32_t prev_version_rel_offset, const char *key_data, const char *value_data) noexcept {
        char* current_ptr = reinterpret_cast<char*>(record_base_ptr);
        
        *reinterpret_cast<uint32_t *>(current_ptr) = static_cast<uint32_t>(key_len);
        current_ptr += 4;
        
        *reinterpret_cast<uint32_t *>(current_ptr) = static_cast<uint32_t>(value_len);
        current_ptr += 4;
        
        *reinterpret_cast<uint8_t *>(current_ptr) = is_delete ? FLAG_DELETED : 0;
        current_ptr += 4; 
        
        *reinterpret_cast<TxnID *>(current_ptr) = txn_id;
        current_ptr += 8;

        *reinterpret_cast<uint32_t *>(current_ptr) = prev_version_rel_offset;
        
        char *payload_start = reinterpret_cast<char *>(record_base_ptr) + HEADER_SIZE;
        if (key_len > 0) memcpy(payload_start, key_data, key_len);
        if (value_len > 0) memcpy(payload_start + key_len, value_data, value_len);
    }

    STAX_ALWAYS_INLINE void get_record_key_and_lengths(uint32_t rel_offset, const char **out_key_ptr, uint32_t &out_key_len, uint32_t &out_value_len) const noexcept {
        uint64_t byte_offset = static_cast<uint64_t>(rel_offset) * OFFSET_GRANULARITY;
        if (rel_offset == NIL_RECORD_OFFSET) { *out_key_ptr = nullptr; out_key_len = 0; out_value_len = 0; return; }

        const char *record_base_ptr = reinterpret_cast<const char*>(mmap_base_addr_) + byte_offset;
        out_key_len = *reinterpret_cast<const uint32_t *>(record_base_ptr);
        out_value_len = *reinterpret_cast<const uint32_t *>(record_base_ptr + 4);
        *out_key_ptr = record_base_ptr + HEADER_SIZE;
    }
    
    STAX_ALWAYS_INLINE std::string_view get_record_key_only(uint32_t rel_offset) const noexcept {
        uint64_t byte_offset = static_cast<uint64_t>(rel_offset) * OFFSET_GRANULARITY;
        if (rel_offset == NIL_RECORD_OFFSET) { return {}; }
        const char *record_base_ptr = reinterpret_cast<const char*>(mmap_base_addr_) + byte_offset;
        uint32_t key_len = *reinterpret_cast<const uint32_t *>(record_base_ptr);
        return std::string_view(record_base_ptr + HEADER_SIZE, key_len);
    }


    STAX_ALWAYS_INLINE RecordData get_record_data(uint32_t rel_offset) const noexcept {
        uint64_t byte_offset = static_cast<uint64_t>(rel_offset) * OFFSET_GRANULARITY;
        if (rel_offset == NIL_RECORD_OFFSET) return RecordData{};

        const char *record_base_ptr = reinterpret_cast<const char*>(mmap_base_addr_) + byte_offset;
        
        RecordData record_data;
        record_data.key_len = *reinterpret_cast<const uint32_t *>(record_base_ptr);
        record_data.value_len = *reinterpret_cast<const uint32_t *>(record_base_ptr + 4);
        uint8_t flags = *reinterpret_cast<const uint8_t *>(record_base_ptr + 8);
        record_data.txn_id = *reinterpret_cast<const TxnID *>(record_base_ptr + 12);
        record_data.prev_version_rel_offset = *reinterpret_cast<const uint32_t *>(record_base_ptr + 20);
        
        record_data.key_ptr = record_base_ptr + HEADER_SIZE;
        record_data.value_ptr = record_base_ptr + HEADER_SIZE + record_data.key_len;
        record_data.is_deleted = (flags & FLAG_DELETED) != 0;
        
        return record_data;
    }
    
    STAX_ALWAYS_INLINE void *get_record_address(uint32_t relative_offset) const noexcept {
        uint64_t byte_offset = static_cast<uint64_t>(relative_offset) * OFFSET_GRANULARITY;
        return static_cast<void *>(mmap_base_addr_ + byte_offset);
    }
};