#pragma once

#include <atomic>
#include <vector>
#include <string_view>
#include "stax_tx/transaction.h"
#include <array>

// Maximum dimensions supported by the spatial index.
constexpr size_t STAX_MAX_DIMENSIONS = 128;

// This file contains the core data structures for the StaxDB PATRICIA trie.
// It is included by both the main tree implementation and the dimensional cursor,
// breaking the circular dependency between them.

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
};

struct InternalNode {
    std::atomic<uint64_t> children[16];
    uint32_t key_len;
    uint32_t _padding; // Align to 8 bytes

    InternalNode(uint32_t k_len) : key_len(k_len), _padding(0) {
        for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_release);
    }

    char* get_key_data() { return reinterpret_cast<char*>(this) + sizeof(InternalNode); }
    const char* get_key_data() const { return reinterpret_cast<const char*>(this) + sizeof(InternalNode); }
    std::string_view get_key() const { return std::string_view(get_key_data(), key_len); }
};

struct StaxPath {
    struct Frame {
        uint64_t node_ptr;
        int nibble_in_parent;
    };
    std::vector<Frame> frames;
    uint64_t leaf_handle = 0;
};

struct AABB {
    std::array<uint64_t, STAX_MAX_DIMENSIONS> min_bounds;
    std::array<uint64_t, STAX_MAX_DIMENSIONS> max_bounds;
    uint32_t D;

    AABB(uint32_t dimensions) : D(dimensions) {
        if (D > STAX_MAX_DIMENSIONS) {
            throw std::runtime_error("Dimensionality exceeds compile-time STAX_MAX_DIMENSIONS.");
        }
        min_bounds.fill(0);
        max_bounds.fill(UINT64_MAX);
    }
};

// A struct to hold statistics about a query's execution.
struct QueryStats {
    long long nodes_visited = 0;
    long long leaves_visited = 0;
    long long records_loaded = 0;
    long long records_scanned = 0;
    long long records_accepted = 0;
};
