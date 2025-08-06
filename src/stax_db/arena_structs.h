#pragma once

#include <cstdint>
#include <atomic>
#include <type_traits>

#include "stax_common/common_types.hpp"
#include "stax_common/constants.h"

#pragma pack(push, 8)

struct StaxTreeNode
{

    uint16_t bit_index;

    uint8_t padding[6];

    std::atomic<uint64_t> left_child_ptr;
    std::atomic<uint64_t> right_child_ptr;
};
static_assert(sizeof(StaxTreeNode) == 24, "StaxTreeNode must be 24 bytes for cache alignment.");
static_assert(alignof(StaxTreeNode) == 8, "StaxTreeNode must be 8-byte aligned.");

struct CollectionEntry
{
    std::atomic<uint64_t> root_node_ptr;
    std::atomic<uint64_t> logical_item_count;
    std::atomic<uint64_t> live_record_bytes;
    uint32_t name_hash;
    std::atomic<uint32_t> object_id_counter;
};
static_assert(sizeof(CollectionEntry) == 32, "CollectionEntry must be 32 bytes");

struct FileHeader
{
    uint64_t magic;
    uint16_t version;
    uint16_t reserved_padding_1;
    uint64_t file_size;

    std::atomic<uint64_t> global_alloc_offset;

    std::atomic<TxnID> last_committed_txn_id;

    uint64_t collection_array_offset;
    std::atomic<uint32_t> collection_array_count;
    uint32_t collection_array_capacity;

    uint64_t reserved_pointers[9];

    uint8_t final_padding_bytes[8060];
};
static_assert(sizeof(FileHeader) == 8192, "FileHeader must be 8192 bytes");
static_assert(std::is_standard_layout<FileHeader>::value, "FileHeader must be standard layout");

#pragma pack(pop)