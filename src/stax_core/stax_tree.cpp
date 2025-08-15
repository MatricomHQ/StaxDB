#include "stax_core/stax_tree.hpp"
#include <new>

StaxTree::StaxTree(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref)
    : allocator_(allocator), root_ptr_(root_ref) {}

void StaxTree::insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete) {
    std::atomic<uint64_t>* current_ptr_loc = &root_ptr_;

    while(true) {
        uint64_t current_ptr = current_ptr_loc->load(std::memory_order_acquire);

        if (current_ptr == 0) { // Empty slot, try to insert a new leaf
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return; // Success
            }
            // Lost the race, another thread inserted. The allocated record is leaked, but handled by the arena.
            continue; // Retry from the same spot
        }

        if (is_leaf(current_ptr)) {
            uint64_t existing_record_offset = get_offset(current_ptr);
            StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(existing_record_offset);
            std::string_view existing_key = existing_record->get_key();

            if (existing_key == key) { // Key match, create a new version
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, existing_record_offset);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                 if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return; // Success
                }
                // Lost the race, retry
                continue;
            }

            // Key mismatch, need to split the leaf into an internal node
            int split_idx = find_first_differing_nibble(key, existing_key);

            uint64_t new_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
            InternalNode* new_node = allocator_.get_ptr<InternalNode>(new_node_offset);
            new (new_node) InternalNode();
            new_node->test_nibble_idx = split_idx;

            int nibble_new = get_nibble_at(key, split_idx);
            int nibble_old = get_nibble_at(existing_key, split_idx);

            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            new_node->children[nibble_new].store(make_leaf_ptr(new_record_offset), std::memory_order_relaxed);
            new_node->children[nibble_old].store(current_ptr, std::memory_order_relaxed); // Point to the old leaf

            uint64_t new_internal_node_ptr = get_offset(new_node_offset); // Internal nodes are not tagged
            if (current_ptr_loc->compare_exchange_strong(current_ptr, new_internal_node_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return; // Success
            }
            // Lost the race, retry
            continue;
        }

        // It's an internal node, traverse down
        InternalNode* node = allocator_.get_ptr<InternalNode>(current_ptr);
        int nibble = get_nibble_at(key, node->test_nibble_idx);
        current_ptr_loc = &node->children[nibble];
    }
}

std::optional<RecordData> StaxTree::get(const TxnContext &ctx, std::string_view key) const {
    StaxRecord* record = get_internal(ctx, key);
    if (!record) {
        return std::nullopt;
    }

    RecordData data;
    data.key_ptr = record->get_key_data();
    data.key_len = record->key_len;
    data.value_ptr = record->get_value_data();
    data.value_len = record->value_len;
    data.txn_id = record->txn_id;
    data.prev_version_offset = record->prev_version_offset;
    data.is_deleted = record->is_deleted;

    return data;
}


StaxRecord* StaxTree::get_internal(const TxnContext &ctx, std::string_view key) const {
    uint64_t current_ptr = root_ptr_.load(std::memory_order_relaxed);

    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        InternalNode* node = allocator_.get_ptr<InternalNode>(current_ptr);
        int nibble = get_nibble_at(key, node->test_nibble_idx);
        current_ptr = node->children[nibble].load(std::memory_order_relaxed);
        #if defined(__x86_64__) || defined(__i386__)
            if (current_ptr != 0) _mm_prefetch(allocator_.get_ptr<void>(get_offset(current_ptr)), _MM_HINT_T0);
        #elif defined(__aarch64__)
            if (current_ptr != 0) __builtin_prefetch(allocator_.get_ptr<void>(get_offset(current_ptr)), 0, 0);
        #endif
    }

    if (current_ptr == 0) return nullptr;

    uint64_t record_offset = get_offset(current_ptr);
    StaxRecord* head_record = allocator_.get_ptr<StaxRecord>(record_offset);

    if (head_record == nullptr || head_record->get_key() != key) return nullptr;

    // Traverse the version chain
    uint64_t current_version_offset = record_offset;
    while (current_version_offset != 0) {
        StaxRecord* record = allocator_.get_ptr<StaxRecord>(current_version_offset);
        if (record->txn_id <= ctx.read_snapshot_id) {
            if (record->is_deleted) return nullptr; // Tombstone found
            return record; // Visible version found
        }
        current_version_offset = record->prev_version_offset;
    }
    return nullptr; // No visible version found
}

void StaxTree::remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
    insert(local_alloc, ctx, key, "", true);
}

uint64_t StaxTree::allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
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
    if (value.length() > 0) {
        memcpy(record->get_value_data(), value.data(), value.length());
    }
    return offset;
}

int StaxTree::find_first_differing_nibble(std::string_view k1, std::string_view k2) {
    const size_t len1 = k1.length();
    const size_t len2 = k2.length();
    const size_t min_len = std::min(len1, len2);
    size_t i = 0;

    // Compare 8 bytes at a time
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

    // Compare byte by byte
    for (; i < min_len; ++i) {
        if (k1[i] != k2[i]) {
            uint8_t xor_val = k1[i] ^ k2[i];
            return i * 2 + ((xor_val & 0xF0) == 0);
        }
    }

    // If one key is a prefix of the other
    return (len1 == len2) ? -1 : min_len * 2;
}