#include "stax_core/stax_tree.hpp"
#include <cstring>
#include <algorithm>
#include <stdexcept>

// =================================================================================================
// --- StaxRecord Method Implementations ---
// =================================================================================================

char* StaxRecord::get_key_data() {
    return reinterpret_cast<char*>(this) + sizeof(StaxRecord);
}
const char* StaxRecord::get_key_data() const {
    return reinterpret_cast<const char*>(this) + sizeof(StaxRecord);
}
char* StaxRecord::get_value_data() {
    return reinterpret_cast<char*>(this) + sizeof(StaxRecord) + key_len;
}
const char* StaxRecord::get_value_data() const {
    return reinterpret_cast<const char*>(this) + sizeof(StaxRecord) + key_len;
}
std::string_view StaxRecord::get_key() const {
    return std::string_view(get_key_data(), key_len);
}

// =================================================================================================
// --- InternalNode Method Implementations ---
// =================================================================================================

InternalNode::InternalNode() : test_nibble_idx(0) {
    for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_relaxed);
}

// =================================================================================================
// --- StaxTree16 Method Implementations ---
// =================================================================================================

StaxTree16::StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref)
    : allocator_(allocator), root_ptr_(root_ref) {}

void StaxTree16::insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete) {
    std::atomic<uint64_t>* current_ptr_loc = &root_ptr_;

    while(true) {
        uint64_t current_ptr = current_ptr_loc->load(std::memory_order_acquire);

        if (current_ptr == 0) { // Tree is empty or we are at a null child pointer
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
            // Lost the race, another thread inserted. The allocated record will be leaked by the arena allocator.
            continue;
        }

        if (is_leaf(current_ptr)) {
            uint64_t existing_record_offset = get_offset(current_ptr);
            StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(existing_record_offset);
            std::string_view existing_key = existing_record->get_key();

            if (existing_key == key) { // Key exists, create new version
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, existing_record_offset);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                 if (current_ptr_loc->compare_exchange_strong(current_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
                // Lost the race, retry
                continue;
            }

            // Conflict, need to split
            int split_idx = find_first_differing_nibble(key, existing_key);

            uint64_t new_node_offset = local_alloc.allocate(sizeof(InternalNode), alignof(InternalNode));
            InternalNode* new_node = allocator_.get_ptr<InternalNode>(new_node_offset);
            new (new_node) InternalNode();
            new_node->test_nibble_idx = split_idx;

            int nibble_new = get_nibble_at(key, split_idx);
            int nibble_old = get_nibble_at(existing_key, split_idx);

            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            new_node->children[nibble_new].store(make_leaf_ptr(new_record_offset), std::memory_order_relaxed);
            new_node->children[nibble_old].store(current_ptr, std::memory_order_relaxed);

            if (current_ptr_loc->compare_exchange_strong(current_ptr, new_node_offset, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
            // Lost the race, retry
            continue;
        }

        // It's an internal node, traverse deeper
        InternalNode* node = allocator_.get_ptr<InternalNode>(current_ptr);
        int nibble = get_nibble_at(key, node->test_nibble_idx);
        current_ptr_loc = &node->children[nibble];
    }
}

StaxRecord* StaxTree16::get(const TxnContext &ctx, std::string_view key) const {
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

    // After finding a leaf, we must verify the full key.
    if (head_record == nullptr || head_record->get_key() != key) return nullptr;

    // Found the correct key, now find the correct version for the transaction
    uint64_t current_version_offset = record_offset;
    while (current_version_offset != 0) {
        StaxRecord* record = allocator_.get_ptr<StaxRecord>(current_version_offset);
        if (record->txn_id <= ctx.read_snapshot_id) {
            if (record->is_deleted) return nullptr;
            return record;
        }
        current_version_offset = record->prev_version_offset;
    }
    return nullptr;
}

void StaxTree16::remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
    insert(local_alloc, ctx, key, "", true);
}

std::vector<StaxRecord*> StaxTree16::range(const TxnContext &ctx, std::string_view prefix) const {
    std::vector<StaxRecord*> results;
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    if (current_ptr == 0) {
        return results;
    }

    // 1. Traverse the tree to the node that corresponds to the prefix.
    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        InternalNode* node = allocator_.get_ptr<InternalNode>(current_ptr);
        if (node->test_nibble_idx >= prefix.length() * 2) {
            break; // The whole subtree from here is potentially within the range.
        }
        int nibble = get_nibble_at(prefix, node->test_nibble_idx);
        current_ptr = node->children[nibble].load(std::memory_order_relaxed);
    }

    if (current_ptr == 0) {
        return results;
    }

    // 2. From that node, do a traversal of the subtree, collecting all valid records.
    std::stack<uint64_t> to_visit;
    to_visit.push(current_ptr);

    while (!to_visit.empty()) {
        uint64_t path_ptr = to_visit.top();
        to_visit.pop();

        if (is_leaf(path_ptr)) {
            uint64_t record_offset = get_offset(path_ptr);
            StaxRecord* head_record = allocator_.get_ptr<StaxRecord>(record_offset);

            if (head_record->get_key().rfind(prefix, 0) != 0) {
                continue; // Key doesn't match prefix, skip.
            }

            // Find the correct version for this transaction
            uint64_t current_version_offset = record_offset;
            while (current_version_offset != 0) {
                StaxRecord* record = allocator_.get_ptr<StaxRecord>(current_version_offset);
                if (record->txn_id <= ctx.read_snapshot_id) {
                    if (!record->is_deleted) {
                        results.push_back(record);
                    }
                    break; // Found the latest visible version for this key
                }
                current_version_offset = record->prev_version_offset;
            }
        } else { // It's an internal node
            InternalNode* node = allocator_.get_ptr<InternalNode>(path_ptr);
            for (int i = 15; i >= 0; --i) {
                uint64_t child_ptr = node->children[i].load(std::memory_order_relaxed);
                if (child_ptr != 0) {
                    to_visit.push(child_ptr);
                }
            }
        }
    }
    return results;
}

std::vector<StaxRecord*> StaxTree16::range(const TxnContext &ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts) const {
    // 1. Get all records for the given prefix.
    std::vector<StaxRecord*> prefix_results = range(ctx, prefix);
    std::vector<StaxRecord*> final_results;

    // 2. Filter the results based on the timestamp range.
    const size_t expected_key_len = prefix.length() + sizeof(uint64_t);
    for (StaxRecord* record : prefix_results) {
        std::string_view key = record->get_key();

        // Ensure the key has the exact expected length for this type of query.
        if (key.length() == expected_key_len) {
            // Extract the timestamp part of the key.
            std::string_view ts_sv = key.substr(prefix.length());

            // Convert from big-endian and check if it's within the desired range.
            try {
                uint64_t timestamp = big_endian_str_to_uint64(ts_sv);
                if (timestamp >= start_ts && timestamp <= end_ts) {
                    final_results.push_back(record);
                }
            } catch (const std::invalid_argument& e) {
                // Ignore keys that don't have a valid timestamp format.
            }
        }
    }

    return final_results;
}


// --- Private Method Implementations ---

bool StaxTree16::is_leaf(uint64_t ptr) {
    return (ptr & LEAF_TAG) != 0;
}

uint64_t StaxTree16::get_offset(uint64_t ptr) {
    return ptr & POINTER_MASK;
}

uint64_t StaxTree16::make_leaf_ptr(uint64_t record_byte_offset) {
    return record_byte_offset | LEAF_TAG;
}

int StaxTree16::get_nibble_at(std::string_view key, uint32_t nibble_idx) {
    size_t byte_idx = nibble_idx >> 1; // Faster division by 2
    if (byte_idx >= key.length()) return 0;
    uint8_t byte = key[byte_idx];
    return (nibble_idx & 1) == 0 ? (byte >> 4) & 0x0F : byte & 0x0F; // Faster modulo 2
}

uint64_t StaxTree16::allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
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
    memcpy(record->get_value_data(), value.data(), value.length());
    return offset;
}

int StaxTree16::find_first_differing_nibble(std::string_view k1, std::string_view k2) {
    const size_t len1 = k1.length();
    const size_t len2 = k2.length();
    const size_t min_len = std::min(len1, len2);
    size_t i = 0;

    // Fast path for comparing 64-bit chunks
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
        i *= 8; // update byte offset
    }

    // Byte-by-byte comparison for the remainder
    for (; i < min_len; ++i) {
        if (k1[i] != k2[i]) {
            uint8_t xor_val = k1[i] ^ k2[i];
            return i * 2 + ((xor_val & 0xF0) == 0); // Check if the high nibble is the same
        }
    }

    // If one key is a prefix of the other
    return (len1 == len2) ? -1 : min_len * 2;
}