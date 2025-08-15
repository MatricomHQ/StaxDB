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


// --- StaxTree::Cursor Implementation ---

// Helper function for numerical range scans
static uint64_t big_endian_str_to_uint64(std::string_view s) {
    if (s.length() != 8) {
        // This can happen if a key shares the prefix but isn't a valid numerical key.
        // Return a value that will fail the range check.
        return std::numeric_limits<uint64_t>::max();
    }
    uint64_t val = 0;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[0])) << 56;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[1])) << 48;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[2])) << 40;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[3])) << 32;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[4])) << 24;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[5])) << 16;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[6])) << 8;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[7]));
    return val;
}


StaxTree::Cursor::Cursor(const StaxTree* tree, const TxnContext& ctx, std::string_view prefix, bool is_end)
    : tree_(tree), ctx_(ctx), prefix_(prefix), is_end_sentinel_(is_end) {
    if (is_end_sentinel_) {
        return;
    }

    uint64_t current_ptr = tree_->root_ptr_.load(std::memory_order_acquire);
    if (current_ptr == 0) {
        is_end_sentinel_ = true; // Tree is empty
        return;
    }

    // Navigate to the starting node for the prefix scan
    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        InternalNode* node = tree_->allocator_.get_ptr<InternalNode>(current_ptr);
        if (node->test_nibble_idx >= prefix.length() * 2) {
            break; // The whole subtree from here is potentially part of the range
        }
        int nibble = get_nibble_at(prefix, node->test_nibble_idx);
        current_ptr = node->children[nibble].load(std::memory_order_relaxed);
    }

    if (current_ptr != 0) {
        to_visit_.push(current_ptr);
    }

    advance(); // Find the first element
}

StaxTree::Cursor::Cursor(const StaxTree* tree, const TxnContext& ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts, bool is_end)
    : tree_(tree), ctx_(ctx), prefix_(prefix), start_ts_(start_ts), end_ts_(end_ts), is_end_sentinel_(is_end) {
     if (is_end_sentinel_) {
        return;
    }
    // The traversal logic is the same as the prefix-only scan; the timestamp check is done during iteration.
    uint64_t current_ptr = tree_->root_ptr_.load(std::memory_order_acquire);
    if (current_ptr == 0) {
        is_end_sentinel_ = true;
        return;
    }
    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        InternalNode* node = tree_->allocator_.get_ptr<InternalNode>(current_ptr);
        if (node->test_nibble_idx >= prefix.length() * 2) {
            break;
        }
        int nibble = get_nibble_at(prefix, node->test_nibble_idx);
        current_ptr = node->children[nibble].load(std::memory_order_relaxed);
    }
    if (current_ptr != 0) {
        to_visit_.push(current_ptr);
    }
    advance();
}

void StaxTree::Cursor::advance() {
    current_record_ = std::nullopt;

    while (!to_visit_.empty()) {
        uint64_t path_ptr = to_visit_.top();
        to_visit_.pop();

        if (is_leaf(path_ptr)) {
            uint64_t head_offset = get_offset(path_ptr);
            StaxRecord* head_record = tree_->allocator_.get_ptr<StaxRecord>(head_offset);
            std::string_view key = head_record->get_key();

            // 1. Check if the key has the correct prefix
            if (key.rfind(prefix_, 0) != 0) {
                continue;
            }

            // 2. If it's a timestamp query, check the range
            if (start_ts_ && !check_timestamp(key)) {
                continue;
            }

            // 3. Find the visible version of the record
            StaxRecord* visible_record = get_visible_record(head_offset);

            if (visible_record) {
                // Found a valid, visible record.
                RecordData data;
                data.key_ptr = visible_record->get_key_data();
                data.key_len = visible_record->key_len;
                data.value_ptr = visible_record->get_value_data();
                data.value_len = visible_record->value_len;
                data.txn_id = visible_record->txn_id;
                data.prev_version_offset = visible_record->prev_version_offset;
                data.is_deleted = visible_record->is_deleted;
                current_record_ = data;
                return; // Found an item, stop advancing
            }
        } else { // It's an internal node
            InternalNode* node = tree_->allocator_.get_ptr<InternalNode>(path_ptr);
            // Push children in reverse order to visit them lexicographically
            for (int i = 15; i >= 0; --i) {
                uint64_t child_ptr = node->children[i].load(std::memory_order_relaxed);
                if (child_ptr != 0) {
                    to_visit_.push(child_ptr);
                }
            }
        }
    }

    // If we get here, the traversal is complete.
    is_end_sentinel_ = true;
}

StaxRecord* StaxTree::Cursor::get_visible_record(uint64_t head_record_offset) const {
    uint64_t current_offset = head_record_offset;
    while (current_offset != 0) {
        StaxRecord* record = tree_->allocator_.get_ptr<StaxRecord>(current_offset);
        if (record->txn_id <= ctx_.read_snapshot_id) {
            if (record->is_deleted) return nullptr; // Found a tombstone
            return record; // Found a visible version
        }
        current_offset = record->prev_version_offset;
    }
    return nullptr; // No visible version found
}

bool StaxTree::Cursor::check_timestamp(std::string_view key) const {
    const size_t expected_key_len = prefix_.length() + sizeof(uint64_t);
    if (key.length() != expected_key_len) {
        return false;
    }
    std::string_view ts_sv = key.substr(prefix_.length());
    uint64_t timestamp = big_endian_str_to_uint64(ts_sv);
    return timestamp >= *start_ts_ && timestamp <= *end_ts_;
}


// --- Iterator Methods ---
StaxTree::Cursor& StaxTree::Cursor::operator++() {
    advance();
    return *this;
}

StaxTree::Cursor::reference StaxTree::Cursor::operator*() const {
    return *current_record_;
}

StaxTree::Cursor::pointer StaxTree::Cursor::operator->() const {
    return &(*current_record_);
}

bool StaxTree::Cursor::operator!=(const Cursor& other) const {
    // Two iterators are different if one is the end sentinel and the other is not.
    return is_end_sentinel_ != other.is_end_sentinel_;
}

bool StaxTree::Cursor::operator==(const Cursor& other) const {
    return is_end_sentinel_ == other.is_end_sentinel_;
}


// --- Range-based for loop support ---
StaxTree::Cursor& StaxTree::Cursor::begin() {
    return *this;
}

StaxTree::Cursor StaxTree::Cursor::end() {
    // Return a new cursor that is explicitly an end sentinel.
    if (start_ts_) {
        return Cursor(tree_, ctx_, prefix_, *start_ts_, *end_ts_, true);
    } else {
        return Cursor(tree_, ctx_, prefix_, true);
    }
}


// --- StaxTree range() methods ---

StaxTree::Cursor StaxTree::range(const TxnContext& ctx, std::string_view prefix) const {
    return Cursor(this, ctx, prefix);
}

StaxTree::Cursor StaxTree::range(const TxnContext& ctx, std::string_view prefix, uint64_t start_ts, uint64_t end_ts) const {
    return Cursor(this, ctx, prefix, start_ts, end_ts);
}