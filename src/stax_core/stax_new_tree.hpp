#pragma once

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <stack>
#include <cstring>
#include <optional>

#include "stax_common/constants.h"
#include "stax_tx/transaction.h"
#include "stax_db/arena_structs.h"
#include "dimensional.hpp"
#include <queue>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif

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


#include "stax_structs.hpp"

// =================================================================================================
// --- Utility Functions ---
// =================================================================================================

// Converts a uint64_t to a big-endian byte string for lexicographical ordering.
inline std::string uint64_to_big_endian_str(uint64_t val) {
    std::string s(8, '\0');
    s[0] = (val >> 56) & 0xFF;
    s[1] = (val >> 48) & 0xFF;
    s[2] = (val >> 40) & 0xFF;
    s[3] = (val >> 32) & 0xFF;
    s[4] = (val >> 24) & 0xFF;
    s[5] = (val >> 16) & 0xFF;
    s[6] = (val >> 8) & 0xFF;
    s[7] = val & 0xFF;
    return s;
}

// Converts a big-endian byte string view back to a uint64_t.
inline uint64_t big_endian_str_to_uint64(std::string_view s) {
    if (s.length() != 8) {
        throw std::invalid_argument("String view must be 8 bytes long for uint64_t conversion.");
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

// =================================================================================================
// --- StaxTree16 (Niblet Tree Implementation) ---
// =================================================================================================
class StaxTree16
{
private:
    StaxAllocator &allocator_;
    std::atomic<uint64_t> &root_ptr_;
    uint32_t D_; // Dimensionality of the keys. D=0 for non-dimensional trees.

public:
    StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref, uint32_t D = 0)
    : allocator_(allocator),
      root_ptr_(root_ref),
      D_(D) {}

    // Dimensionality accessor
    uint32_t get_dimensionality() const { return D_; }

    std::optional<StaxPath> find_path_for_seek(std::string_view key, bool find_first_on_mismatch) const;
    std::optional<StaxPath> find_path_for_seek_last(std::string_view key, bool find_last_on_mismatch) const;

    // Cursor factory method
    std::unique_ptr<StaxCursor<StaxTree16>> create_cursor(
        std::string_view start_key,
        std::string_view end_key,
        bool track_aabb) const {
        return std::make_unique<StaxCursor<StaxTree16>>(this, start_key, end_key, track_aabb);
    }

    // Dimensional query methods
    std::vector<void*> query_box(const AABB& query_box, QueryStats& stats) const;
    std::vector<void*> query_sphere(const uint64_t* center, long double radius, QueryStats& stats) const;
    std::vector<void*> query_knn(const uint64_t* query_point, int k, QueryStats& stats) const;

    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);
    void range_scan(const TxnContext &ctx, std::string_view start_key, std::string_view end_key, std::vector<StaxRecord*>& results) const;
    StaxRecord* get_visible_record(StaxRecord* record_head, const TxnContext& ctx) const;
    StaxRecord* get(const TxnContext &ctx, std::string_view key) const;

    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key) {
        insert(local_alloc, ctx, key, "", true);
    }

    void insert_batch(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, const CoreKVPair* kv_pairs, size_t num_kvs, TransactionBatch& batch) {
        for (size_t i = 0; i < num_kvs; ++i) {
            insert(local_alloc, ctx, kv_pairs[i].key, kv_pairs[i].value, false);
        }
    }

    void multi_get_simd(const TxnContext& ctx, const std::vector<std::string_view>& keys, std::vector<std::optional<RecordData>>& results) const {
        results.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            StaxRecord* record = get(ctx, keys[i]);
            if (record) {
                results[i] = RecordData{
                    record->get_key_data(),
                    record->key_len,
                    record->get_value_data(),
                    record->value_len,
                    record->is_deleted
                };
            }
        }
    }

private:
    // --- Other Private Helpers ---
    STAX_ALWAYS_INLINE int get_child_idx(int nibble, uint32_t test_idx) const { return nibble ^ (test_idx & 0x0F); }
    STAX_ALWAYS_INLINE int get_nibble_from_child_idx(int child_idx, uint32_t test_idx) const { return child_idx ^ (test_idx & 0x0F); }

public:
    // A pointer is either a leaf or an internal node.
    // We use the LSB to distinguish: 1 for leaf, 0 for internal.
    // For internal nodes, we encode the test_nibble_idx in the high bits.
    // This removes a memory fetch during traversal.
    static constexpr uint64_t LEAF_TAG = 1ULL;
    // We reserve 48 bits for the offset, which supports up to 256TB.
    static constexpr uint64_t OFFSET_MASK = (1ULL << 48) - 1;
    // We use the next 16 bits for the test_nibble_idx.
    static constexpr int IDX_SHIFT = 48;

    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr) { return (ptr & LEAF_TAG) != 0; }

    static STAX_ALWAYS_INLINE uint64_t get_offset(uint64_t ptr) {
        // This conditional will be compiled to an efficient cmov instruction.
        return is_leaf(ptr) ? (ptr & ~LEAF_TAG) : (ptr & OFFSET_MASK);
    }

    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t record_byte_offset) {
        // Ensure we don't have garbage in the high bits, then set the tag.
        return (record_byte_offset & OFFSET_MASK) | LEAF_TAG;
    }

    static STAX_ALWAYS_INLINE uint32_t get_test_idx(uint64_t ptr) {
        // This is only valid for internal node pointers.
        return ptr >> IDX_SHIFT;
    }

    static STAX_ALWAYS_INLINE uint64_t make_internal_ptr(uint64_t node_offset, uint32_t test_idx) {
        if (test_idx >= (1 << 16)) {
            // Keys longer than 32KB are not supported by this optimization.
            throw std::runtime_error("Key is too long to encode test_idx in pointer.");
        }
        // Ensure offset is clean, then OR in the test_idx at its shifted position.
        return (node_offset & OFFSET_MASK) | (static_cast<uint64_t>(test_idx) << IDX_SHIFT);
    }

    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx) {
        size_t byte_idx = nibble_idx >> 1; // Faster division by 2
        if (byte_idx >= key.length()) return 0;
        uint8_t byte = key[byte_idx];
        return (nibble_idx & 1) == 0 ? (byte >> 4) & 0x0F : byte & 0x0F; // Faster modulo 2
    }

    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset) {
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

    static int find_first_differing_nibble(std::string_view k1, std::string_view k2) {
        const size_t len1 = k1.length();
        const size_t len2 = k2.length();
        const size_t min_len = std::min(len1, len2);
        size_t i = 0;
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
        for (; i < min_len; ++i) {
            if (k1[i] != k2[i]) {
                uint8_t xor_val = k1[i] ^ k2[i];
                return i * 2 + ((xor_val & 0xF0) == 0);
            }
        }
        return (len1 == len2) ? -1 : min_len * 2;
    }

    StaxAllocator& get_allocator() { return allocator_; }
    const StaxAllocator& get_allocator() const { return allocator_; }
    std::atomic<uint64_t>& get_root_ptr() { return root_ptr_; }
    const std::atomic<uint64_t>& get_root_ptr() const { return root_ptr_; }
};

inline std::optional<StaxPath> StaxTree16::find_path_for_seek(std::string_view key, bool find_first_on_mismatch) const {
    StaxPath path;
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    int last_nibble = -1;

    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        uint32_t test_idx = get_test_idx(current_ptr);
        path.frames.push_back({current_ptr, last_nibble, test_idx});
        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
        int nibble = get_nibble_at(key, test_idx);
        last_nibble = nibble;
        current_ptr = node->children[nibble].load(std::memory_order_acquire);
    }

    if (current_ptr != 0) { // is_leaf
        StaxRecord* record = allocator_.get_ptr<StaxRecord>(get_offset(current_ptr));
        if (record->get_key() >= key) {
            path.leaf_handle = current_ptr;
            path.leaf_nibble_in_parent = last_nibble;
            return path;
        }
    }

    if (find_first_on_mismatch) {
        // The key wasn't found. We need to backtrack up the path to find the
        // next lexicographical leaf.
        while (!path.frames.empty()) {
            StaxPath::Frame last_frame = path.frames.back();
            path.frames.pop_back();

            InternalNode* parent_node = allocator_.get_ptr<InternalNode>(get_offset(last_frame.node_ptr));

            // Scan siblings in the parent node for the next valid branch.
            for (int nibble = last_frame.nibble_in_parent + 1; nibble < 16; ++nibble) {
                uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
                if (child_ptr != 0) {
                    // Found the next branch. Now, do a left-most descent to find its first leaf.
                    path.frames.push_back({last_frame.node_ptr, nibble, last_frame.test_idx});
                    current_ptr = child_ptr;
                    while (!is_leaf(current_ptr)) {
                        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
                        // Find the first valid child (0-15) and descend.
                        bool found_child = false;
                        uint32_t test_idx = get_test_idx(current_ptr);
                        for (int i = 0; i < 16; ++i) {
                            uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                            if (next_ptr != 0) {
                                path.frames.push_back({current_ptr, i, test_idx});
                                current_ptr = next_ptr;
                                found_child = true;
                                break;
                            }
                        }
                        if (!found_child) {
                             // Should not happen in a well-formed tree
                             return std::nullopt;
                        }
                    }
                    path.leaf_handle = current_ptr;
                    return path;
                }
            }
        }
    }

    return std::nullopt; // No next key found
}

inline std::optional<StaxPath> StaxTree16::find_path_for_seek_last(std::string_view key, bool find_last_on_mismatch) const {
    StaxPath path;
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    int last_nibble = -1;

    while (current_ptr != 0 && !is_leaf(current_ptr)) {
        uint32_t test_idx = get_test_idx(current_ptr);
        path.frames.push_back({current_ptr, last_nibble, test_idx});
        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
        int nibble = get_nibble_at(key, test_idx);
        last_nibble = nibble;
        current_ptr = node->children[nibble].load(std::memory_order_acquire);
    }

    if (current_ptr != 0) { // is_leaf
        StaxRecord* record = allocator_.get_ptr<StaxRecord>(get_offset(current_ptr));
        if (record->get_key() <= key) {
            path.leaf_handle = current_ptr;
            path.leaf_nibble_in_parent = last_nibble;
            return path;
        }
    }

    if (find_last_on_mismatch) {
        while (!path.frames.empty()) {
            StaxPath::Frame last_frame = path.frames.back();
            path.frames.pop_back();

            InternalNode* parent_node = allocator_.get_ptr<InternalNode>(get_offset(last_frame.node_ptr));

            for (int nibble = last_frame.nibble_in_parent - 1; nibble >= 0; --nibble) {
                uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
                if (child_ptr != 0) {
                    path.frames.push_back({last_frame.node_ptr, nibble, last_frame.test_idx});
                    current_ptr = child_ptr;
                    while (!is_leaf(current_ptr)) {
                        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
                        bool found_child = false;
                        uint32_t test_idx = get_test_idx(current_ptr);
                        for (int i = 15; i >= 0; --i) {
                            uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                            if (next_ptr != 0) {
                                path.frames.push_back({current_ptr, i, test_idx});
                                current_ptr = next_ptr;
                                found_child = true;
                                break;
                            }
                        }
                        if (!found_child) return std::nullopt;
                    }
                    path.leaf_handle = current_ptr;
                    path.leaf_nibble_in_parent = -1; // Not relevant for right-most descent
                    return path;
                }
            }
        }
    }

    return std::nullopt;
}


#include "dimensional.inl"

inline std::vector<void*> StaxTree16::query_box(const AABB& query_box, QueryStats& stats) const {
    std::vector<void*> results;
    auto cursor = create_cursor("", "\xFF", true); // Max range
    cursor->seek_first();
    stats.nodes_visited++;

    while (cursor->is_valid()) {
        stats.leaves_visited++;
        const AABB* current_aabb = cursor->get_current_aabb();
        if (current_aabb->is_valid() && aabbs_intersect(*current_aabb, query_box)) {
            stats.records_loaded++;
            StaxRecord* record = allocator_.get_ptr<StaxRecord>(get_offset(reinterpret_cast<uint64_t>(cursor->get_record_handle())));
            std::vector<uint64_t> coords(D_);
            SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D_);
            stats.records_scanned++;
            bool in_box = true;
            for(uint32_t i=0; i < D_; ++i) {
                if (coords[i] < query_box.min_bounds[i] || coords[i] > query_box.max_bounds[i]) {
                    in_box = false;
                    break;
                }
            }
            if (in_box) {
                stats.records_accepted++;
                results.push_back(cursor->get_record_handle());
            }
            cursor->move_next();
        } else {
            cursor->move_next();
        }
    }
    return results;
}

inline std::vector<void*> StaxTree16::query_sphere(const uint64_t* center, long double radius, QueryStats& stats) const {
    // 1. Create a bounding box from the sphere.
    AABB sphere_box(D_);
    for (uint32_t i = 0; i < D_; ++i) {
        sphere_box.min_bounds[i] = (center[i] > radius) ? center[i] - static_cast<uint64_t>(radius) : 0;
        sphere_box.max_bounds[i] = (center[i] < UINT64_MAX - radius) ? center[i] + static_cast<uint64_t>(radius) : UINT64_MAX;
    }

    // 2. Use query_box to get candidate points.
    QueryStats box_stats;
    std::vector<void*> candidates = query_box(sphere_box, box_stats);
    stats.nodes_visited += box_stats.nodes_visited;
    stats.leaves_visited += box_stats.leaves_visited;
    stats.records_loaded += box_stats.records_loaded;

    std::vector<void*> results;
    long double radius_sq = radius * radius;

    // 3. Final filtering.
    for (void* handle : candidates) {
        stats.records_scanned++;
        StaxRecord* record = allocator_.get_ptr<StaxRecord>(get_offset(reinterpret_cast<uint64_t>(handle)));
        std::vector<uint64_t> coords(D_);
        SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D_);
        if (SpatialKeywords::PointDistSq(coords.data(), center, D_) <= radius_sq) {
            stats.records_accepted++;
            results.push_back(handle);
        }
    }

    return results;
}

inline std::vector<void*> StaxTree16::query_knn(const uint64_t* query_point, int k, QueryStats& stats) const {
    if (k <= 0) return {};

    std::string center_key = SpatialKeywords::generate_apk(query_point, D_);

    using ResultPair = std::pair<long double, void*>;
    auto cmp = [](const ResultPair& a, const ResultPair& b) {
        if (a.first != b.first) {
            return a.first < b.first; // Max-heap on distance
        }
        return a.second < b.second; // Tie-break on handle
    };
    std::priority_queue<ResultPair, std::vector<ResultPair>, decltype(cmp)> best_results(cmp);

    auto forward_cursor = create_cursor(center_key, "\xFF", true);
    auto backward_cursor = create_cursor("", center_key, true);

    forward_cursor->seek_first();
    backward_cursor->seek_last();

    bool forward_pruned = false;
    bool backward_pruned = false;

    // Handle the exact match case where both cursors might point to the same initial record.
    if (forward_cursor->is_valid() && backward_cursor->is_valid() && forward_cursor->get_record_handle() == backward_cursor->get_record_handle()) {
        backward_cursor->move_prev();
    }

    while ((forward_cursor->is_valid() && !forward_pruned) || (backward_cursor->is_valid() && !backward_pruned)) {
        stats.nodes_visited++;
        long double current_max_dist_sq = (best_results.size() == static_cast<size_t>(k)) ? best_results.top().first : -1.0L;

        bool use_forward = false;
        if (forward_cursor->is_valid() && !forward_pruned) {
            if (!backward_cursor->is_valid() || backward_pruned) {
                use_forward = true;
            } else {
                long double f_dist = SpatialKeywords::distance_to_box_sq(query_point, *forward_cursor->get_current_aabb(), D_);
                long double b_dist = SpatialKeywords::distance_to_box_sq(query_point, *backward_cursor->get_current_aabb(), D_);
                if (f_dist < b_dist) use_forward = true;
            }
        }

        auto* cursor_to_use = use_forward ? forward_cursor.get() : backward_cursor.get();
        bool& cursor_pruned = use_forward ? forward_pruned : backward_pruned;

        stats.leaves_visited++;
        const AABB* box = cursor_to_use->get_current_aabb();
        if (current_max_dist_sq >= 0 && SpatialKeywords::distance_to_box_sq(query_point, *box, D_) > current_max_dist_sq) {
            cursor_pruned = true;
        } else {
            void* handle = cursor_to_use->get_record_handle();
            stats.records_loaded++;
            StaxRecord* record = allocator_.get_ptr<StaxRecord>(get_offset(reinterpret_cast<uint64_t>(handle)));
            std::vector<uint64_t> coords(D_);
            SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D_);
            stats.records_scanned++;
            long double dist_sq = SpatialKeywords::PointDistSq(coords.data(), query_point, D_);

            if (best_results.size() < static_cast<size_t>(k)) {
                best_results.push({dist_sq, handle});
                stats.records_accepted++;
            } else if (dist_sq < best_results.top().first) {
                best_results.pop();
                best_results.push({dist_sq, handle});
            }
        }

        if (use_forward) forward_cursor->move_next(); else backward_cursor->move_prev();
    }

    std::vector<void*> results;
    results.reserve(best_results.size());
    while(!best_results.empty()) {
        results.push_back(best_results.top().second);
        best_results.pop();
    }
    std::reverse(results.begin(), results.end());
    return results;
}


inline void StaxTree16::insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete) {
    while (true) {
        std::atomic<uint64_t>* parent_ptr_loc = &root_ptr_;
        uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);

        while (current_ptr != 0 && !is_leaf(current_ptr)) {
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            uint32_t test_idx = get_test_idx(current_ptr);

            std::string_view rep_key = node->get_key();
            int d_idx = find_first_differing_nibble(key, rep_key);

            if (d_idx != -1 && static_cast<uint32_t>(d_idx) < test_idx) {
                // Path diverges before this node. Split is needed above.
                break;
            }

            int nibble = get_nibble_at(key, test_idx);
            parent_ptr_loc = &node->children[nibble];
            current_ptr = parent_ptr_loc->load(std::memory_order_acquire);
        }

        uint64_t expected_ptr = current_ptr;

        if (current_ptr == 0) {
            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
            if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
        } else if (is_leaf(current_ptr)) {
            uint64_t leaf_offset = get_offset(current_ptr);
            StaxRecord* existing_record = allocator_.get_ptr<StaxRecord>(leaf_offset);
            if (existing_record->get_key() == key) {
                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, leaf_offset);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);
                if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_leaf_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
            } else {
                std::string_view existing_key = existing_record->get_key();
                int d_idx = find_first_differing_nibble(key, existing_key);

                size_t new_node_size = sizeof(InternalNode) + key.length();
                uint64_t new_internal_node_offset = local_alloc.allocate(new_node_size, alignof(InternalNode));
                InternalNode* new_node = new (allocator_.get_ptr<InternalNode>(new_internal_node_offset)) InternalNode(key.length());
                memcpy(new_node->get_key_data(), key.data(), key.length());

                uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
                uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);

                int new_key_nibble = get_nibble_at(key, d_idx);
                int existing_key_nibble = get_nibble_at(existing_key, d_idx);

                new_node->children[new_key_nibble].store(new_leaf_ptr, std::memory_order_relaxed);
                new_node->children[existing_key_nibble].store(current_ptr, std::memory_order_relaxed);

                uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
                if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
            }
        } else { // Split internal node
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            std::string_view rep_key = node->get_key();
            int d_idx = find_first_differing_nibble(key, rep_key);

            size_t new_node_size = sizeof(InternalNode) + key.length();
            uint64_t new_internal_node_offset = local_alloc.allocate(new_node_size, alignof(InternalNode));
            InternalNode* new_node = new (allocator_.get_ptr<InternalNode>(new_internal_node_offset)) InternalNode(key.length());
            memcpy(new_node->get_key_data(), key.data(), key.length());

            uint64_t new_record_offset = allocate_new_record(local_alloc, ctx, key, value, is_delete, 0);
            uint64_t new_leaf_ptr = make_leaf_ptr(new_record_offset);

            int new_key_nibble = get_nibble_at(key, d_idx);
            int existing_key_nibble = get_nibble_at(rep_key, d_idx);

            new_node->children[new_key_nibble].store(new_leaf_ptr, std::memory_order_relaxed);
            new_node->children[existing_key_nibble].store(current_ptr, std::memory_order_relaxed);

            uint64_t new_internal_ptr = make_internal_ptr(new_internal_node_offset, d_idx);
            if (parent_ptr_loc->compare_exchange_strong(expected_ptr, new_internal_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
        }
    }
}

inline void StaxTree16::range_scan(const TxnContext &ctx, std::string_view start_key, std::string_view end_key, std::vector<StaxRecord*>& results) const {
    if (start_key > end_key) return;

    struct ScanState {
        uint64_t node_ptr;
        bool has_lower_bound;
        bool has_upper_bound;
    };

    std::vector<ScanState> stack;
    uint64_t root_ptr = root_ptr_.load(std::memory_order_acquire);
    if (root_ptr != 0) {
        stack.push_back({root_ptr, true, true});
    }

    while(!stack.empty()) {
        ScanState current_state = stack.back();
        stack.pop_back();

        uint64_t node_ptr = current_state.node_ptr;
        bool has_lower_bound = current_state.has_lower_bound;
        bool has_upper_bound = current_state.has_upper_bound;

        if (is_leaf(node_ptr)) {
            StaxRecord* record_head = allocator_.get_ptr<StaxRecord>(get_offset(node_ptr));
            StaxRecord* visible_record = get_visible_record(record_head, ctx);
            if (visible_record) {
                std::string_view key = visible_record->get_key();
                if ((!has_lower_bound || key >= start_key) && (!has_upper_bound || key <= end_key)) {
                    results.push_back(visible_record);
                }
            }
            continue;
        }

        InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(node_ptr));
        uint32_t test_idx = get_test_idx(node_ptr);

        int start_nibble = has_lower_bound ? get_nibble_at(start_key, test_idx) : 0;
        int end_nibble = has_upper_bound ? get_nibble_at(end_key, test_idx) : 15;

        for (int i = end_nibble; i >= start_nibble; --i) {
            uint64_t child_ptr = node->children[i].load(std::memory_order_acquire);
            if (!child_ptr) continue;

            bool new_has_lower_bound = has_lower_bound && (i == start_nibble);
            bool new_has_upper_bound = has_upper_bound && (i == end_nibble);

            stack.push_back({child_ptr, new_has_lower_bound, new_has_upper_bound});
        }
    }
}

inline StaxRecord* StaxTree16::get_visible_record(StaxRecord* record_head, const TxnContext& ctx) const {
    StaxRecord* current_rec = record_head;
    while (current_rec != nullptr) {
        if (current_rec->txn_id <= ctx.read_snapshot_id) {
            if (current_rec->is_deleted) {
                return nullptr;
            }
            return current_rec;
        }
        current_rec = allocator_.get_ptr<StaxRecord>(current_rec->prev_version_offset);
    }
    return nullptr;
}

inline StaxRecord* StaxTree16::get(const TxnContext &ctx, std::string_view key) const {
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    while (current_ptr != 0) {
        if (is_leaf(current_ptr)) {
            StaxRecord* record_head = allocator_.get_ptr<StaxRecord>(get_offset(current_ptr));
            if (record_head->get_key() == key) {
                return get_visible_record(record_head, ctx);
            }
            return nullptr;
        } else {
            uint32_t test_idx = get_test_idx(current_ptr);
            int nibble = get_nibble_at(key, test_idx);
            InternalNode* node = allocator_.get_ptr<InternalNode>(get_offset(current_ptr));
            current_ptr = node->children[nibble].load(std::memory_order_acquire);
        }
    }
    return nullptr;
}
