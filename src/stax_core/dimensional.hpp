#pragma once

#include <cstdint>
#include <array>
#include <vector>
#include <string>
#include <string_view>
#include <memory>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <optional>

#include "stax_structs.hpp"

// Forward-declare StaxTree16 and StaxCursor to break the include cycle
class StaxTree16;
template<typename Tree> class StaxCursor;

namespace SpatialKeywords {

    namespace detail {

    // Corrected and Optimized scalar kernel
    inline void generate_apk_interleaved_scalar(const uint64_t* coords, uint32_t D, uint8_t*& write_ptr) {
        const int num_nibbles_to_interleave = 2 * 2; // Hardcoded to 2 bytes per dim
        uint8_t byte_buffer = 0;
        bool high_nibble_set = false;

        for (int nibble_idx_from_top = 0; nibble_idx_from_top < num_nibbles_to_interleave; ++nibble_idx_from_top) {
            const int bit_shift = 60 - (nibble_idx_from_top * 4);
            for (uint32_t dim_idx = 0; dim_idx < D; ++dim_idx) {
                const uint8_t nibble = (coords[dim_idx] >> bit_shift) & 0x0F;
                if (high_nibble_set) {
                    byte_buffer |= nibble;
                    *write_ptr++ = byte_buffer;
                } else {
                    byte_buffer = nibble << 4;
                }
                high_nibble_set = !high_nibble_set;
            }
        }
        if (high_nibble_set) {
            *write_ptr++ = byte_buffer;
        }
    }

    } // namespace detail

    inline size_t get_max_apk_size(uint32_t D) {
        constexpr size_t HEADER_SIZE = 2;
        const int bytes_per_dim = 2;
        const size_t interleaved_size = D * bytes_per_dim;
        const size_t original_coords_size = D * sizeof(uint64_t);
        return HEADER_SIZE + interleaved_size + original_coords_size;
    }

    inline size_t generate_apk(const uint64_t* coords, uint32_t D, uint8_t* key_buffer, size_t buffer_size) {
        constexpr size_t HEADER_SIZE = 2;
        if (D == 0) {
            if (buffer_size < HEADER_SIZE) throw std::runtime_error("Buffer too small.");
            memset(key_buffer, 0, HEADER_SIZE);
            return HEADER_SIZE;
        }

        const int bytes_per_dim = 2;
        const size_t interleaved_size = D * bytes_per_dim;
        const size_t original_coords_size = D * sizeof(uint64_t);
        const size_t required_size = HEADER_SIZE + interleaved_size + original_coords_size;

        if (buffer_size < required_size) {
            throw std::runtime_error("Buffer too small for APK generation.");
        }

        uint16_t dim_be = htobe16(static_cast<uint16_t>(D));
        memcpy(key_buffer, &dim_be, HEADER_SIZE);
        uint8_t* write_ptr = key_buffer + HEADER_SIZE;

        detail::generate_apk_interleaved_scalar(coords, D, write_ptr);
        memcpy(write_ptr, coords, original_coords_size);

        return required_size;
    }

    inline void get_coords_from_apk(std::string_view apk, uint64_t* coords, uint32_t D) {
        const size_t coords_offset = 2 + (D * 2);
        memcpy(coords, apk.data() + coords_offset, D * sizeof(uint64_t));
    }

    inline long double PointDistSq(const uint64_t* p1, const uint64_t* p2, uint32_t D) {
        long double total_dist = 0.0L;
        for (uint32_t i = 0; i < D; ++i) {
            long double d = static_cast<long double>(p1[i]) - static_cast<long double>(p2[i]);
            total_dist += d * d;
        }
        return total_dist;
    }

    inline long double distance_to_box_sq(const uint64_t* point, const AABB& box, uint32_t D) {
        long double dist_sq = 0.0L;
        for (uint32_t i = 0; i < D; ++i) {
            long double d = 0.0L;
            if (point[i] < box.min_bounds[i]) {
                d = static_cast<long double>(box.min_bounds[i]) - static_cast<long double>(point[i]);
            } else if (point[i] > box.max_bounds[i]) {
                d = static_cast<long double>(point[i]) - static_cast<long double>(box.max_bounds[i]);
            }
            dist_sq += d * d;
        }
        return dist_sq;
    }

} // namespace SpatialKeywords

inline bool aabbs_intersect(const AABB& a, const AABB& b) {
    for (uint32_t i = 0; i < a.D; ++i) {
        if (a.max_bounds[i] < b.min_bounds[i] || a.min_bounds[i] > b.max_bounds[i]) {
            return false;
        }
    }
    return true;
}

// The get_nibble_from_fragment function needs to be declared for use in refine_box_with_fragment
inline int get_nibble_from_fragment(const uint8_t* fragment, uint8_t nibble_idx) {
    uint8_t byte = fragment[nibble_idx / 2];
    return (nibble_idx % 2 == 0) ? (byte >> 4) & 0x0F : byte & 0x0F;
}

template<size_t FANOUT>
inline void refine_box_for_nibble(AABB& box, uint32_t key_nibble_offset, int nibble_value, uint32_t D, StaxCursor<StaxTree16>* cursor = nullptr);

template<size_t FANOUT>
inline void refine_box_with_fragment(
    AABB& box,
    const uint8_t* fragment_bytes,
    uint8_t fragment_len_nibbles,
    uint32_t key_nibble_offset,
    uint32_t D,
    StaxCursor<StaxTree16>* cursor = nullptr)
{
    for (uint8_t i = 0; i < fragment_len_nibbles; ++i) {
        int nibble = get_nibble_from_fragment(fragment_bytes, i);
        refine_box_for_nibble<FANOUT>(box, key_nibble_offset + i, nibble, D, cursor);
    }
}

template<typename Tree>
class StaxCursor {
    friend class StaxTree16; // Allow the tree to call private skip method.

    template <typename Cursor>
    friend void refine_box_for_nibble(AABB& box, uint32_t key_nibble_offset, int nibble_value, uint32_t D, Cursor* cursor);

public:
    StaxCursor(const Tree* tree, std::string_view start_key, std::string_view end_key, bool track_aabb);

    void seek_first();
    void seek_last();
    void move_next();
    void move_prev();
    bool is_valid() const;
    void* get_record_handle() const;
    const AABB* get_current_aabb() const;
    std::string_view get_key() const;
    void skip_to(std::string_view key);

private:
    // Represents one change to a bound in the AABB. Used for the undo log.
    struct AABBChange {
        uint32_t dim_idx;
        uint64_t old_min;
        uint64_t old_max;
    };

    struct CursorFrame {
        uint64_t node_ptr;
        int nibble_in_parent;
        size_t aabb_log_size; // Restore point for the AABB change log.
        uint32_t key_nibble_offset;
    };

    const Tree* tree_;
    const bool track_aabb_;
    void* current_record_handle_;
    std::string start_key_;
    std::string end_key_;
    std::vector<CursorFrame> stack_;
    std::unique_ptr<AABB> current_aabb_;

    // A stack-based, heap-free "undo log" for AABB changes.
    static constexpr size_t MAX_AABB_CHANGES = 1024;
    std::array<AABBChange, MAX_AABB_CHANGES> aabb_change_log_;
    size_t aabb_change_log_size_ = 0;

    void log_aabb_change(uint32_t dim_idx, uint64_t old_min, uint64_t old_max);
    void restore_aabb(size_t target_log_size);
    void skip_current_branch(const AABB& query_box);
};
