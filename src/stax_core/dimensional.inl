#pragma once

#include "stax_new_tree.hpp"
#include <stdexcept>

// =================================================================================================
// --- StaxCursor Implementation ---
// =================================================================================================

template<typename Tree>
StaxCursor<Tree>::StaxCursor(const Tree* tree, std::string_view start_key, std::string_view end_key, bool track_aabb)
    : tree_(tree),
      track_aabb_(track_aabb),
      current_record_handle_(nullptr),
      start_key_(start_key),
      end_key_(end_key) {
    if (track_aabb_) {
        current_aabb_ = std::make_unique<AABB>(tree_->get_dimensionality());
    }
}

template<typename Tree>
std::string StaxCursor<Tree>::stack_to_string() const {
    std::stringstream ss;
    ss << "Stack (size " << stack_.size() << "): [";
    for(size_t i=0; i<stack_.size(); ++i) {
        ss << "{" << (void*)stack_[i].node_ptr << ", nib:" << stack_[i].nibble_in_parent << ", t_idx:" << stack_[i].test_idx << "}";
        if (i < stack_.size() - 1) ss << ", ";
    }
    ss << "]";
    return ss.str();
}

template<typename Tree>
void StaxCursor<Tree>::recompute_aabb_from_stack() {
    if (!track_aabb_) return;
    current_aabb_->reset();
    for (size_t i = 0; i < stack_.size(); ++i) {
        const auto& frame = stack_[i];
        if (frame.nibble_in_parent != -1) {
            // The test_idx that decided this nibble belongs to the parent frame.
            const auto& parent_frame = stack_[i-1];
            refine_box_for_nibble(*current_aabb_, parent_frame.test_idx, frame.nibble_in_parent, tree_->get_dimensionality());
        }
    }
}


template<typename Tree>
void StaxCursor<Tree>::seek_first() {
    std::optional<StaxPath> path = tree_->find_path_for_seek(start_key_, true);
    if (path && path->leaf_handle != 0) {
        stack_.clear();
        for (const auto& frame : path->frames) {
            stack_.push_back({frame.node_ptr, frame.nibble_in_parent, frame.test_idx});
        }
        // Add a final "frame" for the leaf itself to complete the path
        stack_.push_back({path->leaf_handle, path->leaf_nibble_in_parent, 0});

        recompute_aabb_from_stack();

        // Pop the leaf frame, as the stack should only contain internal nodes
        stack_.pop_back();

        current_record_handle_ = reinterpret_cast<void*>(path->leaf_handle);
        StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(path->leaf_handle));
        if (record->get_key() > end_key_) {
            current_record_handle_ = nullptr;
        }
    } else {
        current_record_handle_ = nullptr;
    }
}

template<typename Tree>
void StaxCursor<Tree>::seek_last() {
    std::optional<StaxPath> path = tree_->find_path_for_seek_last(end_key_, true);
    if (path && path->leaf_handle != 0) {
        stack_.clear();
        for (const auto& frame : path->frames) {
            stack_.push_back({frame.node_ptr, frame.nibble_in_parent, frame.test_idx});
        }
        stack_.push_back({path->leaf_handle, path->leaf_nibble_in_parent, 0});
        recompute_aabb_from_stack();
        stack_.pop_back();

        current_record_handle_ = reinterpret_cast<void*>(path->leaf_handle);
        StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(path->leaf_handle));
        if (record->get_key() < start_key_) {
            current_record_handle_ = nullptr;
        }
    } else {
        current_record_handle_ = nullptr;
    }
}


template<typename Tree>
void StaxCursor<Tree>::move_next() {
    if (!is_valid()) return;

    while (!stack_.empty()) {
        CursorFrame last_frame = stack_.back();
        stack_.pop_back();

        InternalNode* parent_node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(last_frame.node_ptr));

        for (int nibble = last_frame.nibble_in_parent + 1; nibble < 16; ++nibble) {
            uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
            if (child_ptr != 0) {
                stack_.push_back({last_frame.node_ptr, nibble, last_frame.test_idx});
                uint64_t current_ptr = child_ptr;

                while (!StaxTree16::is_leaf(current_ptr)) {
                    uint32_t test_idx = StaxTree16::get_test_idx(current_ptr);
                    InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
                    bool found_child = false;
                    for (int i = 0; i < 16; ++i) {
                        uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                        if (next_ptr != 0) {
                            stack_.push_back({current_ptr, i, test_idx});
                            current_ptr = next_ptr;
                            found_child = true;
                            break;
                        }
                    }
                    if (!found_child) { current_record_handle_ = nullptr; return; }
                }

                stack_.push_back({current_ptr, -1, 0}); // Add leaf to stack for recomputation
                recompute_aabb_from_stack();
                stack_.pop_back(); // Remove leaf from stack

                StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(current_ptr));
                if (record->get_key() > end_key_) { current_record_handle_ = nullptr; return; }
                current_record_handle_ = reinterpret_cast<void*>(current_ptr);
                return;
            }
        }
    }
    current_record_handle_ = nullptr;
}

template<typename Tree>
void StaxCursor<Tree>::move_prev() {
    if (!is_valid()) return;

    while (!stack_.empty()) {
        CursorFrame last_frame = stack_.back();
        stack_.pop_back();

        InternalNode* parent_node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(last_frame.node_ptr));

        for (int nibble = last_frame.nibble_in_parent - 1; nibble >= 0; --nibble) {
            uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
            if (child_ptr != 0) {
                stack_.push_back({last_frame.node_ptr, nibble, last_frame.test_idx});
                uint64_t current_ptr = child_ptr;

                while (!StaxTree16::is_leaf(current_ptr)) {
                    uint32_t test_idx = StaxTree16::get_test_idx(current_ptr);
                    InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
                    bool found_child = false;
                    for (int i = 15; i >= 0; --i) {
                        uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                        if (next_ptr != 0) {
                            stack_.push_back({current_ptr, i, test_idx});
                            current_ptr = next_ptr;
                            found_child = true;
                            break;
                        }
                    }
                    if (!found_child) { current_record_handle_ = nullptr; return; }
                }

                stack_.push_back({current_ptr, -1, 0});
                recompute_aabb_from_stack();
                stack_.pop_back();

                StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(current_ptr));
                if (record->get_key() < start_key_) { current_record_handle_ = nullptr; return; }
                current_record_handle_ = reinterpret_cast<void*>(current_ptr);
                return;
            }
        }
    }
    current_record_handle_ = nullptr;
}


template<typename Tree>
bool StaxCursor<Tree>::is_valid() const {
    return current_record_handle_ != nullptr;
}

template<typename Tree>
void* StaxCursor<Tree>::get_record_handle() const {
    return current_record_handle_;
}

template<typename Tree>
const AABB* StaxCursor<Tree>::get_current_aabb() const {
    return current_aabb_.get();
}

template<typename Tree>
std::string_view StaxCursor<Tree>::get_key() const {
    if (!is_valid()) return {};
    StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(current_record_handle_)));
    return record->get_key();
}

template<typename Tree>
void StaxCursor<Tree>::skip_to(std::string_view key) {
    if (key > end_key_) {
        current_record_handle_ = nullptr;
        return;
    }
    start_key_ = key;
    seek_first();
}

inline void refine_box_for_nibble(AABB& box, uint32_t test_idx, int nibble_value, uint32_t D) {
    if (D == 0) return;

    // The key contains a 2-byte (4 nibble) header. The interleaved prefix starts after this.
    const uint32_t header_nibbles = 4;
    // The prefix contains 4 nibbles per dimension.
    const uint32_t prefix_nibbles = D * 4;

    // If the split point is outside the interleaved prefix, we cannot refine the AABB.
    // This is the critical guard against using test_idx from the raw coordinate data.
    if (test_idx < header_nibbles || test_idx >= (header_nibbles + prefix_nibbles)) {
        return;
    }

    // Make the test_idx relative to the start of the prefix.
    const uint32_t test_idx_in_prefix = test_idx - header_nibbles;

    const uint32_t dim_idx = test_idx_in_prefix % D;
    const uint32_t nibble_pos_in_dim = test_idx_in_prefix / D;

    const int bit_shift = 60 - (nibble_pos_in_dim * 4);
    const uint64_t min_for_nibble = static_cast<uint64_t>(nibble_value) << bit_shift;
    const uint64_t mask_for_lower_bits = (1ULL << bit_shift) - 1;
    const uint64_t max_for_nibble = min_for_nibble | mask_for_lower_bits;

    box.min_bounds[dim_idx] = std::max(box.min_bounds[dim_idx], min_for_nibble);
    box.max_bounds[dim_idx] = std::min(box.max_bounds[dim_idx], max_for_nibble);
}
