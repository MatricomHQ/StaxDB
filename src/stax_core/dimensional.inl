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
void StaxCursor<Tree>::seek_first() {
    std::optional<StaxPath> path = tree_->find_path_for_seek(start_key_, true);
    if (path && path->leaf_handle != 0) {
        stack_.clear();
        aabb_change_log_size_ = 0;
        if (track_aabb_) {
            current_aabb_ = std::make_unique<AABB>(tree_->get_dimensionality());
        }

        uint32_t key_nibble_offset = 0;
        for (const auto& p : path->frames) {
            InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(p.node_ptr));
            stack_.push_back({p.node_ptr, p.nibble_in_parent, aabb_change_log_size_, key_nibble_offset});

            if (track_aabb_) {
                // The fragment is not interleaved and cannot be used to refine the AABB.
                if (p.nibble_in_parent != -1) {
                    refine_box_for_nibble(
                        *current_aabb_,
                        StaxTree16::get_test_idx(p.node_ptr), // Use parent's test_idx
                        p.nibble_in_parent,
                        tree_->get_dimensionality(), this
                    );
                }
            }
        }
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
    // A proper implementation would find the key <= end_key and then right-most descend if needed.
    // This simplified version just does a full right-most descent.
    stack_.clear();
    aabb_change_log_size_ = 0;
    uint64_t current_ptr = tree_->get_root_ptr().load(std::memory_order_acquire);
    int last_nibble = -1;
    uint32_t key_nibble_offset = 0;

    while (current_ptr != 0 && !StaxTree16::is_leaf(current_ptr)) {
         if (track_aabb_ && last_nibble != -1) {
            uint32_t test_idx = StaxTree16::get_test_idx(stack_.back().node_ptr);
            refine_box_for_nibble(*current_aabb_, test_idx, last_nibble, tree_->get_dimensionality(), this);
        }
        stack_.push_back({current_ptr, last_nibble, aabb_change_log_size_, key_nibble_offset});

        InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
        bool found = false;
        for(int i=15; i>=0; --i) {
            uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
            if (next_ptr != 0) {
                last_nibble = i;
                current_ptr = next_ptr;
                found = true;
                break;
            }
        }
        if (!found) { current_record_handle_ = nullptr; return; }
    }
    current_record_handle_ = reinterpret_cast<void*>(current_ptr);
    if(is_valid()){
        StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(current_record_handle_)));
        if(record->get_key() < start_key_){
             current_record_handle_ = nullptr;
        }
    }
}


template<typename Tree>
void StaxCursor<Tree>::move_next() {
    if (!is_valid()) return;

    if (track_aabb_ && !stack_.empty()) {
        restore_aabb(stack_.back().aabb_log_size);
    }

    while (!stack_.empty()) {
        CursorFrame last_frame = stack_.back();
        stack_.pop_back();

        InternalNode* parent_node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(last_frame.node_ptr));
        uint32_t parent_test_idx = StaxTree16::get_test_idx(last_frame.node_ptr);

        for (int nibble = last_frame.nibble_in_parent + 1; nibble < 16; ++nibble) {
            uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
            if (child_ptr != 0) {
                if (track_aabb_) {
                    refine_box_for_nibble(*current_aabb_, parent_test_idx, nibble, tree_->get_dimensionality(), this);
                }
                stack_.push_back({last_frame.node_ptr, nibble, aabb_change_log_size_, 0});

                uint64_t current_ptr = child_ptr;

                while (!StaxTree16::is_leaf(current_ptr)) {
                    InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
                    uint32_t current_test_idx = StaxTree16::get_test_idx(current_ptr);

                    bool found_child = false;
                    for (int i = 0; i < 16; ++i) {
                        uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                        if (next_ptr != 0) {
                            if (track_aabb_) {
                                refine_box_for_nibble(*current_aabb_, current_test_idx, i, tree_->get_dimensionality(), this);
                            }
                            stack_.push_back({current_ptr, i, aabb_change_log_size_, 0});
                            current_ptr = next_ptr;
                            found_child = true;
                            break;
                        }
                    }
                    if (!found_child) { current_record_handle_ = nullptr; return; }
                }

                StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(current_ptr));
                if (record->get_key() > end_key_) { current_record_handle_ = nullptr; return; }

                current_record_handle_ = reinterpret_cast<void*>(current_ptr);
                return;
            }
        }
        if (track_aabb_ && !stack_.empty()) {
            restore_aabb(stack_.back().aabb_log_size);
        }
    }
    current_record_handle_ = nullptr;
}

template<typename Tree>
void StaxCursor<Tree>::move_prev() {
    if (!is_valid()) return;

    if (track_aabb_ && !stack_.empty()) {
        restore_aabb(stack_.back().aabb_log_size);
    }

    while (!stack_.empty()) {
        CursorFrame last_frame = stack_.back();
        stack_.pop_back();

        InternalNode* parent_node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(last_frame.node_ptr));
        uint32_t parent_test_idx = StaxTree16::get_test_idx(last_frame.node_ptr);

        for (int nibble = last_frame.nibble_in_parent - 1; nibble >= 0; --nibble) {
            uint64_t child_ptr = parent_node->children[nibble].load(std::memory_order_acquire);
            if (child_ptr != 0) {
                 if (track_aabb_) {
                    refine_box_for_nibble(*current_aabb_, parent_test_idx, nibble, tree_->get_dimensionality(), this);
                }
                stack_.push_back({last_frame.node_ptr, nibble, aabb_change_log_size_, 0});

                uint64_t current_ptr = child_ptr;

                while (!StaxTree16::is_leaf(current_ptr)) {
                    InternalNode* node = tree_->get_allocator().template get_ptr<InternalNode>(StaxTree16::get_offset(current_ptr));
                    uint32_t current_test_idx = StaxTree16::get_test_idx(current_ptr);

                    bool found_child = false;
                    for (int i = 15; i >= 0; --i) {
                        uint64_t next_ptr = node->children[i].load(std::memory_order_acquire);
                        if (next_ptr != 0) {
                             if (track_aabb_) {
                                refine_box_for_nibble(*current_aabb_, current_test_idx, i, tree_->get_dimensionality(), this);
                            }
                            stack_.push_back({current_ptr, i, aabb_change_log_size_, 0});
                            current_ptr = next_ptr;
                            found_child = true;
                            break;
                        }
                    }
                    if (!found_child) { current_record_handle_ = nullptr; return; }
                }

                StaxRecord* record = tree_->get_allocator().template get_ptr<StaxRecord>(StaxTree16::get_offset(current_ptr));
                if (record->get_key() < start_key_) { current_record_handle_ = nullptr; return; }

                current_record_handle_ = reinterpret_cast<void*>(current_ptr);
                return;
            }
        }

        if (track_aabb_ && !stack_.empty()) {
            restore_aabb(stack_.back().aabb_log_size);
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

template<typename Tree>
void StaxCursor<Tree>::log_aabb_change(uint32_t dim_idx, uint64_t old_min, uint64_t old_max) {
    if (aabb_change_log_size_ >= MAX_AABB_CHANGES) throw std::runtime_error("AABB change log exhausted.");
    aabb_change_log_[aabb_change_log_size_++] = {dim_idx, old_min, old_max};
}

template<typename Tree>
void StaxCursor<Tree>::restore_aabb(size_t target_log_size) {
    if (!track_aabb_) return;
    while (aabb_change_log_size_ > target_log_size) {
        const auto& change = aabb_change_log_[--aabb_change_log_size_];
        current_aabb_->min_bounds[change.dim_idx] = change.old_min;
        current_aabb_->max_bounds[change.dim_idx] = change.old_max;
    }
}

template<typename Tree>
void StaxCursor<Tree>::skip_current_branch(const AABB& query_box) {
    if (!is_valid() || !track_aabb_) {
        move_next();
        return;
    }

    while(!stack_.empty()) {
        restore_aabb(stack_.back().aabb_log_size);
        if (aabbs_intersect(*current_aabb_, query_box)) {
            // This parent branch is relevant.
            // The normal move_next logic will find the next leaf in this branch.
            move_next();
            return;
        }
        // This whole parent branch is irrelevant, so we ascend further.
        stack_.pop_back();
    }
    // If we reach here, no more relevant branches were found.
    current_record_handle_ = nullptr;
}

template <typename Cursor>
inline void refine_box_for_nibble(AABB& box, uint32_t key_nibble_offset, int nibble_value, uint32_t D, Cursor* cursor) {
    if (D == 0) return;

    const uint32_t dim_idx = key_nibble_offset % D;
    const uint32_t nibble_pos_in_dim = key_nibble_offset / D;

    if (nibble_pos_in_dim >= 4) return;

    const int bit_shift = 60 - (nibble_pos_in_dim * 4);
    const uint64_t min_for_nibble = static_cast<uint64_t>(nibble_value) << bit_shift;
    const uint64_t mask_for_lower_bits = (1ULL << bit_shift) - 1;
    const uint64_t max_for_nibble = min_for_nibble | mask_for_lower_bits;

    if (cursor) {
        cursor->log_aabb_change(dim_idx, box.min_bounds[dim_idx], box.max_bounds[dim_idx]);
    }

    box.min_bounds[dim_idx] = std::max(box.min_bounds[dim_idx], min_for_nibble);
    box.max_bounds[dim_idx] = std::min(box.max_bounds[dim_idx], max_for_nibble);
}
