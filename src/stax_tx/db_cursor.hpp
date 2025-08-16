#pragma once

#include <string_view>
#include <stack>
#include <vector>
#include <memory>
#include <queue>      
#include <functional> 
#include <utility>    

#include "stax_core/stax_tree.hpp"
#include "stax_common/common_types.hpp"
#include "stax_tx/transaction.h" 
#include "stax_db/db.h"            


class StaxTree;
class MergedCursorImpl;

namespace { 
    static const TxnContext inert_context = {0, 0, 0};
}


class DBCursor {
public:
    DBCursor() : ctx_(inert_context) {}
    ~DBCursor() = default;
    
    DBCursor(DBCursor&& other) noexcept;
    DBCursor& operator=(DBCursor&& other) noexcept;

    DBCursor(const DBCursor&) = delete;
    DBCursor& operator=(const DBCursor&) = delete;

    bool is_valid() const;
    std::string_view key() const;
    DataView value() const;
    void next();
    
    
    DBCursor(Database* db, const TxnContext& ctx, uint32_t collection_idx, std::string_view start_key, std::optional<std::string_view> end_key);
    DBCursor(Database* db, const TxnContext& ctx, StaxTree* tree, std::optional<std::string_view> end_key, bool raw_mode = false); 
    DBCursor(Database* db, const TxnContext& ctx, StaxTree* tree, std::string_view start_key, std::optional<std::string_view> end_key, bool raw_mode = false);


private:
    friend class Collection;
    friend class MergedCursorImpl;
    friend class Database; 

    void validate_current_leaf();
    void advance_to_next_physical_leaf();

    std::unique_ptr<MergedCursorImpl> impl_;
    
    Database* db_ = nullptr;
    const TxnContext& ctx_;
    StaxTree* tree_ = nullptr; 
    bool is_valid_ = false;
    bool raw_mode_ = false;

    std::stack<std::pair<uint64_t, int>, std::vector<std::pair<uint64_t, int>>> path_stack_;
    
    RecordData current_record_data_;

    const char* current_key_ptr_ = nullptr;
    uint16_t current_key_len_ = 0;
    
    std::string end_key_buffer_;
    std::string_view end_key_view_;
    bool has_end_key_ = false;
};

inline DBCursor::DBCursor(Database* db, const TxnContext& ctx, StaxTree* tree, std::string_view start_key, std::optional<std::string_view> end_key, bool raw_mode)
    : db_(db), ctx_(ctx), tree_(tree), raw_mode_(raw_mode) {
    if (end_key) {
        has_end_key_ = true;
        end_key_buffer_ = *end_key;
        end_key_view_ = end_key_buffer_;
    }
    tree->seek(start_key, path_stack_);
    if (!path_stack_.empty()) {
        validate_current_leaf();
    } else {
        is_valid_ = false;
    }
}

inline void DBCursor::validate_current_leaf() {
    is_valid_ = false;
    if (path_stack_.empty()) return;

    uint64_t leaf_ptr = path_stack_.top().first;
    StaxRecord* record = tree_->get_allocator().get_ptr<StaxRecord>(StaxTree::get_offset(leaf_ptr));

    current_key_ptr_ = record->get_key_data();
    current_key_len_ = record->key_len;
    if (has_end_key_ && key() >= end_key_view_) {
        is_valid_ = false;
        return;
    }

    while (true) {
        // Version visibility check
        if (raw_mode_ || record->txn_id <= ctx_.read_snapshot_id) {
            if (record->is_deleted) {
                // Tombstone found, advance to the next physical leaf
                advance_to_next_physical_leaf();
                return;
            }

            current_record_data_.key_ptr = current_key_ptr_;
            current_record_data_.key_len = current_key_len_;
            current_record_data_.value_ptr = record->get_value_data();
            current_record_data_.value_len = record->value_len;
            current_record_data_.txn_id = record->txn_id;
            current_record_data_.prev_version_offset = record->prev_version_offset;
            current_record_data_.is_deleted = record->is_deleted;
            is_valid_ = true;
            return;
        }

        if (record->prev_version_offset == 0) {
            // No older version, advance to next leaf
            advance_to_next_physical_leaf();
            return;
        }
        record = tree_->get_allocator().get_ptr<StaxRecord>(record->prev_version_offset);
    }
}

inline void DBCursor::advance_to_next_physical_leaf() {
    if (path_stack_.empty()) {
        is_valid_ = false;
        return;
    }

    // Backtrack up the stack to find the next sibling
    path_stack_.pop();

    while (!path_stack_.empty()) {
        auto& [parent_ptr, nibble_idx] = path_stack_.top();
        InternalNode* parent_node = tree_->get_allocator().get_ptr<InternalNode>(StaxTree::get_offset(parent_ptr));

        for (int i = nibble_idx; i < 16; ++i) {
            uint64_t child_ptr = parent_node->children[i].load(std::memory_order_relaxed);
            if (child_ptr != 0) {
                path_stack_.top().second = i + 1;
                path_stack_.push({child_ptr, 0});
                // Found the next branch, now dive down to the first leaf
                while (true) {
                    uint64_t dive_ptr = path_stack_.top().first;
                    if (StaxTree::is_leaf(dive_ptr)) {
                        validate_current_leaf();
                        return;
                    }
                    InternalNode* dive_node = tree_->get_allocator().get_ptr<InternalNode>(StaxTree::get_offset(dive_ptr));
                    bool found_child = false;
                    for (int j = 0; j < 16; ++j) {
                        uint64_t next_dive_ptr = dive_node->children[j].load(std::memory_order_relaxed);
                        if (next_dive_ptr != 0) {
                            path_stack_.top().second = j + 1;
                            path_stack_.push({next_dive_ptr, 0});
                            found_child = true;
                            break;
                        }
                    }
                    if (!found_child) { // Should not happen in a well-formed tree
                        is_valid_ = false;
                        return;
                    }
                }
            }
        }
        path_stack_.pop();
    }
    is_valid_ = false; // No more leaves
}




inline DBCursor::DBCursor(DBCursor&& other) noexcept
    : impl_(std::move(other.impl_)),
      db_(other.db_),
      ctx_(other.ctx_), 
      tree_(other.tree_),
      is_valid_(other.is_valid_),
      raw_mode_(other.raw_mode_),
      path_stack_(std::move(other.path_stack_)),
      current_record_data_(other.current_record_data_),
      current_key_ptr_(other.current_key_ptr_),
      current_key_len_(other.current_key_len_),
      end_key_buffer_(std::move(other.end_key_buffer_)),
      
      has_end_key_(other.has_end_key_)
{
    
    
    if (has_end_key_) {
        end_key_view_ = end_key_buffer_;
    }

    
    other.is_valid_ = false;
    other.tree_ = nullptr;
    other.db_ = nullptr;
}

inline DBCursor& DBCursor::operator=(DBCursor&& other) noexcept {
    if (this != &other) {
        
        impl_.reset(); 

        impl_ = std::move(other.impl_);
        db_ = other.db_;
        
        
        
        tree_ = other.tree_;
        is_valid_ = other.is_valid_;
        raw_mode_ = other.raw_mode_;
        path_stack_ = std::move(other.path_stack_);
        current_record_data_ = other.current_record_data_;
        current_key_ptr_ = other.current_key_ptr_;
        current_key_len_ = other.current_key_len_;
        end_key_buffer_ = std::move(other.end_key_buffer_);
        has_end_key_ = other.has_end_key_;
        
        
        
        if (has_end_key_) {
            end_key_view_ = end_key_buffer_;
        } else {
            end_key_view_ = {};
        }

        
        other.is_valid_ = false;
        other.tree_ = nullptr;
        other.db_ = nullptr;
    }
    return *this;
}


struct MergeCursorState {
    DBCursor cursor;
    size_t generation_index;

    bool operator>(const MergeCursorState& other) const {
        if (!cursor.is_valid() && other.cursor.is_valid()) return true;
        if (cursor.is_valid() && !other.cursor.is_valid()) return false;
        if (!cursor.is_valid() && !other.cursor.is_valid()) return false;
        
        int key_cmp = cursor.key().compare(other.cursor.key());
        if (key_cmp != 0) return key_cmp > 0;
        
        return generation_index > other.generation_index;
    }
};

class MergedCursorImpl {
public:
    Database* db_;
    const TxnContext& ctx_;

    std::priority_queue<MergeCursorState, std::vector<MergeCursorState>, std::greater<MergeCursorState>> pq_;
    std::string last_key_buffer_;
    std::string_view last_key_view_;
    RecordData current_record_data_;
    bool is_valid_ = false;

    std::string end_key_buffer_;
    std::string_view end_key_view_;
    bool has_end_key_ = false;

    MergedCursorImpl(Database* db, const TxnContext& ctx, uint32_t collection_idx, std::string_view start_key, std::optional<std::string_view> end_key);
    void advance();
};