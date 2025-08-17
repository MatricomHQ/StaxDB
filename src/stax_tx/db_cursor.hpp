#pragma once

#include <string_view>
#include <stack>
#include <vector>
#include <memory>
#include <queue>      
#include <functional> 
#include <utility>    

#include "stax_core/stax_new_tree.hpp"
#include "stax_common/common_types.hpp"
#include "stax_tx/transaction.h"
#include "stax_db/db.h"

class StaxTree16;
class MergedCursorImpl;

namespace { 
    static const TxnContext inert_context = {0, 0, 0};
}


class DBCursor {
public:
    DBCursor();
    ~DBCursor();
    
    DBCursor(DBCursor&& other) noexcept;
    DBCursor& operator=(DBCursor&& other) noexcept;

    DBCursor(const DBCursor&) = delete;
    DBCursor& operator=(const DBCursor&) = delete;

    bool is_valid() const;
    std::string_view key() const;
    DataView value() const;
    void next();
    StaxRecord* get_current_record() const { return current_record_; }
    
    DBCursor(StaxTree16* tree, const TxnContext& ctx, std::string_view start_key, std::optional<std::string_view> end_key);

private:
    friend class Collection;
    friend class Database; 

    void find_initial_leaf();
    void advance_to_next_valid(bool is_initial_seek = false);
    bool is_visible(StaxRecord* record);

    StaxTree16* tree_ = nullptr;
    const TxnContext& ctx_;
    bool is_valid_ = false;

    std::stack<std::pair<uint64_t, int>> path_stack_;
    StaxRecord* current_record_ = nullptr;
    
    std::string start_key_buffer_;
    std::string_view start_key_view_;

    std::string end_key_buffer_;
    std::string_view end_key_view_;
    bool has_end_key_ = false;
};

inline DBCursor::DBCursor(DBCursor&& other) noexcept
    : tree_(other.tree_),
      ctx_(other.ctx_),
      is_valid_(other.is_valid_),
      path_stack_(std::move(other.path_stack_)),
      current_record_(other.current_record_),
      start_key_buffer_(std::move(other.start_key_buffer_)),
      end_key_buffer_(std::move(other.end_key_buffer_)),
      has_end_key_(other.has_end_key_)
{
    start_key_view_ = start_key_buffer_;
    if (has_end_key_) {
        end_key_view_ = end_key_buffer_;
    }
    
    other.is_valid_ = false;
    other.tree_ = nullptr;
    other.current_record_ = nullptr;
}

inline DBCursor& DBCursor::operator=(DBCursor&& other) noexcept {
    if (this != &other) {
        tree_ = other.tree_;
        // ctx_ cannot be moved as it's a reference
        is_valid_ = other.is_valid_;
        path_stack_ = std::move(other.path_stack_);
        current_record_ = other.current_record_;
        start_key_buffer_ = std::move(other.start_key_buffer_);
        end_key_buffer_ = std::move(other.end_key_buffer_);
        has_end_key_ = other.has_end_key_;

        start_key_view_ = start_key_buffer_;
        if (has_end_key_) {
            end_key_view_ = end_key_buffer_;
        }
        
        other.is_valid_ = false;
        other.tree_ = nullptr;
        other.current_record_ = nullptr;
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