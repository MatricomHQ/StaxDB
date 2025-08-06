#pragma once

#include <string_view>
#include <stack>
#include <vector>
#include <memory>
#include <queue>      
#include <functional> 
#include <utility>    

#include "stax_core/value_store.hpp"
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

    std::stack<uint64_t, std::vector<uint64_t>> path_stack_;
    
    RecordData current_record_data_;

    const char* current_key_ptr_ = nullptr;
    uint16_t current_key_len_ = 0;
    
    std::string end_key_buffer_;
    std::string_view end_key_view_;
    bool has_end_key_ = false;
};


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