
//
//
#include "stax_db/query.h"
#include "stax_common/roaring.h"
#include "stax_db/db.h"
#include "stax_tx/transaction.h"
#include "stax_tx/db_cursor.hpp"

#include <stdexcept>
#include <algorithm>
#include <map>

FlexDoc::FlexDoc(DataView raw_data, std::string_view primary_key) : data_(raw_data), primary_key_(primary_key) {}

std::optional<std::string_view> FlexDoc::get_field(std::string_view field_name) const
{
    std::string_view remaining = static_cast<std::string_view>(data_);
    while (!remaining.empty())
    {
        size_t pipe_pos = remaining.find('|');
        std::string_view token = remaining.substr(0, pipe_pos);

        size_t colon_pos = token.find(':');
        if (colon_pos != std::string_view::npos)
        {
            if (token.substr(0, colon_pos) == field_name)
            {
                return token.substr(colon_pos + 1);
            }
        }

        if (pipe_pos == std::string_view::npos)
            break;
        remaining = remaining.substr(pipe_pos + 1);
    }
    return std::nullopt;
}

QueryBuilder::QueryBuilder(Database *db, uint32_t collection_idx, std::string_view ns_param, size_t thread_id)
    : db_(db), collection_idx_(collection_idx), ns_(ns_param), thread_id_(thread_id)
{
}

QueryBuilder &QueryBuilder::where(std::string_view attribute, QueryOp op, uint64_t value)
{
    conditions_.push_back({std::string(attribute), op, value, std::nullopt});
    return *this;
}

QueryBuilder &QueryBuilder::where(std::string_view attribute, QueryOp op, uint64_t val1, uint64_t val2)
{
    conditions_.push_back({std::string(attribute), op, val1, val2});
    return *this;
}

QueryBuilder &QueryBuilder::where_string(std::string_view attribute, QueryOp op, std::string_view value)
{
    conditions_.push_back({std::string(attribute), op, value, std::nullopt});
    return *this;
}

QueryBuilder &QueryBuilder::where_string(std::string_view attribute, QueryOp op, std::string_view val1, std::string_view val2)
{
    conditions_.push_back({std::string(attribute), op, val1, val2});
    return *this;
}

QueryBuilder &QueryBuilder::limit(size_t max_results)
{
    limit_ = max_results;
    return *this;
}

QueryBuilder &QueryBuilder::select(std::initializer_list<std::string_view> fields)
{
    select_fields_.reserve(fields.size());
    for (const auto &f : fields)
    {
        select_fields_.emplace_back(f);
    }
    return *this;
}

std::vector<FlexDoc> QueryBuilder::execute()
{
    if (!db_)
        throw std::runtime_error("Database not set for QueryBuilder");

    Collection &col = db_->get_collection_by_idx(collection_idx_);
    TxnContext ctx = col.begin_transaction_context(thread_id_, true); // Read-only is fine for queries
    std::vector<FlexDoc> results;

    // Fast path for primary key range scan
    if (conditions_.size() == 1 && conditions_[0].attribute_name == "primary_key" && conditions_[0].op == QueryOp::BETWEEN) {
        const auto& cond = conditions_[0];
        if (std::holds_alternative<std::string_view>(cond.value1) && cond.value2 && std::holds_alternative<std::string_view>(*cond.value2)) {
            std::string_view start_key = std::get<std::string_view>(cond.value1);
            std::string_view end_key = std::get<std::string_view>(*cond.value2);

            for (auto cursor = col.seek(ctx, start_key, end_key); cursor->is_valid(); cursor->next()) {
                if (results.size() >= limit_) {
                    break;
                }
                results.emplace_back(cursor->value(), cursor->key());
            }
            col.abort(ctx); // Abort read-only transaction
            return results;
        }
    }

    // Fast path for primary key prefix scan
    if (conditions_.size() == 1 && conditions_[0].attribute_name == "primary_key" && conditions_[0].op == QueryOp::PREFIX) {
        const auto& cond = conditions_[0];
        if (std::holds_alternative<std::string_view>(cond.value1)) {
            std::string_view prefix = std::get<std::string_view>(cond.value1);

            for (auto cursor = col.seek(ctx, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
                if (results.size() >= limit_) {
                    break;
                }
                results.emplace_back(cursor->value(), cursor->key());
            }
            col.abort(ctx); // Abort read-only transaction
            return results;
        }
    }

    // Existing logic with roaring bitmaps for secondary indexes
    roaring_bitmap_t *final_ids = roaring_bitmap_create();
    bool first_filter = true;

    for (const auto &cond : conditions_)
    {
        // Only handle simple string equality on secondary indexes for now.
        if (std::holds_alternative<std::string_view>(cond.value1) && cond.op == QueryOp::EQ) {
            roaring_bitmap_t *str_ids = roaring_bitmap_create();
            char key_buffer[256];
            int len = snprintf(key_buffer, sizeof(key_buffer), "idx_str:%s:%s:%.*s:",
                               ns_.c_str(), cond.attribute_name.c_str(),
                               (int)std::get<std::string_view>(cond.value1).length(),
                               std::get<std::string_view>(cond.value1).data());
            std::string_view key_prefix(key_buffer, len);

            for (auto cursor = col.seek_raw(ctx, key_prefix); cursor->is_valid() && cursor->key().starts_with(key_prefix); cursor->next())
            {
                auto key = cursor->key();
                size_t last_colon = key.find_last_of(':');
                uint64_t id = 0;
                PathEngine::value_to_uint64(key.substr(last_colon + 1), id);
                if (id != 0)
                    roaring_bitmap_add(str_ids, static_cast<uint32_t>(id));
                if (roaring_bitmap_get_cardinality(str_ids) >= limit_ * 10)
                    break;
            }

            if (first_filter)
            {
                roaring_bitmap_free(final_ids);
                final_ids = str_ids;
                first_filter = false;
            }
            else
            {
                roaring_bitmap_and_inplace(final_ids, str_ids);
                roaring_bitmap_free(str_ids);
            }
        }
        // NOTE: Other condition types (numeric, z-order) are omitted for simplicity as they were not fully implemented.
    }

    uint64_t count = roaring_bitmap_get_cardinality(final_ids);
    if (count > 0)
    {
        roaring_uint32_iterator_t *it = roaring_create_iterator(final_ids);
        size_t retrieved_count = 0;
        char doc_key_buffer[256];

        while (it->has_next && retrieved_count < limit_)
        {
            uint32_t id;
            roaring_read_uint32(it, &id);

            int len = snprintf(doc_key_buffer, sizeof(doc_key_buffer), "doc:%s:%u", ns_.c_str(), id);
            if (len > 0 && (size_t)len < sizeof(doc_key_buffer))
            {
                if (auto record_data = col.get(ctx, std::string_view(doc_key_buffer, len)))
                {
                    results.emplace_back(DataView(record_data->value_ptr, record_data->value_len), std::string_view(doc_key_buffer, len));
                    retrieved_count++;
                }
            }
            roaring_advance_uint32_iterator(it);
        }
        roaring_free_iterator(it);
    }

    roaring_bitmap_free(final_ids);
    col.abort(ctx); // Abort read-only transaction
    return results;
}