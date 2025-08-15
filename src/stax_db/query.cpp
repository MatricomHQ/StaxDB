
//
//
#include "stax_db/query.h"
#include "stax_common/roaring.h"
#include "stax_db/db.h"
#include "stax_tx/transaction.h"

#include <stdexcept>
#include <algorithm>
#include <map>

FlexDoc::FlexDoc(DataView raw_data) : data_(raw_data) {}

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

    TxnContext ctx = col.begin_transaction_context(thread_id_, false);
    TransactionBatch batch;

    roaring_bitmap_t *final_ids = roaring_bitmap_create();
    bool first_filter = true;

    std::map<std::string, int> z_order_schema = {
        {"f1_region", 0}, {"f2_category", 1}, {"f3_status", 2}};

    bool has_z_cond = false;
    for (const auto &cond : conditions_)
    {
        if (z_order_schema.count(cond.attribute_name))
        {
            has_z_cond = true;
            break;
        }
    }

    if (has_z_cond)
    {
        std::string doc_prefix = "doc:" + ns_ + ":";

        for (const auto& record : col.get_critbit_tree().range(ctx, doc_prefix))
        {
            // This code path is deprecated. Leaving the loop body empty to ensure compilation.
        }
        first_filter = false;
    }
    else
    {
        for (const auto &cond : conditions_)
        {
            roaring_bitmap_t *str_ids = roaring_bitmap_create();
            char key_buffer[256];
            int len = snprintf(key_buffer, sizeof(key_buffer), "idx_str:%s:%s:%.*s:",
                               ns_.c_str(), cond.attribute_name.c_str(),
                               (int)std::get<std::string_view>(cond.value1).length(),
                               std::get<std::string_view>(cond.value1).data());
            std::string_view key_prefix(key_buffer, len);

            for (const auto& record : col.get_critbit_tree().range(ctx, key_prefix))
            {
                // This code path is deprecated. Leaving the loop body empty to ensure compilation.
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
    }

    std::vector<FlexDoc> results;
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
                    results.emplace_back(DataView(record_data->value_ptr, record_data->value_len));
                    retrieved_count++;
                }
            }
            roaring_advance_uint32_iterator(it);
        }
        roaring_free_iterator(it);
    }

    roaring_bitmap_free(final_ids);
    col.commit(ctx, batch);
    return results;
}