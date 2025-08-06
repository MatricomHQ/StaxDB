#include "stax_graph/graph_engine.h"
#include "stax_tx/db_cursor.hpp"
#include "stax_common/binary_utils.h"
#include <cassert>
#include <stdexcept>
#include <algorithm>
#include <utility>
#include <future>
#include <queue>
#include <map>
#include <set>

#if defined(_WIN32)
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#include <stdlib.h>
#define htobe64(x) _byteswap_uint64(x)
#define be64toh(x) _byteswap_uint64(x)
#else
#include <arpa/inet.h>
#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define htobe64(x) OSSwapHostToBigInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#else
#include <endian.h>
#endif
#endif

static constexpr char OFV_PROPERTY_PREFIX = 'p';
static constexpr char OFV_RELATIONSHIP_PREFIX = 'r';

static bool roaring_container_contains_internal(const roaring_container_t *c, uint16_t val)
{
    if (c->is_bitset)
    {
        return (c->container_data.bitset.bitset[val / 64] & (1ULL << (val % 64))) != 0;
    }
    else
    {

        const array_container_t *array_c = &c->container_data.array;
        int32_t low = 0;
        int32_t high = array_c->cardinality - 1;
        while (low <= high)
        {
            int32_t mid = low + (high - low) / 2;
            if (array_c->content[mid] == val)
                return true;
            if (array_c->content[mid] < val)
                low = mid + 1;
            else
                high = mid + 1;
        }
        return false;
    }
}

static bool roaring_bitmap_contains_internal(const roaring_bitmap_t *r, uint32_t val)
{
    if (!r)
        return false;
    uint16_t high = val >> 16;
    uint16_t low = val & 0xFFFF;
    const roaring_array_t *ra = &r->high_low_container;

    int32_t low_idx = 0;
    int32_t high_idx = ra->num_containers - 1;
    while (low_idx <= high_idx)
    {
        int32_t mid_idx = low_idx + (high_idx - low_idx) / 2;
        if (ra->keys[mid_idx] == high)
        {
            return roaring_container_contains_internal(ra->containers[mid_idx], low);
        }
        if (ra->keys[mid_idx] < high)
            low_idx = mid_idx + 1;
        else
            high_idx = mid_idx - 1;
    }
    return false;
}

uint32_t hash_fnv1a_32(std::string_view s)
{
    uint32_t hash = 2166136261u;
    for (char c : s)
    {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

QueryPipeline::QueryPipeline(std::unique_ptr<QueryOperator> source) : source_(std::move(source)) {}

bool QueryPipeline::next(uint32_t &out_id)
{
    if (!source_)
        return false;
    return source_->next(out_id);
}

IndexScanOperator::IndexScanOperator(Collection *col, const TxnContext &ctx, uint32_t field_id, uint32_t value_id)
    : col_(col), ctx_(ctx), field_id_(field_id), value_id_(value_id)
{
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE * 2];
    size_t key_prefix_len = to_binary_key_buf(field_id, prefix_buf, sizeof(prefix_buf));
    key_prefix_len += to_binary_key_buf(value_id, prefix_buf + key_prefix_len, sizeof(prefix_buf) - key_prefix_len);
    key_prefix_ = std::string(prefix_buf, key_prefix_len);
    reset();
}

bool IndexScanOperator::next(uint32_t &out_id)
{
    if (cursor_ && cursor_->is_valid())
    {
        std::string_view key = cursor_->key();
        if (key.length() == GraphTransaction::BINARY_U32_SIZE * 3 && key.starts_with(key_prefix_))
        {
            out_id = from_binary_key_u32(key.substr(GraphTransaction::BINARY_U32_SIZE * 2));
            cursor_->next();
            return true;
        }
    }
    return false;
}

void IndexScanOperator::reset()
{
    cursor_ = col_->seek(ctx_, key_prefix_);
}

ForwardScanOperator::ForwardScanOperator(Collection *col, const TxnContext &ctx, uint32_t source_id, uint32_t field_id)
    : col_(col), ctx_(ctx), source_id_(source_id), field_id_(field_id)
{

    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE];
    size_t key_prefix_len = to_binary_key_buf(source_id_, prefix_buf, sizeof(prefix_buf));
    prefix_buf[key_prefix_len++] = OFV_RELATIONSHIP_PREFIX;
    key_prefix_len += to_binary_key_buf(field_id_, prefix_buf + key_prefix_len, sizeof(prefix_buf) - key_prefix_len);
    key_prefix_ = std::string(prefix_buf, key_prefix_len);
    reset();
}

bool ForwardScanOperator::next(uint32_t &out_id)
{
    if (cursor_ && cursor_->is_valid())
    {
        std::string_view key = cursor_->key();

        if (key.length() == GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE * 2 && key.starts_with(key_prefix_))
        {
            out_id = from_binary_key_u32(key.substr(GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE));
            cursor_->next();
            return true;
        }
    }
    return false;
}

void ForwardScanOperator::reset()
{
    cursor_ = col_->seek(ctx_, key_prefix_);
}

IntersectOperator::IntersectOperator(std::unique_ptr<QueryOperator> left, std::unique_ptr<QueryOperator> right)
    : left_(std::move(left)), right_(std::move(right))
{
    reset();
}

void IntersectOperator::reset()
{
    left_->reset();
    right_->reset();
    left_valid_ = left_->next(left_val_);
    right_valid_ = right_->next(right_val_);
}

bool IntersectOperator::next(uint32_t &out_id)
{
    while (left_valid_ && right_valid_)
    {
        if (left_val_ < right_val_)
        {
            left_valid_ = left_->next(left_val_);
        }
        else if (right_val_ < left_val_)
        {
            right_valid_ = right_->next(right_val_);
        }
        else
        {
            out_id = left_val_;
            left_valid_ = left_->next(left_val_);
            right_valid_ = right_->next(right_val_);
            return true;
        }
    }
    return false;
}

GraphReader::GraphReader(::Database *db, const TxnContext &ctx)
    : db_(db), ctx_(ctx)
{
    ofv_col_ = db_->get_ofv_collection();
    fvo_col_ = db_->get_fvo_collection();
}

std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> GraphReader::get_properties_and_relationships(uint32_t obj_id)
{

    char prefix_buf[GraphTransaction::BINARY_U32_SIZE];
    size_t prefix_len = to_binary_key_buf(obj_id, prefix_buf, sizeof(prefix_buf));
    std::string_view prefix(prefix_buf, prefix_len);

    std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> results;

    for (auto cursor = ofv_col_->seek(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
    {
        std::string_view key_view = cursor->key();
        DataView value_view = cursor->value();

        if (key_view.length() < GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE)
        {
            continue;
        }

        char type_prefix = key_view[GraphTransaction::BINARY_U32_SIZE];

        if (type_prefix == OFV_PROPERTY_PREFIX)
        {

            if (key_view.length() != GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE)
                continue;
            
            if (value_view.len < 2) continue;

            uint32_t field_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + 1));
            uint32_t value_id;
            
            
            
            
            std::string_view value_payload(value_view.data, value_view.len);
            char value_type_code = value_payload[0];
            size_t null_terminator_pos = value_payload.find('\0', 1);
            if (null_terminator_pos == std::string_view::npos) continue;
            std::string_view value_data = value_payload.substr(null_terminator_pos + 1);


            if (value_type_code == StaxValueType::String)
            {
                value_id = hash_fnv1a_32(value_data);
            }
            else if (value_type_code == StaxValueType::Numeric || value_type_code == StaxValueType::Geo)
            {
                value_id = static_cast<uint32_t>(from_binary_key_u64(value_data));
            }
            else
            {
                continue;
            }
            results.emplace_back(obj_id, field_id, value_id);
        }
        else if (type_prefix == OFV_RELATIONSHIP_PREFIX)
        {

            if (key_view.length() != GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE * 2)
                continue;

            uint32_t field_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + 1, GraphTransaction::BINARY_U32_SIZE));
            uint32_t target_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE));
            results.emplace_back(obj_id, field_id, target_id);
        }
    }
    return results;
}

std::optional<DataView> GraphReader::get_property_for_object_direct(uint32_t obj_id, uint32_t field_id)
{
    char key_buf[GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE];
    size_t key_len = to_binary_key_buf(obj_id, key_buf, sizeof(key_buf));
    key_buf[key_len++] = OFV_PROPERTY_PREFIX;
    key_len += to_binary_key_buf(field_id, key_buf + key_len, sizeof(key_buf) - key_len);
    std::string_view key(key_buf, key_len);

    auto result = ofv_col_->get(ctx_, key);
    if (result && result->value_len > 0)
    {
        return DataView(result->value_ptr, result->value_len);
    }
    return std::nullopt;
}

std::optional<std::string_view> GraphReader::get_property_for_object_string(uint32_t obj_id, uint32_t field_id)
{
    auto result = get_property_for_object_direct(obj_id, field_id);
    if (result && result->len > 1 && result->data[0] == StaxValueType::String)
    {
        std::string_view full_payload(result->data, result->len);
        size_t null_pos = full_payload.find('\0', 1);
        if (null_pos == std::string_view::npos) return std::nullopt;
        return full_payload.substr(null_pos + 1);
    }
    return std::nullopt;
}

std::optional<uint64_t> GraphReader::get_property_for_object_numeric(uint32_t obj_id, uint32_t field_id)
{
    auto result = get_property_for_object_direct(obj_id, field_id);
    if (result && result->len > 1 && (result->data[0] == StaxValueType::Numeric || result->data[0] == StaxValueType::Geo))
    {
        std::string_view full_payload(result->data, result->len);
        size_t null_pos = full_payload.find('\0', 1);
        if (null_pos == std::string_view::npos) return std::nullopt;
        return from_binary_key_u64(full_payload.substr(null_pos + 1));
    }
    return std::nullopt;
}

std::set<uint32_t> GraphReader::get_all_relationship_types()
{
    std::set<uint32_t> rel_types;
    for (auto cursor = fvo_col_->seek_first(ctx_); cursor->is_valid(); cursor->next()) {
        std::string_view key = cursor->key();
        // Relationship keys in FVO are (field_id, target_id, source_id)
        if (key.length() == GraphTransaction::BINARY_U32_SIZE * 3) {
            rel_types.insert(from_binary_key_u32(key.substr(0, GraphTransaction::BINARY_U32_SIZE)));
        }
    }
    return rel_types;
}


std::vector<uint32_t> GraphReader::get_objects_by_property(uint32_t field_id, std::string_view value_str)
{
    return get_objects_by_property(field_id, hash_fnv1a_32(value_str));
}

std::vector<uint32_t> GraphReader::get_objects_by_property(uint32_t field_id, uint32_t value_id)
{
    roaring_bitmap_t *temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap)
        return std::vector<uint32_t>();

    get_objects_by_property_into_roaring(field_id, value_id, temp_bitmap);

    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    std::vector<uint32_t> results_vec;
    if (cardinality > 0)
    {
        results_vec.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results_vec.data());
    }

    roaring_bitmap_free(temp_bitmap);
    return results_vec;
}

void GraphReader::get_objects_by_property_into_roaring(uint32_t field_id, uint32_t value_id, roaring_bitmap_t *target_bitmap)
{
    if (!target_bitmap || value_id == 0)
        return;

    char fvo_prefix_buf[GraphTransaction::BINARY_U32_SIZE * 2];
    size_t fvo_prefix_len = to_binary_key_buf(field_id, fvo_prefix_buf, GraphTransaction::BINARY_U32_SIZE);
    fvo_prefix_len += to_binary_key_buf(value_id, fvo_prefix_buf + fvo_prefix_len, GraphTransaction::BINARY_U32_SIZE);
    std::string_view fvo_prefix(fvo_prefix_buf, fvo_prefix_len);

    for (auto cursor = fvo_col_->seek_raw(ctx_, fvo_prefix); cursor->is_valid() && cursor->key().starts_with(fvo_prefix); cursor->next())
    {
        std::string_view key_view = cursor->key();
        if (key_view.length() == GraphTransaction::BINARY_U32_SIZE * 3)
        {
            uint32_t obj_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE * 2));
            roaring_bitmap_add(target_bitmap, obj_id);
        }
    }
}

void GraphReader::get_objects_by_property_range_into_roaring(uint32_t field_id, uint64_t start_numeric_val, uint64_t end_numeric_val, roaring_bitmap_t *target_bitmap)
{
    if (!target_bitmap)
        return;

    char start_key_buf[GraphTransaction::BINARY_U32_SIZE + GraphTransaction::BINARY_U64_SIZE];
    size_t start_key_len = to_binary_key_buf(field_id, start_key_buf, sizeof(start_key_buf));
    start_key_len += to_binary_key_buf(start_numeric_val, start_key_buf + start_key_len, sizeof(start_key_buf) - start_key_len);
    std::string_view start_key(start_key_buf, start_key_len);

    char end_key_buf[GraphTransaction::BINARY_U32_SIZE + GraphTransaction::BINARY_U64_SIZE];
    size_t end_key_len = to_binary_key_buf(field_id, end_key_buf, sizeof(end_key_buf));
    end_key_len += to_binary_key_buf(end_numeric_val, end_key_buf + end_key_len, sizeof(end_key_buf) - end_key_len);

    std::string end_key_exclusive = std::string(end_key_buf, end_key_len) + '\xff';

    for (auto cursor = fvo_col_->seek_raw(ctx_, start_key, end_key_exclusive); cursor->is_valid(); cursor->next())
    {
        std::string_view key_view = cursor->key();
        if (key_view.length() == GraphTransaction::BINARY_U32_SIZE + GraphTransaction::BINARY_U64_SIZE + GraphTransaction::BINARY_U32_SIZE)
        {
            uint32_t obj_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + GraphTransaction::BINARY_U64_SIZE));
            roaring_bitmap_add(target_bitmap, obj_id);
        }
    }
}

size_t GraphReader::count_objects_by_property(uint32_t field_id, uint32_t value_id)
{
    roaring_bitmap_t *temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap)
    {
        throw std::runtime_error("Failed to create temporary roaring bitmap for count_objects_by_property.");
    }

    get_objects_by_property_into_roaring(field_id, value_id, temp_bitmap);

    uint64_t count = roaring_bitmap_get_cardinality(temp_bitmap);
    roaring_bitmap_free(temp_bitmap);

    return static_cast<size_t>(count);
}

size_t GraphReader::count_relationships_by_type(uint32_t relationship_field_id)
{
    char fvo_rel_prefix_buf[GraphTransaction::BINARY_U32_SIZE];
    size_t fvo_rel_prefix_len = to_binary_key_buf(relationship_field_id, fvo_rel_prefix_buf, sizeof(fvo_rel_prefix_buf));
    std::string_view fvo_rel_prefix(fvo_rel_prefix_buf, fvo_rel_prefix_len);

    size_t count = 0;
    for (auto cursor = fvo_col_->seek_raw(ctx_, fvo_rel_prefix); cursor->is_valid() && cursor->key().starts_with(fvo_rel_prefix); cursor->next())
    {
        count++;
    }

    return count;
}

std::vector<uint32_t> GraphReader::get_outgoing_relationships(uint32_t source_obj_id, uint32_t relationship_field_id)
{
    std::vector<uint32_t> results_vec;
    roaring_bitmap_t *temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap)
        return results_vec;

    get_outgoing_relationships_into_roaring(source_obj_id, relationship_field_id, temp_bitmap);

    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    if (cardinality > 0)
    {
        results_vec.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results_vec.data());
    }

    roaring_bitmap_free(temp_bitmap);
    return results_vec;
}

void GraphReader::get_outgoing_relationships_into_roaring(uint32_t source_obj_id, uint32_t relationship_field_id, roaring_bitmap_t *target_bitmap)
{
    if (!target_bitmap)
        return;

    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE];
    size_t prefix_len = to_binary_key_buf(source_obj_id, prefix_buf, sizeof(prefix_buf));
    prefix_buf[prefix_len++] = OFV_RELATIONSHIP_PREFIX;
    prefix_len += to_binary_key_buf(relationship_field_id, prefix_buf + prefix_len, sizeof(prefix_buf) - prefix_len);
    std::string_view prefix(prefix_buf, prefix_len);

    for (auto cursor = ofv_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
    {
        std::string_view key_view = cursor->key();

        if (key_view.length() == GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE * 2)
        {
            uint32_t target_obj_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE));
            if (target_obj_id != 0)
            {
                roaring_bitmap_add(target_bitmap, target_obj_id);
            }
        }
    }
}

void GraphReader::get_outgoing_relationships_for_many_into_roaring(roaring_bitmap_t *source_nodes, uint32_t relationship_field_id, roaring_bitmap_t *target_bitmap)
{
    if (!target_bitmap || !source_nodes)
        return;

    roaring_uint32_iterator_t *it = roaring_create_iterator(source_nodes);
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE];

    while (it->has_next)
    {
        uint32_t source_id;
        roaring_read_uint32(it, &source_id);

        size_t prefix_len = to_binary_key_buf(source_id, prefix_buf, sizeof(prefix_buf));
        prefix_buf[prefix_len++] = OFV_RELATIONSHIP_PREFIX;
        prefix_len += to_binary_key_buf(relationship_field_id, prefix_buf + prefix_len, sizeof(prefix_buf) - prefix_len);
        std::string_view prefix(prefix_buf, prefix_len);

        auto cursor = ofv_col_->seek_raw(ctx_, prefix);
        for (; cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
        {
            std::string_view key_view = cursor->key();
            if (key_view.length() == GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE * 2)
            {
                uint32_t target_obj_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE));
                if (target_obj_id != 0)
                {
                    roaring_bitmap_add(target_bitmap, target_obj_id);
                }
            }
        }
        roaring_advance_uint32_iterator(it);
    }
    roaring_free_iterator(it);
}

std::vector<uint32_t> GraphReader::get_incoming_relationships(uint32_t target_obj_id, uint32_t relationship_field_id)
{
    std::vector<uint32_t> results;
    roaring_bitmap_t *temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap)
        return results;

    roaring_bitmap_t *target_nodes_bitmap = roaring_bitmap_create();
    if (!target_nodes_bitmap)
    {
        roaring_bitmap_free(temp_bitmap);
        return results;
    }
    roaring_bitmap_add(target_nodes_bitmap, target_obj_id);

    get_incoming_relationships_for_many_into_roaring(target_nodes_bitmap, relationship_field_id, temp_bitmap);

    roaring_bitmap_free(target_nodes_bitmap);

    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    if (cardinality > 0)
    {
        results.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results.data());
    }

    roaring_bitmap_free(temp_bitmap);
    return results;
}

void GraphReader::get_incoming_relationships_for_many_into_roaring(roaring_bitmap_t *target_nodes, uint32_t relationship_field_id, roaring_bitmap_t *source_bitmap)
{
    if (!target_nodes || !source_bitmap)
        return;

    roaring_uint32_iterator_t *it = roaring_create_iterator(target_nodes);
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE * 2];
    size_t base_prefix_len = to_binary_key_buf(relationship_field_id, prefix_buf, GraphTransaction::BINARY_U32_SIZE);

    while (it->has_next)
    {
        uint32_t target_id;
        roaring_read_uint32(it, &target_id);

        uint32_t target_hash = target_id;
        size_t prefix_len = base_prefix_len + to_binary_key_buf(target_hash, prefix_buf + base_prefix_len, GraphTransaction::BINARY_U32_SIZE);
        std::string_view prefix(prefix_buf, prefix_len);

        for (auto cursor = fvo_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
        {
            std::string_view key_view = cursor->key();
            if (key_view.length() == GraphTransaction::BINARY_U32_SIZE * 3)
            {
                uint32_t source_obj_id = from_binary_key_u32(key_view.substr(GraphTransaction::BINARY_U32_SIZE * 2, GraphTransaction::BINARY_U32_SIZE));
                roaring_bitmap_add(source_bitmap, source_obj_id);
            }
        }
        roaring_advance_uint32_iterator(it);
    }
    roaring_free_iterator(it);
}

std::vector<uint32_t> GraphReader::find_shortest_path(uint32_t start_node, uint32_t end_node, uint32_t relationship_field_id)
{
    if (start_node == end_node)
        return {start_node};

    std::queue<uint32_t> q;
    std::map<uint32_t, uint32_t> predecessors;
    std::set<uint32_t> visited;

    q.push(start_node);
    visited.insert(start_node);
    predecessors[start_node] = 0;

    while (!q.empty())
    {
        uint32_t current_node = q.front();
        q.pop();

        if (current_node == end_node)
        {
            std::vector<uint32_t> path;
            uint32_t at = end_node;
            while (at != 0)
            {
                path.push_back(at);
                at = predecessors[at];
            }
            std::reverse(path.begin(), path.end());
            return path;
        }

        roaring_bitmap_t *neighbors = roaring_bitmap_create();
        get_outgoing_relationships_into_roaring(current_node, relationship_field_id, neighbors);

        roaring_uint32_iterator_t *it = roaring_create_iterator(neighbors);
        while (it->has_next)
        {
            uint32_t neighbor;
            roaring_read_uint32(it, &neighbor);
            if (visited.find(neighbor) == visited.end())
            {
                visited.insert(neighbor);
                predecessors[neighbor] = current_node;
                q.push(neighbor);
            }
            roaring_advance_uint32_iterator(it);
        }
        roaring_free_iterator(it);
        roaring_bitmap_free(neighbors);
    }

    return {};
}

uint64_t GraphReader::count_triangles(uint32_t relationship_field_id)
{
    std::atomic<uint64_t> total_triangles = 0;

    roaring_bitmap_t *all_nodes_with_edges = roaring_bitmap_create();

    char fvo_rel_prefix_buf[GraphTransaction::BINARY_U32_SIZE];
    size_t fvo_rel_prefix_len = to_binary_key_buf(relationship_field_id, fvo_rel_prefix_buf, sizeof(fvo_rel_prefix_buf));
    std::string_view fvo_rel_prefix(fvo_rel_prefix_buf, fvo_rel_prefix_len);

    for (auto cursor = fvo_col_->seek_raw(ctx_, fvo_rel_prefix); cursor->is_valid() && cursor->key().starts_with(fvo_rel_prefix); cursor->next())
    {
        std::string_view key = cursor->key();

        if (key.length() == GraphTransaction::BINARY_U32_SIZE * 3)
        {

            roaring_bitmap_add(all_nodes_with_edges, from_binary_key_u32(key.substr(GraphTransaction::BINARY_U32_SIZE * 2)));
        }
    }

    uint64_t num_nodes = roaring_bitmap_get_cardinality(all_nodes_with_edges);

    if (num_nodes < 3)
    {
        roaring_bitmap_free(all_nodes_with_edges);
        return 0;
    }

    uint32_t *nodes_array = (uint32_t *)malloc(num_nodes * sizeof(uint32_t));
    if (!nodes_array)
    {
        roaring_bitmap_free(all_nodes_with_edges);
        throw std::bad_alloc();
    }
    roaring_bitmap_to_uint32_array(all_nodes_with_edges, nodes_array);
    roaring_bitmap_free(all_nodes_with_edges);

    uint64_t local_triangle_count = 0;
    for (size_t u_idx = 0; u_idx < num_nodes; ++u_idx)
    {
        uint32_t u = nodes_array[u_idx];
        roaring_bitmap_t *u_neighbors = roaring_bitmap_create();
        get_outgoing_relationships_into_roaring(u, relationship_field_id, u_neighbors);
        if (roaring_bitmap_is_empty(u_neighbors))
        {
            roaring_bitmap_free(u_neighbors);
            continue;
        }

        roaring_uint32_iterator_t *it_v = roaring_create_iterator(u_neighbors);
        while (it_v->has_next)
        {
            uint32_t v;
            roaring_read_uint32(it_v, &v);

            roaring_bitmap_t *v_neighbors = roaring_bitmap_create();
            get_outgoing_relationships_into_roaring(v, relationship_field_id, v_neighbors);

            roaring_bitmap_t *common = roaring_bitmap_and(u_neighbors, v_neighbors);
            local_triangle_count += roaring_bitmap_get_cardinality(common);
            roaring_bitmap_free(common);

            roaring_bitmap_free(v_neighbors);
            roaring_advance_uint32_iterator(it_v);
        }
        roaring_free_iterator(it_v);
        roaring_bitmap_free(u_neighbors);
    }
    total_triangles += local_triangle_count;

    free(nodes_array);
    return total_triangles.load() / 3;
}

std::unique_ptr<QueryPipeline> GraphReader::get_common_neighbors(uint32_t node1, uint32_t node2, uint32_t relationship_field_id)
{
    auto scan1 = std::make_unique<ForwardScanOperator>(ofv_col_, ctx_, node1, relationship_field_id);
    auto scan2 = std::make_unique<ForwardScanOperator>(ofv_col_, ctx_, node2, relationship_field_id);

    auto intersect = std::make_unique<IntersectOperator>(std::move(scan1), std::move(scan2));

    return std::make_unique<QueryPipeline>(std::move(intersect));
}

bool GraphReader::has_relationship(uint32_t source_obj_id, uint32_t relationship_field_id, uint32_t target_obj_id)
{

    char key_buf[GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE * 2];
    size_t key_len = to_binary_key_buf(source_obj_id, key_buf, sizeof(key_buf));
    key_buf[key_len++] = OFV_RELATIONSHIP_PREFIX;
    key_len += to_binary_key_buf(relationship_field_id, key_buf + key_len, sizeof(key_buf) - key_len);
    key_len += to_binary_key_buf(target_obj_id, key_buf + key_len, sizeof(key_buf) - key_len);
    std::string_view key(key_buf, key_len);

    return ofv_col_->get(ctx_, key).has_value();
}

static const char FVO_PLACEHOLDER_VALUE = '1';

GraphTransaction::GraphTransaction(::Database *db, size_t thread_id)
    : db_(db), thread_id_(thread_id),
      ctx_(db->begin_transaction_context(thread_id, false)),
      ofv_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      fvo_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      ofv_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH)),
      fvo_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH))
{
    ofv_col_ = db_->get_ofv_collection();
    fvo_col_ = db_->get_fvo_collection();

    if (ofv_col_)
        ofv_col_idx_ = ofv_col_->get_id();
    if (fvo_col_)
        fvo_col_idx_ = fvo_col_->get_id();

    ofv_data_offset_ = 0;
    ofv_kv_pairs_count_ = 0;
    fvo_data_offset_ = 0;
    fvo_kv_pairs_count_ = 0;
}

GraphTransaction::GraphTransaction(::Database *db, size_t thread_id, TxnID explicit_read_snapshot_id, TxnID explicit_commit_id)
    : db_(db), thread_id_(thread_id),
      ctx_({explicit_commit_id, explicit_read_snapshot_id, thread_id}),
      ofv_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      fvo_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      ofv_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH)),
      fvo_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH))
{
    ofv_col_ = db_->get_ofv_collection();
    fvo_col_ = db_->get_fvo_collection();

    if (ofv_col_)
        ofv_col_idx_ = ofv_col_->get_id();
    if (fvo_col_)
        fvo_col_idx_ = fvo_col_->get_id();

    ofv_data_offset_ = 0;
    ofv_kv_pairs_count_ = 0;
    fvo_data_offset_ = 0;
    fvo_kv_pairs_count_ = 0;
}

GraphTransaction::~GraphTransaction()
{
    if (!is_finished_)
    {
        abort();
    }
}

TxnID GraphTransaction::get_txn_id() const
{
    return ctx_.txn_id;
}

TxnID GraphTransaction::get_read_snapshot_id() const
{
    return ctx_.read_snapshot_id;
}

void GraphTransaction::track_relationship_field(uint32_t field_id) {
    seen_relationship_field_ids_.insert(field_id);
}

void GraphTransaction::insert_fact(uint32_t obj_id, uint32_t field_id, uint32_t val_id)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS)
    {
        flush_pending_writes();
    }

    track_relationship_field(field_id);
    
    size_t ofv_key_len = BINARY_U32_SIZE + 1 + BINARY_U32_SIZE * 2;
    size_t ofv_val_len = 1;
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *ofv_key_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    to_binary_key_buf(obj_id, ofv_key_ptr, BINARY_U32_SIZE);
    ofv_key_ptr[BINARY_U32_SIZE] = OFV_RELATIONSHIP_PREFIX;
    to_binary_key_buf(field_id, ofv_key_ptr + BINARY_U32_SIZE + 1, BINARY_U32_SIZE);
    to_binary_key_buf(val_id, ofv_key_ptr + BINARY_U32_SIZE + 1 + BINARY_U32_SIZE, BINARY_U32_SIZE);

    char *ofv_val_ptr = ofv_key_ptr + ofv_key_len;
    *ofv_val_ptr = StaxValueType::Relationship;

    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {ofv_key_ptr, ofv_key_len};
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {ofv_val_ptr, ofv_val_len};
    ofv_data_offset_ += ofv_key_len + ofv_val_len;

    
    size_t fvo_key_len = BINARY_U32_SIZE * 3;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    to_binary_key_buf(field_id, fvo_key_ptr, BINARY_U32_SIZE);
    to_binary_key_buf(val_id, fvo_key_ptr + BINARY_U32_SIZE, BINARY_U32_SIZE);
    to_binary_key_buf(obj_id, fvo_key_ptr + BINARY_U32_SIZE * 2, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, fvo_key_len};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += fvo_key_len;

    has_writes_ = true;
}

void GraphTransaction::insert_fact_string(uint32_t obj_id, uint32_t field_id, std::string_view field_name, std::string_view value_str)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS)
    {
        flush_pending_writes();
    }
    
    
    size_t ofv_key_len = BINARY_U32_SIZE + 1 + BINARY_U32_SIZE;
    size_t ofv_val_len = 1 + field_name.length() + 1 + value_str.length();
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *ofv_key_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    to_binary_key_buf(obj_id, ofv_key_ptr, BINARY_U32_SIZE);
    ofv_key_ptr[BINARY_U32_SIZE] = OFV_PROPERTY_PREFIX;
    to_binary_key_buf(field_id, ofv_key_ptr + BINARY_U32_SIZE + 1, BINARY_U32_SIZE);

    char *ofv_val_ptr = ofv_key_ptr + ofv_key_len;
    *ofv_val_ptr = StaxValueType::String;
    memcpy(ofv_val_ptr + 1, field_name.data(), field_name.length());
    *(ofv_val_ptr + 1 + field_name.length()) = '\0';
    memcpy(ofv_val_ptr + 1 + field_name.length() + 1, value_str.data(), value_str.length());

    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {ofv_key_ptr, ofv_key_len};
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {ofv_val_ptr, ofv_val_len};
    ofv_data_offset_ += ofv_key_len + ofv_val_len;

    
    uint32_t value_hash_or_id = hash_fnv1a_32(value_str);
    size_t fvo_key_len = BINARY_U32_SIZE * 3;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    to_binary_key_buf(field_id, fvo_key_ptr, BINARY_U32_SIZE);
    to_binary_key_buf(value_hash_or_id, fvo_key_ptr + BINARY_U32_SIZE, BINARY_U32_SIZE);
    to_binary_key_buf(obj_id, fvo_key_ptr + BINARY_U32_SIZE * 2, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, fvo_key_len};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += fvo_key_len;

    has_writes_ = true;
}


void GraphTransaction::insert_fact_numeric(uint32_t obj_id, uint32_t field_id, std::string_view field_name, uint64_t numeric_val)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS)
    {
        flush_pending_writes();
    }

    size_t ofv_key_len = BINARY_U32_SIZE + 1 + BINARY_U32_SIZE;
    size_t ofv_val_len = 1 + field_name.length() + 1 + BINARY_U64_SIZE;
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *ofv_key_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    to_binary_key_buf(obj_id, ofv_key_ptr, BINARY_U32_SIZE);
    ofv_key_ptr[BINARY_U32_SIZE] = OFV_PROPERTY_PREFIX;
    to_binary_key_buf(field_id, ofv_key_ptr + BINARY_U32_SIZE + 1, BINARY_U32_SIZE);

    char *ofv_val_ptr = ofv_key_ptr + ofv_key_len;
    *ofv_val_ptr = StaxValueType::Numeric;
    memcpy(ofv_val_ptr + 1, field_name.data(), field_name.length());
    *(ofv_val_ptr + 1 + field_name.length()) = '\0';
    to_binary_key_buf(numeric_val, ofv_val_ptr + 1 + field_name.length() + 1, BINARY_U64_SIZE);

    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {ofv_key_ptr, ofv_key_len};
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {ofv_val_ptr, ofv_val_len};
    ofv_data_offset_ += ofv_key_len + ofv_val_len;

    size_t fvo_key_len = BINARY_U32_SIZE + BINARY_U64_SIZE + BINARY_U32_SIZE;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char *fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    to_binary_key_buf(field_id, fvo_key_ptr, BINARY_U32_SIZE);
    to_binary_key_buf(numeric_val, fvo_key_ptr + BINARY_U32_SIZE, BINARY_U64_SIZE);
    to_binary_key_buf(obj_id, fvo_key_ptr + BINARY_U32_SIZE + BINARY_U64_SIZE, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, fvo_key_len};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += fvo_key_len;

    has_writes_ = true;
}

void GraphTransaction::insert_fact_geo(uint32_t obj_id, uint32_t field_id, std::string_view field_name, double latitude, double longitude) {
    uint64_t geohash = GeoHash::encode(latitude, longitude);
    
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS)
    {
        flush_pending_writes();
    }

    size_t ofv_key_len = BINARY_U32_SIZE + 1 + BINARY_U32_SIZE;
    size_t ofv_val_len = 1 + field_name.length() + 1 + BINARY_U64_SIZE;
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char* ofv_key_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    to_binary_key_buf(obj_id, ofv_key_ptr, BINARY_U32_SIZE);
    ofv_key_ptr[BINARY_U32_SIZE] = OFV_PROPERTY_PREFIX;
    to_binary_key_buf(field_id, ofv_key_ptr + BINARY_U32_SIZE + 1, BINARY_U32_SIZE);

    char* ofv_val_ptr = ofv_key_ptr + ofv_key_len;
    *ofv_val_ptr = StaxValueType::Geo;
    memcpy(ofv_val_ptr + 1, field_name.data(), field_name.length());
    *(ofv_val_ptr + 1 + field_name.length()) = '\0';
    to_binary_key_buf(geohash, ofv_val_ptr + 1 + field_name.length() + 1, BINARY_U64_SIZE);

    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {ofv_key_ptr, ofv_key_len};
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {ofv_val_ptr, ofv_val_len};
    ofv_data_offset_ += ofv_key_len + ofv_val_len;

    
    size_t fvo_key_len = BINARY_U32_SIZE + BINARY_U64_SIZE + BINARY_U32_SIZE;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE)
    {
        flush_pending_writes();
    }
    char* fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    to_binary_key_buf(field_id, fvo_key_ptr, BINARY_U32_SIZE);
    to_binary_key_buf(geohash, fvo_key_ptr + BINARY_U32_SIZE, BINARY_U64_SIZE);
    to_binary_key_buf(obj_id, fvo_key_ptr + BINARY_U32_SIZE + BINARY_U64_SIZE, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, fvo_key_len};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += fvo_key_len;

    has_writes_ = true;
}


void GraphTransaction::multi_insert_low_level_on_collection(::Collection *target_col, const TxnContext &ctx, TransactionBatch &batch, const CoreKVPair *kv_pairs, size_t num_kvs, uint64_t total_live_bytes_to_add)
{
    if (!target_col || num_kvs == 0)
        return;
    target_col->get_critbit_tree().insert_batch(ctx, kv_pairs, num_kvs, batch);
}

void GraphTransaction::remove_fact(uint32_t obj_id, uint32_t field_id, uint32_t val_id)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    
    char ofv_key_buf[BINARY_U32_SIZE + 1 + BINARY_U32_SIZE * 2];
    size_t ofv_key_len = to_binary_key_buf(obj_id, ofv_key_buf, sizeof(ofv_key_buf));
    ofv_key_buf[ofv_key_len++] = OFV_RELATIONSHIP_PREFIX;
    ofv_key_len += to_binary_key_buf(field_id, ofv_key_buf + ofv_key_len, sizeof(ofv_key_buf) - ofv_key_len);
    ofv_key_len += to_binary_key_buf(val_id, ofv_key_buf + ofv_key_len, sizeof(ofv_key_buf) - ofv_key_len);
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, ofv_key_len));

    
    char fvo_key_buf[BINARY_U32_SIZE * 3];
    size_t fvo_key_len = to_binary_key_buf(field_id, fvo_key_buf, sizeof(fvo_key_buf));
    fvo_key_len += to_binary_key_buf(val_id, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_key_len += to_binary_key_buf(obj_id, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, fvo_key_len));

    has_writes_ = true;
}

void GraphTransaction::remove_fact(uint32_t obj_id, uint32_t field_id, std::string_view value_str)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    
    char ofv_key_buf[BINARY_U32_SIZE + 1 + BINARY_U32_SIZE];
    size_t ofv_key_len = to_binary_key_buf(obj_id, ofv_key_buf, sizeof(ofv_key_buf));
    ofv_key_buf[ofv_key_len++] = OFV_PROPERTY_PREFIX;
    ofv_key_len += to_binary_key_buf(field_id, ofv_key_buf + ofv_key_len, sizeof(ofv_key_buf) - ofv_key_len);
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, ofv_key_len));

    
    uint32_t value_hash = hash_fnv1a_32(value_str);
    char fvo_key_buf[BINARY_U32_SIZE * 3];
    size_t fvo_key_len = to_binary_key_buf(field_id, fvo_key_buf, sizeof(fvo_key_buf));
    fvo_key_len += to_binary_key_buf(value_hash, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_key_len += to_binary_key_buf(obj_id, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, fvo_key_len));

    has_writes_ = true;
}

void GraphTransaction::remove_fact_numeric(uint32_t obj_id, uint32_t field_id, uint64_t numeric_val)
{
    if (is_finished_)
        throw std::runtime_error("GraphTransaction: Transaction already finished.");

    
    char ofv_key_buf[BINARY_U32_SIZE + 1 + BINARY_U32_SIZE];
    size_t ofv_key_len = to_binary_key_buf(obj_id, ofv_key_buf, sizeof(ofv_key_buf));
    ofv_key_buf[ofv_key_len++] = OFV_PROPERTY_PREFIX;
    ofv_key_len += to_binary_key_buf(field_id, ofv_key_buf + ofv_key_len, sizeof(ofv_key_buf) - ofv_key_len);
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, ofv_key_len));

    
    char fvo_key_buf[BINARY_U32_SIZE + BINARY_U64_SIZE + BINARY_U32_SIZE];
    size_t fvo_key_len = to_binary_key_buf(field_id, fvo_key_buf, sizeof(fvo_key_buf));
    fvo_key_len += to_binary_key_buf(numeric_val, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_key_len += to_binary_key_buf(obj_id, fvo_key_buf + fvo_key_len, sizeof(fvo_key_buf) - fvo_key_len);
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, fvo_key_len));

    has_writes_ = true;
}

void GraphTransaction::update_object(uint32_t obj_id, const std::vector<StaxObjectProperty>& properties) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");
    
    // Phase 1: Clear only existing properties for the object, leaving relationships intact.
    clear_object_properties(obj_id);

    // Phase 2: Insert new/updated properties
    for (const auto& prop : properties) {
        std::string_view field_name_sv = to_string_view(prop.field);
        uint32_t field_id = global_id_map.get_or_create_id(field_name_sv);
        switch (prop.type) {
            case STAX_PROP_STRING:
                insert_fact_string(obj_id, field_id, field_name_sv, to_string_view(prop.value.string_val));
                break;
            case STAX_PROP_NUMERIC:
                insert_fact_numeric(obj_id, field_id, field_name_sv, prop.value.numeric_val);
                break;
            case STAX_PROP_GEO:
                insert_fact_geo(obj_id, field_id, field_name_sv, prop.value.geo_val.lat, prop.value.geo_val.lon);
                break;
        }
    }
    has_writes_ = true;
}

void GraphTransaction::clear_object_properties(uint32_t obj_id) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    TxnContext read_ctx = {0, ctx_.read_snapshot_id, thread_id_};
    
    char start_key_buf[BINARY_U32_SIZE];
    size_t start_key_len = to_binary_key_buf(obj_id, start_key_buf, sizeof(start_key_buf));
    std::string_view start_key(start_key_buf, start_key_len);

    char end_key_buf[BINARY_U32_SIZE];
    to_binary_key_buf(obj_id + 1, end_key_buf, sizeof(end_key_buf));
    std::string_view end_key(end_key_buf, sizeof(end_key_buf));
    
    for (auto cursor = ofv_col_->seek(read_ctx, start_key, end_key); cursor->is_valid(); cursor->next())
    {
        std::string_view key_view = cursor->key();
        DataView value_view = cursor->value();
        
        char type_prefix = key_view[BINARY_U32_SIZE];
        
        // Only process properties, skip relationships
        if (type_prefix != OFV_PROPERTY_PREFIX) continue;

        uint32_t field_id = from_binary_key_u32(key_view.substr(BINARY_U32_SIZE + 1, BINARY_U32_SIZE));

        if (value_view.len < 2) continue;
        std::string_view value_payload(value_view.data, value_view.len);
        size_t null_pos = value_payload.find('\0', 1);
        if (null_pos == std::string_view::npos) continue;
        
        char value_type_code = value_payload[0];
        std::string_view value_data = value_payload.substr(null_pos + 1);

        if (value_type_code == StaxValueType::String) {
            remove_fact(obj_id, field_id, value_data);
        } else if (value_type_code == StaxValueType::Numeric || value_type_code == StaxValueType::Geo) {
            remove_fact_numeric(obj_id, field_id, from_binary_key_u64(value_data));
        }
    }
    has_writes_ = true;
}


void GraphTransaction::clear_object_facts(uint32_t obj_id)
{
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    TxnContext read_ctx = {0, ctx_.read_snapshot_id, thread_id_};
    
    // Phase 1: Clear all outgoing properties and relationships (OFV scan)
    clear_object_properties(obj_id); // Reuse the corrected property-only deletion

    char start_key_buf[BINARY_U32_SIZE];
    size_t start_key_len = to_binary_key_buf(obj_id, start_key_buf, sizeof(start_key_buf));
    std::string_view start_key(start_key_buf, start_key_len);

    char end_key_buf[BINARY_U32_SIZE];
    to_binary_key_buf(obj_id + 1, end_key_buf, sizeof(end_key_buf));
    std::string_view end_key(end_key_buf, sizeof(end_key_buf));
    
    for (auto cursor = ofv_col_->seek(read_ctx, start_key, end_key); cursor->is_valid(); cursor->next())
    {
        std::string_view key_view = cursor->key();
        
        char type_prefix = key_view[BINARY_U32_SIZE];
        if (type_prefix != OFV_RELATIONSHIP_PREFIX) continue;

        uint32_t field_id = from_binary_key_u32(key_view.substr(BINARY_U32_SIZE + 1, BINARY_U32_SIZE));
        uint32_t target_id = from_binary_key_u32(key_view.substr(BINARY_U32_SIZE + 1 + BINARY_U32_SIZE));
        remove_fact(obj_id, field_id, target_id);
    }
    
    // Phase 2: Efficiently clear all incoming relationships (FVO prefix scans)
    GraphReader reader(db_, read_ctx);
    auto all_rel_types = reader.get_all_relationship_types(); 
    for (uint32_t field_id : all_rel_types) {
        char prefix_buf[BINARY_U32_SIZE * 2];
        size_t prefix_len = to_binary_key_buf(field_id, prefix_buf, sizeof(prefix_buf));
        prefix_len += to_binary_key_buf(obj_id, prefix_buf + prefix_len, sizeof(prefix_buf) - prefix_len);
        std::string_view prefix(prefix_buf, prefix_len);

        for (auto cursor = fvo_col_->seek(read_ctx, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
             std::string_view key_view = cursor->key();
             if (key_view.length() == BINARY_U32_SIZE * 3) {
                uint32_t source_id = from_binary_key_u32(key_view.substr(BINARY_U32_SIZE * 2));
                remove_fact(source_id, field_id, obj_id);
             }
        }
    }

    has_writes_ = true;
}


void GraphTransaction::flush_pending_writes()
{

    const size_t OFV_RECORD_ALLOC_SIZE = CollectionRecordAllocator::get_allocated_record_size(BINARY_U32_SIZE + 1 + BINARY_U32_SIZE * 2, 1);
    const size_t FVO_RECORD_ALLOC_SIZE = CollectionRecordAllocator::get_allocated_record_size(BINARY_U32_SIZE * 3, 1);

    if (ofv_kv_pairs_count_ > 0)
    {
        size_t count = ofv_kv_pairs_count_;
        TxnContext worker_ctx = {ctx_.txn_id, ctx_.read_snapshot_id, thread_id_};
        multi_insert_low_level_on_collection(ofv_col_, worker_ctx, ofv_batch_deltas_, ofv_kv_pairs_array_.get(), count, count * OFV_RECORD_ALLOC_SIZE);
    }
    if (fvo_kv_pairs_count_ > 0)
    {
        size_t count = fvo_kv_pairs_count_;
        TxnContext worker_ctx = {ctx_.txn_id, ctx_.read_snapshot_id, thread_id_};
        multi_insert_low_level_on_collection(fvo_col_, worker_ctx, fvo_batch_deltas_, fvo_kv_pairs_array_.get(), count, count * FVO_RECORD_ALLOC_SIZE);
    }

    if (ofv_kv_pairs_count_ > 0)
    {
        ofv_kv_pairs_count_ = 0;
        ofv_data_offset_ = 0;
    }
    if (fvo_kv_pairs_count_ > 0)
    {
        fvo_kv_pairs_count_ = 0;
        fvo_data_offset_ = 0;
    }
}

void GraphTransaction::commit()
{
    if (is_finished_)
        return;

    flush_pending_writes();

    ofv_col_->commit(ctx_, ofv_batch_deltas_);
    fvo_col_->commit(ctx_, fvo_batch_deltas_);
    is_finished_ = true;
}

void GraphTransaction::abort()
{
    if (is_finished_)
        return;
    ofv_col_->abort(ctx_);
    fvo_col_->abort(ctx_);
    is_finished_ = true;
}