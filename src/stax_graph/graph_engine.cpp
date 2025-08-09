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
#include <cstdio> // For printf

#if defined(_WIN32)
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#include <stdlib.h>
#else
#include <arpa/inet.h>
#endif

static constexpr char OFV_PROPERTY_PREFIX = 'p';
static constexpr char OFV_RELATIONSHIP_PREFIX = 'r';
static constexpr char KEY_SEPARATOR = '\0';

QueryPipeline::QueryPipeline(std::unique_ptr<QueryOperator> source) : source_(std::move(source)) {}

bool QueryPipeline::next(uint32_t &out_id)
{
    if (!source_)
        return false;
    return source_->next(out_id);
}

IndexScanOperator::IndexScanOperator(Collection *col, const TxnContext &ctx, std::string_view field_name, std::string_view value)
    : col_(col), ctx_(ctx)
{
    key_prefix_ = std::string(field_name) + KEY_SEPARATOR + std::string(value) + KEY_SEPARATOR;
    reset();
}

bool IndexScanOperator::next(uint32_t &out_id)
{
    if (cursor_ && cursor_->is_valid())
    {
        std::string_view key = cursor_->key();
        if (key.starts_with(key_prefix_))
        {
            std::string_view id_part = key.substr(key_prefix_.length());
            if (id_part.length() == GraphTransaction::BINARY_U32_SIZE) {
                out_id = from_binary_key_u32(id_part);
                cursor_->next();
                return true;
            }
        }
    }
    return false;
}

void IndexScanOperator::reset()
{
    cursor_ = col_->seek(ctx_, key_prefix_);
}

ForwardScanOperator::ForwardScanOperator(Collection *col, const TxnContext &ctx, uint32_t source_id, std::string_view field_name)
    : col_(col), ctx_(ctx), source_id_(source_id), field_name_(field_name)
{
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1 + 1 + 1];
    size_t key_len = to_binary_key_buf(source_id_, prefix_buf, sizeof(prefix_buf));
    prefix_buf[key_len++] = KEY_SEPARATOR;
    prefix_buf[key_len++] = OFV_RELATIONSHIP_PREFIX;
    prefix_buf[key_len++] = KEY_SEPARATOR;

    key_prefix_ = std::string(prefix_buf, key_len) + std::string(field_name_) + KEY_SEPARATOR;
    reset();
}

bool ForwardScanOperator::next(uint32_t &out_id)
{
    if (cursor_ && cursor_->is_valid())
    {
        std::string_view key = cursor_->key();

        if (key.starts_with(key_prefix_))
        {
            std::string_view id_part = key.substr(key_prefix_.length());
             if (id_part.length() == GraphTransaction::BINARY_U32_SIZE) {
                out_id = from_binary_key_u32(id_part);
                cursor_->next();
                return true;
            }
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

std::vector<std::tuple<uint32_t, std::string, std::string>> GraphReader::get_properties(uint32_t obj_id)
{
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1];
    size_t prefix_len = to_binary_key_buf(obj_id, prefix_buf, sizeof(prefix_buf));
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    std::string_view prefix(prefix_buf, prefix_len);

    std::vector<std::tuple<uint32_t, std::string, std::string>> results;

    for (auto cursor = ofv_col_->seek(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
    {
        std::string_view key_view = cursor->key();
        std::string_view value_view_sv(cursor->value());

        std::string_view rest_of_key = key_view.substr(prefix.length());
        
        char type_prefix = rest_of_key[0];
        if (type_prefix != OFV_PROPERTY_PREFIX) {
            continue; 
        }
        rest_of_key.remove_prefix(2); 
        
        std::string field_name = std::string(rest_of_key);
        char value_type = value_view_sv[0];
        std::string_view value_data = value_view_sv.substr(1);
        if (value_type == StaxValueType::String) {
            results.emplace_back(obj_id, field_name, std::string(value_data));
        } else if (value_type == StaxValueType::Numeric || value_type == StaxValueType::Geo) {
            results.emplace_back(obj_id, field_name, std::to_string(from_binary_key_u64(value_data)));
        }
    }
    return results;
}

std::vector<std::tuple<uint32_t, std::string, std::string>> GraphReader::get_properties_and_relationships(uint32_t obj_id)
{
    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1];
    size_t prefix_len = to_binary_key_buf(obj_id, prefix_buf, sizeof(prefix_buf));
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    std::string_view prefix(prefix_buf, prefix_len);

    std::vector<std::tuple<uint32_t, std::string, std::string>> results;

    for (auto cursor = ofv_col_->seek(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next())
    {
        std::string_view key_view = cursor->key();
        std::string_view value_view_sv(cursor->value());

        std::string_view rest_of_key = key_view.substr(prefix.length());
        
        char type_prefix = rest_of_key[0];
        rest_of_key.remove_prefix(2); 
        
        if (type_prefix == OFV_PROPERTY_PREFIX)
        {
            std::string field_name = std::string(rest_of_key);
            char value_type = value_view_sv[0];
            std::string_view value_data = value_view_sv.substr(1);
            if (value_type == StaxValueType::String) {
                results.emplace_back(obj_id, field_name, std::string(value_data));
            } else if (value_type == StaxValueType::Numeric || value_type == StaxValueType::Geo) {
                results.emplace_back(obj_id, field_name, std::to_string(from_binary_key_u64(value_data)));
            }
        }
        else if (type_prefix == OFV_RELATIONSHIP_PREFIX)
        {
            size_t separator_pos = rest_of_key.find(KEY_SEPARATOR);
            if (separator_pos != std::string_view::npos) {
                std::string rel_name = std::string(rest_of_key.substr(0, separator_pos));
                uint32_t target_id = from_binary_key_u32(rest_of_key.substr(separator_pos + 1));
                results.emplace_back(obj_id, rel_name, std::to_string(target_id));
            }
        }
    }
    return results;
}

std::optional<DataView> GraphReader::get_property_for_object_direct(uint32_t obj_id, std::string_view field_name)
{
    char key_buf[GraphTransaction::BINARY_U32_SIZE + 1 + 1 + 1 + 256];
    size_t key_len = to_binary_key_buf(obj_id, key_buf, sizeof(key_buf));
    key_buf[key_len++] = KEY_SEPARATOR;
    key_buf[key_len++] = OFV_PROPERTY_PREFIX;
    key_buf[key_len++] = KEY_SEPARATOR;
    memcpy(key_buf + key_len, field_name.data(), field_name.length());
    key_len += field_name.length();
    std::string_view key(key_buf, key_len);

    auto result = ofv_col_->get(ctx_, key);
    if (result && result->value_len > 0)
    {
        return DataView(result->value_ptr, result->value_len);
    }
    return std::nullopt;
}

std::optional<std::string_view> GraphReader::get_property_for_object_string(uint32_t obj_id, std::string_view field_name)
{
    auto result = get_property_for_object_direct(obj_id, field_name);
    if (result && result->len > 1 && result->data[0] == StaxValueType::String)
    {
        return std::string_view(result->data + 1, result->len - 1);
    }
    return std::nullopt;
}

std::optional<uint64_t> GraphReader::get_property_for_object_numeric(uint32_t obj_id, std::string_view field_name)
{
    auto result = get_property_for_object_direct(obj_id, field_name);
    if (result && result->len == 1 + GraphTransaction::BINARY_U64_SIZE && (result->data[0] == StaxValueType::Numeric || result->data[0] == StaxValueType::Geo))
    {
        return from_binary_key_u64(std::string_view(result->data + 1, GraphTransaction::BINARY_U64_SIZE));
    }
    return std::nullopt;
}

std::set<std::string> GraphReader::get_all_relationship_types() {
    std::set<std::string> rel_types;
    for (auto cursor = fvo_col_->seek_first(ctx_); cursor->is_valid(); cursor->next()) {
        std::string_view key = cursor->key();
        size_t first_sep = key.find(KEY_SEPARATOR);
        if (first_sep != std::string_view::npos) {
            rel_types.insert(std::string(key.substr(0, first_sep)));
        }
    }
    return rel_types;
}

std::vector<uint32_t> GraphReader::get_objects_by_property(std::string_view field_name, std::string_view value_str) {
    roaring_bitmap_t* temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap) return {};

    get_objects_by_property_into_roaring(field_name, value_str, temp_bitmap);

    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    std::vector<uint32_t> results_vec;
    if (cardinality > 0) {
        results_vec.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results_vec.data());
    }

    roaring_bitmap_free(temp_bitmap);
    return results_vec;
}

void GraphReader::get_objects_by_property_into_roaring(std::string_view field_name, std::string_view value_str, roaring_bitmap_t* target_bitmap) {
    if (!target_bitmap) return;

    std::string fvo_prefix = std::string(field_name) + KEY_SEPARATOR + std::string(value_str) + KEY_SEPARATOR;

    for (auto cursor = fvo_col_->seek_raw(ctx_, fvo_prefix); cursor->is_valid() && cursor->key().starts_with(fvo_prefix); cursor->next()) {
        std::string_view key_view = cursor->key();
        std::string_view id_part = key_view.substr(fvo_prefix.length());
        if (id_part.length() == GraphTransaction::BINARY_U32_SIZE) {
            roaring_bitmap_add(target_bitmap, from_binary_key_u32(id_part));
        }
    }
}

void GraphReader::get_objects_by_property_range_into_roaring(std::string_view field_name, uint64_t start_numeric_val, uint64_t end_numeric_val, roaring_bitmap_t* target_bitmap) {
    if (!target_bitmap) return;

    char start_val_buf[GraphTransaction::BINARY_U64_SIZE];
    to_binary_key_buf(start_numeric_val, start_val_buf, sizeof(start_val_buf));
    std::string start_key = std::string(field_name) + KEY_SEPARATOR + std::string(start_val_buf, sizeof(start_val_buf)) + KEY_SEPARATOR;

    char end_val_buf[GraphTransaction::BINARY_U64_SIZE];
    to_binary_key_buf(end_numeric_val, end_val_buf, sizeof(end_val_buf));
    std::string end_key_exclusive = std::string(field_name) + KEY_SEPARATOR + std::string(end_val_buf, sizeof(end_val_buf)) + KEY_SEPARATOR + '\xff';

    for (auto cursor = fvo_col_->seek_raw(ctx_, start_key, end_key_exclusive); cursor->is_valid(); cursor->next()) {
        std::string_view key_view = cursor->key();
        
        size_t expected_id_offset = field_name.length() + 1 + GraphTransaction::BINARY_U64_SIZE + 1;
        if (key_view.length() == expected_id_offset + GraphTransaction::BINARY_U32_SIZE) {
             std::string_view id_part = key_view.substr(expected_id_offset);
             roaring_bitmap_add(target_bitmap, from_binary_key_u32(id_part));
        }
    }
}

size_t GraphReader::count_objects_by_property(std::string_view field_name, std::string_view value_str) {
    roaring_bitmap_t* temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap) throw std::bad_alloc();
    get_objects_by_property_into_roaring(field_name, value_str, temp_bitmap);
    size_t count = roaring_bitmap_get_cardinality(temp_bitmap);
    roaring_bitmap_free(temp_bitmap);
    return count;
}

size_t GraphReader::count_relationships_by_type(std::string_view relationship_field_name) {
    std::string fvo_rel_prefix = std::string(relationship_field_name) + KEY_SEPARATOR;
    size_t count = 0;
    for (auto cursor = fvo_col_->seek_raw(ctx_, fvo_rel_prefix); cursor->is_valid() && cursor->key().starts_with(fvo_rel_prefix); cursor->next()) {
        count++;
    }
    return count;
}

std::vector<uint32_t> GraphReader::get_outgoing_relationships(uint32_t source_obj_id, std::string_view relationship_field_name) {
    std::vector<uint32_t> results_vec;
    roaring_bitmap_t* temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap) return results_vec;
    get_outgoing_relationships_into_roaring(source_obj_id, relationship_field_name, temp_bitmap);
    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    if (cardinality > 0) {
        results_vec.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results_vec.data());
    }
    roaring_bitmap_free(temp_bitmap);
    return results_vec;
}

void GraphReader::get_outgoing_relationships_into_roaring(uint32_t source_obj_id, std::string_view relationship_field_name, roaring_bitmap_t* target_bitmap) {
    if (!target_bitmap) return;

    char prefix_buf[GraphTransaction::BINARY_U32_SIZE + 1 + 1 + 1 + 256];
    size_t prefix_len = to_binary_key_buf(source_obj_id, prefix_buf, sizeof(prefix_buf));
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    prefix_buf[prefix_len++] = OFV_RELATIONSHIP_PREFIX;
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    memcpy(prefix_buf + prefix_len, relationship_field_name.data(), relationship_field_name.length());
    prefix_len += relationship_field_name.length();
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    std::string_view prefix(prefix_buf, prefix_len);

    for (auto cursor = ofv_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
        std::string_view key_view = cursor->key();
        std::string_view id_part = key_view.substr(prefix.length());
        if (id_part.length() == GraphTransaction::BINARY_U32_SIZE) {
            roaring_bitmap_add(target_bitmap, from_binary_key_u32(id_part));
        }
    }
}

void GraphReader::get_outgoing_relationships_for_many_into_roaring(roaring_bitmap_t* source_nodes, std::string_view relationship_field_name, roaring_bitmap_t* target_bitmap) {
    if (!target_bitmap || !source_nodes) return;
    roaring_uint32_iterator_t* it = roaring_create_iterator(source_nodes);
    while (it->has_next) {
        uint32_t source_id;
        roaring_read_uint32(it, &source_id);
        get_outgoing_relationships_into_roaring(source_id, relationship_field_name, target_bitmap);
        roaring_advance_uint32_iterator(it);
    }
    roaring_free_iterator(it);
}

std::vector<uint32_t> GraphReader::get_incoming_relationships(uint32_t target_obj_id, std::string_view relationship_field_name) {
    std::vector<uint32_t> results_vec;
    roaring_bitmap_t* temp_bitmap = roaring_bitmap_create();
    if (!temp_bitmap) return results_vec;
    roaring_bitmap_t* target_nodes_bitmap = roaring_bitmap_create();
    if (!target_nodes_bitmap) {
        roaring_bitmap_free(temp_bitmap);
        return results_vec;
    }
    roaring_bitmap_add(target_nodes_bitmap, target_obj_id);
    get_incoming_relationships_for_many_into_roaring(target_nodes_bitmap, relationship_field_name, temp_bitmap);
    roaring_bitmap_free(target_nodes_bitmap);
    uint64_t cardinality = roaring_bitmap_get_cardinality(temp_bitmap);
    if (cardinality > 0) {
        results_vec.resize(cardinality);
        roaring_bitmap_to_uint32_array(temp_bitmap, results_vec.data());
    }
    roaring_bitmap_free(temp_bitmap);
    return results_vec;
}

void GraphReader::get_incoming_relationships_for_many_into_roaring(roaring_bitmap_t* target_nodes, std::string_view relationship_field_name, roaring_bitmap_t* source_bitmap) {
    if (!target_nodes || !source_bitmap) return;
    roaring_uint32_iterator_t* it = roaring_create_iterator(target_nodes);
    while (it->has_next) {
        uint32_t target_id;
        roaring_read_uint32(it, &target_id);
        char target_id_buf[GraphTransaction::BINARY_U32_SIZE];
        to_binary_key_buf(target_id, target_id_buf, sizeof(target_id_buf));
        std::string prefix = std::string(relationship_field_name) + KEY_SEPARATOR + std::string(target_id_buf, sizeof(target_id_buf)) + KEY_SEPARATOR;
        for (auto cursor = fvo_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
            std::string_view key_view = cursor->key();
            std::string_view id_part = key_view.substr(prefix.length());
            if (id_part.length() == GraphTransaction::BINARY_U32_SIZE) {
                roaring_bitmap_add(source_bitmap, from_binary_key_u32(id_part));
            }
        }
        roaring_advance_uint32_iterator(it);
    }
    roaring_free_iterator(it);
}

std::vector<uint32_t> GraphReader::find_shortest_path(uint32_t start_node, uint32_t end_node, std::string_view relationship_field_name) {
    if (start_node == end_node) return {start_node};
    std::queue<uint32_t> q;
    std::map<uint32_t, uint32_t> predecessors;
    std::set<uint32_t> visited;
    q.push(start_node);
    visited.insert(start_node);
    predecessors[start_node] = 0;
    while (!q.empty()) {
        uint32_t current_node = q.front();
        q.pop();
        if (current_node == end_node) {
            std::vector<uint32_t> path;
            uint32_t at = end_node;
            while (at != 0) {
                path.push_back(at);
                at = predecessors[at];
            }
            std::reverse(path.begin(), path.end());
            return path;
        }
        auto neighbors_vec = get_outgoing_relationships(current_node, relationship_field_name);
        for (uint32_t neighbor : neighbors_vec) {
            if (visited.find(neighbor) == visited.end()) {
                visited.insert(neighbor);
                predecessors[neighbor] = current_node;
                q.push(neighbor);
            }
        }
    }
    return {};
}

uint64_t GraphReader::count_triangles(std::string_view relationship_field_name) {
    uint64_t triangle_count = 0;
    
    roaring_bitmap_t* all_nodes = roaring_bitmap_create();
    if (!all_nodes) return 0;

    std::string prefix = std::string(relationship_field_name) + KEY_SEPARATOR;
    for (auto cursor = fvo_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
        std::string_view key = cursor->key();
        std::string_view remainder = key.substr(prefix.length());
        
        if (remainder.length() == GraphTransaction::BINARY_U32_SIZE + 1 + GraphTransaction::BINARY_U32_SIZE) {
            uint32_t target_id = from_binary_key_u32(remainder.substr(0, GraphTransaction::BINARY_U32_SIZE));
            uint32_t source_id = from_binary_key_u32(remainder.substr(GraphTransaction::BINARY_U32_SIZE + 1));
            roaring_bitmap_add(all_nodes, source_id);
            roaring_bitmap_add(all_nodes, target_id);
        }
    }

    roaring_uint32_iterator_t* it = roaring_create_iterator(all_nodes);
    while(it->has_next) {
        uint32_t u;
        roaring_read_uint32(it, &u);

        roaring_bitmap_t* out_neighbors_u = roaring_bitmap_create();
        get_outgoing_relationships_into_roaring(u, relationship_field_name, out_neighbors_u);
        
        roaring_uint32_iterator_t* it_v = roaring_create_iterator(out_neighbors_u);
        while(it_v->has_next) {
            uint32_t v;
            roaring_read_uint32(it_v, &v);
            
            roaring_bitmap_t* out_neighbors_v = roaring_bitmap_create();
            get_outgoing_relationships_into_roaring(v, relationship_field_name, out_neighbors_v);
            
            roaring_uint32_iterator_t* it_w = roaring_create_iterator(out_neighbors_v);
            while(it_w->has_next) {
                uint32_t w;
                roaring_read_uint32(it_w, &w);
                if (has_relationship(w, relationship_field_name, u)) {
                    triangle_count++;
                }
                roaring_advance_uint32_iterator(it_w);
            }
            roaring_free_iterator(it_w);
            roaring_bitmap_free(out_neighbors_v);
            roaring_advance_uint32_iterator(it_v);
        }
        roaring_free_iterator(it_v);
        roaring_bitmap_free(out_neighbors_u);
        roaring_advance_uint32_iterator(it);
    }
    roaring_free_iterator(it);
    roaring_bitmap_free(all_nodes);

    return triangle_count;
}

std::unique_ptr<QueryPipeline> GraphReader::get_common_neighbors(uint32_t node1, uint32_t node2, std::string_view relationship_field_name) {
    auto scan1 = std::make_unique<ForwardScanOperator>(ofv_col_, ctx_, node1, relationship_field_name);
    auto scan2 = std::make_unique<ForwardScanOperator>(ofv_col_, ctx_, node2, relationship_field_name);
    auto intersect = std::make_unique<IntersectOperator>(std::move(scan1), std::move(scan2));
    return std::make_unique<QueryPipeline>(std::move(intersect));
}

bool GraphReader::has_relationship(uint32_t source_obj_id, std::string_view relationship_field_name, uint32_t target_obj_id) {
    char key_buf[GraphTransaction::BINARY_U32_SIZE + 1 + 1 + 256 + 1 + GraphTransaction::BINARY_U32_SIZE];
    size_t key_len = to_binary_key_buf(source_obj_id, key_buf, sizeof(key_buf));
    key_buf[key_len++] = KEY_SEPARATOR;
    key_buf[key_len++] = OFV_RELATIONSHIP_PREFIX;
    key_buf[key_len++] = KEY_SEPARATOR;
    memcpy(key_buf + key_len, relationship_field_name.data(), relationship_field_name.length());
    key_len += relationship_field_name.length();
    key_buf[key_len++] = KEY_SEPARATOR;
    key_len += to_binary_key_buf(target_obj_id, key_buf + key_len, sizeof(key_buf) - key_len);
    std::string_view key(key_buf, key_len);
    return ofv_col_->get(ctx_, key).has_value();
}

static const char FVO_PLACEHOLDER_VALUE = '1';

GraphTransaction::GraphTransaction(::Database* db, size_t thread_id)
    : db_(db), thread_id_(thread_id),
      ctx_(db->begin_transaction_context(thread_id, false)),
      ofv_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      fvo_kv_data_buffer_(std::make_unique<char[]>(MAX_BATCH_KEY_DATA_SIZE)),
      ofv_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH)),
      fvo_kv_pairs_array_(std::make_unique<CoreKVPair[]>(MAX_KV_PAIRS_PER_BATCH)) {
    ofv_col_ = db_->get_ofv_collection();
    fvo_col_ = db_->get_fvo_collection();
    if (ofv_col_) ofv_col_idx_ = ofv_col_->get_id();
    if (fvo_col_) fvo_col_idx_ = fvo_col_->get_id();
    ofv_data_offset_ = 0;
    ofv_kv_pairs_count_ = 0;
    fvo_data_offset_ = 0;
    fvo_kv_pairs_count_ = 0;
}

GraphTransaction::~GraphTransaction() { if (!is_finished_) abort(); }
TxnID GraphTransaction::get_txn_id() const { return ctx_.txn_id; }
TxnID GraphTransaction::get_read_snapshot_id() const { return ctx_.read_snapshot_id; }
void GraphTransaction::track_relationship_field(std::string_view field_name) { seen_relationship_field_ids_.insert(std::string(field_name)); }

void GraphTransaction::insert_fact(uint32_t obj_id, std::string_view field_name, uint32_t val_id) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");
    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS) flush_pending_writes();
    track_relationship_field(field_name);
    
    // OFV
    size_t ofv_key_len = BINARY_U32_SIZE + 1 + 1 + 1 + field_name.length() + 1 + BINARY_U32_SIZE;
    if (ofv_data_offset_ + ofv_key_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* ofv_key_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    char* p = ofv_key_ptr;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE); *p++ = KEY_SEPARATOR; *p++ = OFV_RELATIONSHIP_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(val_id, p, BINARY_U32_SIZE);
    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {ofv_key_ptr, (size_t)(p - ofv_key_ptr)};
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    ofv_data_offset_ += (p - ofv_key_ptr);
    
    // FVO
    size_t fvo_key_len = field_name.length() + 1 + BINARY_U32_SIZE + 1 + BINARY_U32_SIZE;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    p = fvo_key_ptr;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(val_id, p, BINARY_U32_SIZE); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, (size_t)(p - fvo_key_ptr)};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += (p - fvo_key_ptr);

    has_writes_ = true;
}

void GraphTransaction::insert_fact_string(uint32_t obj_id, std::string_view field_name, std::string_view value_str) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");
    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS) flush_pending_writes();

    // OFV
    size_t ofv_key_len = BINARY_U32_SIZE + 1 + 1 + 1 + field_name.length();
    size_t ofv_val_len = 1 + value_str.length();
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* buf_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    char* p = buf_ptr;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE); *p++ = KEY_SEPARATOR; *p++ = OFV_PROPERTY_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {buf_ptr, (size_t)(p - buf_ptr)};
    char* val_ptr = p;
    *p++ = StaxValueType::String;
    memcpy(p, value_str.data(), value_str.length()); p += value_str.length();
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {val_ptr, (size_t)(p - val_ptr)};
    ofv_data_offset_ += (p - buf_ptr);

    // FVO
    size_t fvo_key_len = field_name.length() + 1 + value_str.length() + 1 + BINARY_U32_SIZE;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    p = fvo_key_ptr;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    *p++ = KEY_SEPARATOR;
    memcpy(p, value_str.data(), value_str.length()); p += value_str.length();
    *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, (size_t)(p - fvo_key_ptr)};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += (p - fvo_key_ptr);

    has_writes_ = true;
}

void GraphTransaction::insert_fact_numeric(uint32_t obj_id, std::string_view field_name, uint64_t numeric_val) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");
    if (ofv_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS || fvo_kv_pairs_count_ >= GRAPH_BATCH_FLUSH_THRESHOLD_KVS) flush_pending_writes();

    // OFV
    size_t ofv_key_len = BINARY_U32_SIZE + 1 + 1 + 1 + field_name.length();
    size_t ofv_val_len = 1 + BINARY_U64_SIZE;
    if (ofv_data_offset_ + ofv_key_len + ofv_val_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* buf_ptr = ofv_kv_data_buffer_.get() + ofv_data_offset_;
    char* p = buf_ptr;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE); *p++ = KEY_SEPARATOR; *p++ = OFV_PROPERTY_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    ofv_kv_pairs_array_[ofv_kv_pairs_count_].key = {buf_ptr, (size_t)(p - buf_ptr)};
    char* val_ptr = p;
    *p++ = StaxValueType::Numeric;
    p += to_binary_key_buf(numeric_val, p, BINARY_U64_SIZE);
    ofv_kv_pairs_array_[ofv_kv_pairs_count_++].value = {val_ptr, (size_t)(p - val_ptr)};
    ofv_data_offset_ += (p - buf_ptr);
    
    // FVO
    size_t fvo_key_len = field_name.length() + 1 + BINARY_U64_SIZE + 1 + BINARY_U32_SIZE;
    if (fvo_data_offset_ + fvo_key_len > MAX_BATCH_KEY_DATA_SIZE) flush_pending_writes();
    char* fvo_key_ptr = fvo_kv_data_buffer_.get() + fvo_data_offset_;
    p = fvo_key_ptr;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(numeric_val, p, BINARY_U64_SIZE); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, BINARY_U32_SIZE);
    fvo_kv_pairs_array_[fvo_kv_pairs_count_].key = {fvo_key_ptr, (size_t)(p - fvo_key_ptr)};
    fvo_kv_pairs_array_[fvo_kv_pairs_count_++].value = {&FVO_PLACEHOLDER_VALUE, 1};
    fvo_data_offset_ += (p - fvo_key_ptr);

    has_writes_ = true;
}

void GraphTransaction::insert_fact_geo(uint32_t obj_id, std::string_view field_name, double latitude, double longitude) {
    insert_fact_numeric(obj_id, field_name, GeoHash::encode(latitude, longitude));
}
void GraphTransaction::remove_fact(uint32_t obj_id, std::string_view field_name, uint32_t val_id) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");
    
    // OFV
    char ofv_key_buf[BINARY_U32_SIZE + 1 + 1 + 1 + 256 + 1 + BINARY_U32_SIZE];
    char* p = ofv_key_buf;
    p += to_binary_key_buf(obj_id, p, sizeof(ofv_key_buf)); *p++ = KEY_SEPARATOR; *p++ = OFV_RELATIONSHIP_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length(); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(val_id, p, sizeof(ofv_key_buf) - (p - ofv_key_buf));
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, p - ofv_key_buf));

    // FVO
    char fvo_key_buf[256 + 1 + BINARY_U32_SIZE + 1 + BINARY_U32_SIZE];
    p = fvo_key_buf;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length(); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(val_id, p, sizeof(fvo_key_buf) - (p - fvo_key_buf)); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, sizeof(fvo_key_buf) - (p - fvo_key_buf));
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, p - fvo_key_buf));
    
    has_writes_ = true;
}
void GraphTransaction::remove_fact(uint32_t obj_id, std::string_view field_name, std::string_view value_str) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    // OFV
    char ofv_key_buf[BINARY_U32_SIZE + 1 + 1 + 1 + 256];
    char* p = ofv_key_buf;
    p += to_binary_key_buf(obj_id, p, sizeof(ofv_key_buf)); *p++ = KEY_SEPARATOR; *p++ = OFV_PROPERTY_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, p - ofv_key_buf));

    // FVO
    char fvo_key_buf[256 + 1 + 4096 + 1 + BINARY_U32_SIZE];
    p = fvo_key_buf;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length(); *p++ = KEY_SEPARATOR;
    memcpy(p, value_str.data(), value_str.length()); p += value_str.length(); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, sizeof(fvo_key_buf) - (p - fvo_key_buf));
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, p - fvo_key_buf));

    has_writes_ = true;
}
void GraphTransaction::remove_fact_numeric(uint32_t obj_id, std::string_view field_name, uint64_t numeric_val) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    // OFV
    char ofv_key_buf[BINARY_U32_SIZE + 1 + 1 + 1 + 256];
    char* p = ofv_key_buf;
    p += to_binary_key_buf(obj_id, p, sizeof(ofv_key_buf)); *p++ = KEY_SEPARATOR; *p++ = OFV_PROPERTY_PREFIX; *p++ = KEY_SEPARATOR;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length();
    ofv_col_->remove(ctx_, ofv_batch_deltas_, std::string_view(ofv_key_buf, p - ofv_key_buf));

    // FVO
    char fvo_key_buf[256 + 1 + BINARY_U64_SIZE + 1 + BINARY_U32_SIZE];
    p = fvo_key_buf;
    memcpy(p, field_name.data(), field_name.length()); p += field_name.length(); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(numeric_val, p, sizeof(fvo_key_buf) - (p-fvo_key_buf)); *p++ = KEY_SEPARATOR;
    p += to_binary_key_buf(obj_id, p, sizeof(fvo_key_buf) - (p - fvo_key_buf));
    fvo_col_->remove(ctx_, fvo_batch_deltas_, std::string_view(fvo_key_buf, p - fvo_key_buf));
    
    has_writes_ = true;
}
void GraphTransaction::update_object(uint32_t obj_id, const std::vector<StaxObjectProperty>& properties) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    clear_object_properties(obj_id);

    for (const auto& prop : properties) {
        std::string_view field_name_sv = to_string_view(prop.field);
        switch (prop.type) {
            case STAX_PROP_STRING:
                insert_fact_string(obj_id, field_name_sv, to_string_view(prop.value.string_val));
                break;
            case STAX_PROP_NUMERIC:
                insert_fact_numeric(obj_id, field_name_sv, prop.value.numeric_val);
                break;
            case STAX_PROP_GEO:
                insert_fact_geo(obj_id, field_name_sv, prop.value.geo_val.lat, prop.value.geo_val.lon);
                break;
        }
    }
}
void GraphTransaction::clear_object_facts(uint32_t obj_id) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    GraphReader reader(db_, ctx_);
    auto facts = reader.get_properties_and_relationships(obj_id);
    for (const auto& fact : facts) {
        const auto& [subj, pred, obj_str] = fact;
        
        bool is_numeric = true;
        for(char c : obj_str) {
            if (!isdigit(c)) {
                is_numeric = false;
                break;
            }
        }
        
        if (is_numeric) {
            uint64_t val = std::stoull(obj_str);
            auto prop_val = reader.get_property_for_object_numeric(subj, pred);
            if (prop_val && *prop_val == val) {
                remove_fact_numeric(subj, pred, val);
            } else {
                remove_fact(subj, pred, (uint32_t)val);
            }
        } else {
            remove_fact(subj, pred, obj_str);
        }
    }
    has_writes_ = true;
}

void GraphTransaction::clear_object_properties(uint32_t obj_id) {
    if (is_finished_) throw std::runtime_error("GraphTransaction: Transaction already finished.");

    char prefix_buf[BINARY_U32_SIZE + 1 + 1 + 1];
    size_t prefix_len = to_binary_key_buf(obj_id, prefix_buf, sizeof(prefix_buf));
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    prefix_buf[prefix_len++] = OFV_PROPERTY_PREFIX;
    prefix_buf[prefix_len++] = KEY_SEPARATOR;
    std::string_view prefix(prefix_buf, prefix_len);

    for (auto cursor = ofv_col_->seek_raw(ctx_, prefix); cursor->is_valid() && cursor->key().starts_with(prefix); cursor->next()) {
        std::string_view full_key = cursor->key();
        std::string_view value_with_type = cursor->value();
        std::string_view field_name = full_key.substr(prefix.length());
        
        ofv_col_->remove(ctx_, ofv_batch_deltas_, full_key);

        char value_type = value_with_type[0];
        std::string_view value_data = value_with_type.substr(1);

        if (value_type == StaxValueType::String) {
            std::string fvo_key = std::string(field_name) + KEY_SEPARATOR + std::string(value_data) + KEY_SEPARATOR + to_binary_key(obj_id);
            fvo_col_->remove(ctx_, fvo_batch_deltas_, fvo_key);
        } else if (value_type == StaxValueType::Numeric || value_type == StaxValueType::Geo) {
            std::string fvo_key = std::string(field_name) + KEY_SEPARATOR + std::string(value_data) + KEY_SEPARATOR + to_binary_key(obj_id);
            fvo_col_->remove(ctx_, fvo_batch_deltas_, fvo_key);
        }
    }
    has_writes_ = true;
}

void GraphTransaction::multi_insert_low_level_on_collection(::Collection* target_col, const TxnContext& ctx, TransactionBatch& batch, const CoreKVPair* kv_pairs, size_t num_kvs, uint64_t total_live_bytes_to_add) {
    if (!target_col || num_kvs == 0) return;
    target_col->get_critbit_tree().insert_batch(ctx, kv_pairs, num_kvs, batch);
}

void GraphTransaction::flush_pending_writes() {
    if (ofv_kv_pairs_count_ > 0) {
        multi_insert_low_level_on_collection(ofv_col_, ctx_, ofv_batch_deltas_, ofv_kv_pairs_array_.get(), ofv_kv_pairs_count_, 0);
        ofv_kv_pairs_count_ = 0;
        ofv_data_offset_ = 0;
    }
    if (fvo_kv_pairs_count_ > 0) {
        multi_insert_low_level_on_collection(fvo_col_, ctx_, fvo_batch_deltas_, fvo_kv_pairs_array_.get(), fvo_kv_pairs_count_, 0);
        fvo_kv_pairs_count_ = 0;
        fvo_data_offset_ = 0;
    }
}

void GraphTransaction::commit() {
    if (is_finished_) return;
    flush_pending_writes();
    ofv_col_->commit(ctx_, ofv_batch_deltas_);
    fvo_col_->commit(ctx_, fvo_batch_deltas_);
    is_finished_ = true;
}

void GraphTransaction::abort() {
    if (is_finished_) return;
    ofv_col_->abort(ctx_);
    fvo_col_->abort(ctx_);
    is_finished_ = true;
}