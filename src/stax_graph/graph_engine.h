#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <iostream>
#include <atomic>
#include <thread>
#include <random>
#include <iomanip>
#include <set>
#include <algorithm>
#include "stax_common/roaring.h"

#include "stax_db/db.h"
#include "stax_common/constants.h"
#include "stax_tx/transaction.h"
#include "stax_common/common_types.hpp"
#include "stax_common/geohash.hpp"
#include "stax_common/binary_utils.h" 

class GraphTransaction;
class GraphReader;

class QueryOperator
{
public:
    virtual ~QueryOperator() = default;
    virtual bool next(uint32_t &out_id) = 0;
    virtual void reset() = 0;
};

class QueryPipeline
{
private:
    std::unique_ptr<QueryOperator> source_;

public:
    QueryPipeline(std::unique_ptr<QueryOperator> source);
    bool next(uint32_t &out_id);
};

class IndexScanOperator : public QueryOperator
{
private:
    Collection *col_;
    const TxnContext &ctx_;
    uint32_t field_id_;
    uint32_t value_id_;
    std::unique_ptr<DBCursor> cursor_;
    std::string key_prefix_;

public:
    IndexScanOperator(Collection *col, const TxnContext &ctx, uint32_t field_id, uint32_t value_id);
    bool next(uint32_t &out_id) override;
    void reset() override;
};

class ForwardScanOperator : public QueryOperator
{
private:
    Collection *col_;
    const TxnContext &ctx_;
    uint32_t source_id_;
    uint32_t field_id_;
    std::unique_ptr<DBCursor> cursor_;
    std::string key_prefix_;

public:
    ForwardScanOperator(Collection *col, const TxnContext &ctx, uint32_t source_id, uint32_t field_id);
    bool next(uint32_t &out_id) override;
    void reset() override;
};

class IntersectOperator : public QueryOperator
{
private:
    std::unique_ptr<QueryOperator> left_;
    std::unique_ptr<QueryOperator> right_;
    uint32_t left_val_;
    uint32_t right_val_;
    bool left_valid_;
    bool right_valid_;

public:
    IntersectOperator(std::unique_ptr<QueryOperator> left, std::unique_ptr<QueryOperator> right);
    void reset() override;
    bool next(uint32_t &out_id) override;
};

uint32_t hash_fnv1a_32(std::string_view s);

struct GlobalIDMapShim
{

    uint32_t get_or_create_id(std::string_view s) const
    {
        return hash_fnv1a_32(s);
    }

    uint32_t get_id(std::string_view s) const
    {
        return hash_fnv1a_32(s);
    }

    uint32_t get_total_ids_generated() const
    {
        return 0;
    }
};

constexpr GlobalIDMapShim global_id_map;

namespace Tests
{
    void run_graph_correctness_test();
}

class GraphReader
{
public:
    GraphReader(::Database *db, const TxnContext &ctx);

    std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> get_properties_and_relationships(uint32_t obj_id);
    std::optional<std::string_view> get_property_for_object_string(uint32_t obj_id, uint32_t field_id);
    std::optional<uint64_t> get_property_for_object_numeric(uint32_t obj_id, uint32_t field_id);
    std::set<uint32_t> get_all_relationship_types();

    std::vector<uint32_t> get_objects_by_property(uint32_t field_id, uint32_t value_id);
    std::vector<uint32_t> get_objects_by_property(uint32_t field_id, std::string_view value_str);

    void get_objects_by_property_into_roaring(uint32_t field_id, uint32_t value_id, roaring_bitmap_t *target_bitmap);
    void get_objects_by_property_range_into_roaring(uint32_t field_id, uint64_t start_numeric_val, uint64_t end_numeric_val, roaring_bitmap_t *target_bitmap);
    size_t count_objects_by_property(uint32_t field_id, uint32_t value_id);
    size_t count_relationships_by_type(uint32_t relationship_field_id);
    std::vector<uint32_t> get_outgoing_relationships(uint32_t source_obj_id, uint32_t relationship_field_id);
    void get_outgoing_relationships_into_roaring(uint32_t source_obj_id, uint32_t relationship_field_id, roaring_bitmap_t *target_bitmap);
    void get_outgoing_relationships_for_many_into_roaring(roaring_bitmap_t *source_nodes, uint32_t relationship_field_id, roaring_bitmap_t *target_bitmap);
    std::vector<uint32_t> get_incoming_relationships(uint32_t target_obj_id, uint32_t relationship_field_id);
    void get_incoming_relationships_for_many_into_roaring(roaring_bitmap_t *target_nodes, uint32_t relationship_field_id, roaring_bitmap_t *source_bitmap);

    std::vector<uint32_t> find_shortest_path(uint32_t start_node, uint32_t end_node, uint32_t relationship_field_id);
    uint64_t count_triangles(uint32_t relationship_field_id);
    std::unique_ptr<QueryPipeline> get_common_neighbors(uint32_t node1, uint32_t node2, uint32_t relationship_field_id);
    bool has_relationship(uint32_t source_obj_id, uint32_t relationship_field_id, uint32_t target_obj_id);

private:
    std::optional<DataView> get_property_for_object_direct(uint32_t obj_id, uint32_t field_id);

    ::Database *db_;
    const TxnContext &ctx_;
    ::Collection *ofv_col_;
    ::Collection *fvo_col_;
};

class GraphTransaction
{
public:
    GraphTransaction(::Database *db, size_t thread_id);
    GraphTransaction(::Database *db, size_t thread_id, TxnID explicit_read_snapshot_id, TxnID explicit_commit_id);
    ~GraphTransaction();

    TxnID get_txn_id() const;
    TxnID get_read_snapshot_id() const;

    
    void insert_fact(uint32_t obj_id, uint32_t field_id, uint32_t val_id);
    
    void insert_fact_string(uint32_t obj_id, uint32_t field_id, std::string_view field_name, std::string_view value_str);
    
    void insert_fact_numeric(uint32_t obj_id, uint32_t field_id, std::string_view field_name, uint64_t numeric_val);
    
    void insert_fact_geo(uint32_t obj_id, uint32_t field_id, std::string_view field_name, double latitude, double longitude);
    
    void update_object(uint32_t obj_id, const std::vector<StaxObjectProperty>& properties);
    
    void remove_fact(uint32_t obj_id, uint32_t field_id, uint32_t val_id);
    void remove_fact(uint32_t obj_id, uint32_t field_id, std::string_view value_str);
    void remove_fact_numeric(uint32_t obj_id, uint32_t field_id, uint64_t numeric_val);
    
    void clear_object_facts(uint32_t obj_id);

    void commit();
    void abort();

    static constexpr size_t BINARY_U32_SIZE = 4;
    static constexpr size_t BINARY_U64_SIZE = 8;

private:
    ::Database *db_;
    size_t thread_id_;

    TxnContext ctx_;

    TransactionBatch ofv_batch_deltas_;
    TransactionBatch fvo_batch_deltas_;

    uint32_t ofv_col_idx_;
    uint32_t fvo_col_idx_;

    ::Collection *ofv_col_;
    ::Collection *fvo_col_;

    bool is_finished_ = false;
    bool has_writes_ = false;

    static constexpr size_t MAX_BATCH_KEY_DATA_SIZE = MAX_GRAPH_BATCH_KEY_DATA_SIZE;
    static constexpr size_t MAX_KV_PAIRS_PER_BATCH = MAX_GRAPH_KV_PAIRS_PER_BATCH;
    static constexpr size_t BATCH_FLUSH_THRESHOLD_KVS = GRAPH_BATCH_FLUSH_THRESHOLD_KVS;

    size_t ofv_data_offset_;
    size_t fvo_data_offset_;

    size_t ofv_kv_pairs_count_;
    size_t fvo_kv_pairs_count_;

    std::unique_ptr<char[]> ofv_kv_data_buffer_;
    std::unique_ptr<char[]> fvo_kv_data_buffer_;

    std::unique_ptr<CoreKVPair[]> ofv_kv_pairs_array_;
    std::unique_ptr<CoreKVPair[]> fvo_kv_pairs_array_;

    std::set<uint32_t> seen_relationship_field_ids_;
    void track_relationship_field(uint32_t field_id);
    void clear_object_properties(uint32_t obj_id);

    void multi_insert_low_level_on_collection(::Collection *target_col, const TxnContext &ctx, TransactionBatch &batch, const CoreKVPair *kv_pairs, size_t num_kvs, uint64_t total_live_bytes_to_add);

    void flush_pending_writes();

    struct InternalFact
    {
        uint32_t obj_id;
        uint32_t field_id;
        uint32_t value_id;
        bool operator<(const InternalFact &other) const
        {
            if (obj_id != other.obj_id)
                return obj_id < other.obj_id;
            return field_id < other.field_id;
        }
    };
};