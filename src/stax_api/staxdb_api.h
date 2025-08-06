#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h> 

#include "stax_common/common_types.hpp"

#ifdef __cplusplus

class Database;

struct roaring_bitmap_s;
#endif

#ifdef __cplusplus
extern "C" {
#endif


struct StaxDB_t;
typedef struct StaxDB_t* StaxDB;
struct StaxGraph_t;
typedef struct StaxGraph_t* StaxGraph;


struct roaring_bitmap_s; 
typedef struct roaring_bitmap_s roaring_bitmap_t;


enum StaxResultSetType : uint8_t {
    KV_RESULT = 0,
    GRAPH_ID_RESULT = 1
};




struct StaxResultSet_t {
    uint8_t result_type; 
    roaring_bitmap_t* bitmap; 
    void* kv_data; 
};
typedef struct StaxResultSet_t* StaxResultSet;


typedef uint32_t StaxCollection;


typedef struct {
    StaxSlice data;
    bool found;
} StaxOptionalSlice;

typedef struct {
    StaxSlice key;
    StaxSlice value;
} StaxKVPair;

typedef enum {
    StaxDurability_NoSync = 0,
    StaxDurability_SyncOnCommit = 1
} StaxDurabilityLevel;


typedef struct {
    StaxSlice start_key; 
    StaxSlice end_key;   
} StaxQueryOptions;


typedef struct {
    uint32_t page_number;
    uint32_t total_pages;
    uint64_t total_results;
    uint32_t results_in_page;
    const StaxKVPair* results; 
} StaxPageResult;


typedef enum {
    STAX_GRAPH_FIND_BY_PROPERTY,
    STAX_GRAPH_TRAVERSE,
    STAX_GRAPH_INTERSECT,
    STAX_GRAPH_UNION
} StaxGraphQueryOpType;

typedef enum {
    STAX_GRAPH_OUTGOING,
    STAX_GRAPH_INCOMING
} StaxGraphTraversalDirection;


typedef struct {
    StaxGraphQueryOpType op_type;
    StaxGraphTraversalDirection direction;
    StaxSlice field;
    bool uses_numeric_range; 
    bool has_filter;
    uint8_t filter_property_count;
} StaxGraphQueryStep;



StaxDB staxdb_init_path(const char* path, size_t num_threads, StaxDurabilityLevel durability_level);
void staxdb_close(StaxDB db);
void staxdb_drop(const char* path);


#ifdef __cplusplus
Database* staxdb_get_db_instance(StaxDB db);
#else
void* staxdb_get_db_instance(StaxDB db); 
#endif


StaxCollection staxdb_get_collection(StaxDB db, StaxSlice name);


void staxdb_insert(StaxDB db, StaxCollection collection_idx, StaxSlice key, StaxSlice value);
void staxdb_remove(StaxDB db, StaxCollection collection_idx, StaxSlice key);
StaxOptionalSlice staxdb_get(StaxDB db, StaxCollection collection_idx, StaxSlice key);
void staxdb_insert_batch(StaxDB db, StaxCollection collection_idx, const StaxKVPair* pairs, size_t num_pairs);


#ifdef __cplusplus
StaxResultSet staxdb_execute_range_query(Database* db_instance, StaxCollection collection_idx, const StaxQueryOptions* options);
#else
StaxResultSet staxdb_execute_range_query(void* db_instance_ptr, StaxCollection collection_idx, const StaxQueryOptions* options);
#endif


StaxGraph staxdb_get_graph(StaxDB db);
uint32_t staxdb_graph_insert_object(StaxGraph graph, const StaxObjectProperty* properties, size_t num_properties);
void staxdb_graph_insert_fact_string(StaxGraph graph, uint32_t obj_id, StaxSlice field, StaxSlice value);
void staxdb_graph_insert_fact_numeric(StaxGraph graph, uint32_t obj_id, StaxSlice field, uint64_t value);
void staxdb_graph_insert_fact_geo(StaxGraph graph, uint32_t obj_id, StaxSlice field, double latitude, double longitude);
void staxdb_graph_insert_relationship(StaxGraph graph, uint32_t source_id, StaxSlice rel_type, uint32_t target_id);
void staxdb_graph_commit(StaxGraph graph);

uint32_t staxdb_graph_compile_plan(StaxGraph graph, const StaxGraphQueryStep* steps, size_t num_steps);
StaxResultSet staxdb_graph_execute_plan(StaxGraph graph, uint32_t plan_id, const StaxSlice* params, size_t num_params);


void staxdb_graph_update_object(StaxGraph graph, uint32_t obj_id, const StaxObjectProperty* properties, size_t num_properties);
void staxdb_graph_delete_object(StaxGraph graph, uint32_t obj_id);
StaxResultSet staxdb_graph_get_object(StaxGraph graph, uint32_t obj_id);


void staxdb_graph_update_fact_string(StaxGraph graph, uint32_t obj_id, StaxSlice field, StaxSlice new_value);
void staxdb_graph_update_fact_numeric(StaxGraph graph, uint32_t obj_id, StaxSlice field, uint64_t new_value);



StaxPageResult staxdb_resultset_get_page(StaxResultSet result_set, uint32_t page_number, uint32_t page_size);
uint64_t staxdb_resultset_get_total_count(StaxResultSet result_set);
void staxdb_resultset_free(StaxResultSet result_set);


const char* staxdb_get_last_error();


#ifdef __cplusplus
} 
#endif