
#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <map>
#include <atomic>
#include <memory>
#include <numeric> 


#include "stax_db/db.h"

namespace StaxStats {


struct CollectionStats {
    uint32_t collection_idx;
    std::string collection_name_hash_str; 
    uint64_t logical_item_count;       
    
    uint64_t live_record_bytes;        
    uint64_t total_internal_node_bytes; 
    uint64_t reclaimed_internal_node_bytes; 
    double value_store_reclaimable_space_ratio; 
    double internal_node_fragmentation_ratio; 

    CollectionStats() :
        collection_idx(0),
        logical_item_count(0),
        live_record_bytes(0),
        total_internal_node_bytes(0),
        reclaimed_internal_node_bytes(0),
        value_store_reclaimable_space_ratio(0.0), 
        internal_node_fragmentation_ratio(0.0) {}
};


struct DatabaseStats {
    uint64_t total_logical_item_count;
    uint64_t total_allocated_disk_bytes; 
    uint64_t total_resident_memory_bytes; 
    uint64_t total_live_data_bytes;     
    uint64_t total_logical_allocated_bytes; 
    uint32_t active_generations_count; 
    uint32_t total_collections_count; 

    DatabaseStats() :
        total_logical_item_count(0),
        total_allocated_disk_bytes(0),
        total_resident_memory_bytes(0),
        total_live_data_bytes(0),
        total_logical_allocated_bytes(0), 
        active_generations_count(0),
        total_collections_count(0) {}
};


class DatabaseStatisticsCollector {
public:
    DatabaseStatisticsCollector(Database* db_instance);

    DatabaseStats get_database_summary_stats(bool include_physical_memory_stats = false);

    std::map<uint32_t, CollectionStats> get_all_collection_stats();

    CollectionStats get_collection_stats(uint32_t collection_idx);

private:
    Database* db_ref_;

    CollectionStats calculate_collection_stats_for_generation(uint32_t collection_idx, const DbGeneration& gen);
};

} 