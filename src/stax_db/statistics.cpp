
#include "stax_db/statistics.h"
#include "stax_db/db.h"
#include "stax_core/stax_tree.hpp" 
#include "stax_core/value_store.hpp" 
#include "stax_common/os_file_extensions.h" 
#include <iostream> 
#include <functional> 

namespace StaxStats {

DatabaseStatisticsCollector::DatabaseStatisticsCollector(Database* db_instance) : db_ref_(db_instance) {
    if (!db_instance) {
        throw std::runtime_error("DatabaseStatisticsCollector: Database instance cannot be null.");
    }
}

DatabaseStats DatabaseStatisticsCollector::get_database_summary_stats(bool include_physical_memory_stats) {
    DatabaseStats stats;
    
    std::vector<const DbGeneration*> generations_snapshot;
    {
        UniqueSpinLockGuard lock(db_ref_->get_generations_lock());
        const auto& generations = db_ref_->get_generations();
        stats.active_generations_count = static_cast<uint32_t>(generations.size());
        for (const auto& gen_ptr : generations) {
            generations_snapshot.push_back(gen_ptr.get());
        }
    }

    for (const DbGeneration* gen : generations_snapshot) {
        if (gen && gen->file_header) {
            uint64_t current_gen_logical_size = sizeof(FileHeader);
            current_gen_logical_size += gen->file_header->collection_array_capacity * sizeof(CollectionEntry);
            
            
            
            current_gen_logical_size += gen->file_header->global_alloc_offset.load(std::memory_order_acquire);
            
            stats.total_logical_allocated_bytes += current_gen_logical_size;

            uint32_t num_collections_in_gen = gen->file_header->collection_array_count.load(std::memory_order_acquire);
            for (uint32_t i = 0; i < num_collections_in_gen; ++i) {
                const CollectionEntry& entry = gen->get_collection_entry_ref(i);
                stats.total_logical_item_count += entry.logical_item_count.load(std::memory_order_acquire);
                stats.total_live_data_bytes += entry.live_record_bytes.load(std::memory_order_acquire);
            }
            
            stats.total_allocated_disk_bytes += gen->mmap_size;

            if (include_physical_memory_stats) {
                stats.total_resident_memory_bytes += OSFileExtensions::get_resident_memory_for_range(gen->mmap_base, gen->mmap_size);
            }

            if (gen == generations_snapshot.front()) {
                stats.total_collections_count = num_collections_in_gen;
            }
        }
    }

    return stats;
}

std::map<uint32_t, CollectionStats> DatabaseStatisticsCollector::get_all_collection_stats() {
    std::map<uint32_t, CollectionStats> all_stats;
    
    uint32_t num_collections_to_scan = 0;
    const DbGeneration* active_gen_snapshot = nullptr;
    {
        UniqueSpinLockGuard lock(db_ref_->get_generations_lock());
        if (db_ref_->get_generations().empty()) {
            return all_stats;
        }
        active_gen_snapshot = db_ref_->get_generations().front().get();
        num_collections_to_scan = active_gen_snapshot->file_header->collection_array_count.load(std::memory_order_acquire);
    }

    for (uint32_t i = 0; i < num_collections_to_scan; ++i) {
        if (i < active_gen_snapshot->owned_collections.size() && active_gen_snapshot->owned_collections[i]) {
            CollectionStats col_stats = calculate_collection_stats_for_generation(i, *active_gen_snapshot);
            all_stats[i] = col_stats;
        }
    }

    return all_stats;
}

CollectionStats DatabaseStatisticsCollector::get_collection_stats(uint32_t collection_idx) {
    const DbGeneration* active_gen_snapshot = nullptr;
    bool is_valid_collection = false;
    {
        UniqueSpinLockGuard lock(db_ref_->get_generations_lock());
        if (db_ref_->get_generations().empty()) {
            return CollectionStats();
        }
        active_gen_snapshot = db_ref_->get_generations().front().get();
        if (collection_idx < active_gen_snapshot->file_header->collection_array_count.load(std::memory_order_acquire) &&
            collection_idx < active_gen_snapshot->owned_collections.size() && 
            active_gen_snapshot->owned_collections[collection_idx]) {
            is_valid_collection = true;
        }
    } 

    if (is_valid_collection) {
        return calculate_collection_stats_for_generation(collection_idx, *active_gen_snapshot);
    }

    return CollectionStats();
}

CollectionStats DatabaseStatisticsCollector::calculate_collection_stats_for_generation(uint32_t collection_idx, const DbGeneration& gen) {
    CollectionStats stats;
    stats.collection_idx = collection_idx;
    
    const CollectionEntry& collection_entry = gen.get_collection_entry_ref(collection_idx);

    stats.collection_name_hash_str = "col_hash_" + std::to_string(collection_entry.name_hash);

    stats.logical_item_count = collection_entry.logical_item_count.load(std::memory_order_acquire);
    stats.live_record_bytes = collection_entry.live_record_bytes.load(std::memory_order_acquire);
    
    stats.total_internal_node_bytes = 0;
    stats.reclaimed_internal_node_bytes = 0;
    
    stats.value_store_reclaimable_space_ratio = 0.0; 
    stats.internal_node_fragmentation_ratio = 0.0; 

    return stats;
}


} 