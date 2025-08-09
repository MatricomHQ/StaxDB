#include "stax_api/staxdb_api.h"
#include "stax_db/db.h"
#include "stax_tx/transaction.h"
#include "stax_tx/db_cursor.hpp"
#include "stax_graph/graph_engine.h"
#include "stax_common/roaring.h"
#include "stax_common/binary_utils.h" 
#include <string>
#include <stdexcept>
#include <iostream>
#include <memory>
#include <filesystem>
#include <vector>
#include <thread>
#include <algorithm>
#include <cmath>
#include <charconv> 
#include <map>
#include <set>
#include <cstdio> // For printf

static void hex_dump_slice(const char* prefix, StaxSlice s) {
    printf("%s [size=%zu]: ", prefix, s.len);
    if (!s.data) {
        printf(" (null)\n");
        return;
    }
    for (size_t i = 0; i < s.len; ++i) {
        printf("%02hhx ", static_cast<unsigned char>(s.data[i]));
    }
    printf("\n");
}


struct CompiledQueryStep {
    StaxGraphQueryOpType op_type;
    StaxGraphTraversalDirection direction;
    std::string field_name; 
    bool uses_numeric_range;
    bool has_filter;
    uint8_t filter_property_count;
};





struct StaxKVResultSetData_t {
    std::vector<char> data_buffer;
    std::vector<StaxKVPair> kv_pairs;
};


struct StaxDB_t {
    std::unique_ptr<Database> db;
};

struct StaxGraph_t {
    Database* db_instance;
    std::vector<std::vector<CompiledQueryStep>> compiled_plans;
};



static thread_local std::string last_error_message;

static void set_last_error(const char* msg) {
    last_error_message = msg;
}

static void clear_last_error() {
    last_error_message.clear();
}


StaxDB staxdb_init_path(const char* path, size_t num_threads, StaxDurabilityLevel durability_level_enum) {
    clear_last_error();
    try {
        if (!path) {
            set_last_error("Database path cannot be null.");
            return NULL;
        }
        std::filesystem::path db_dir(path);
        DurabilityLevel cpp_durability_level = (durability_level_enum == StaxDurability_SyncOnCommit) ? DurabilityLevel::SyncOnCommit : DurabilityLevel::NoSync;

        std::unique_ptr<Database> db_ptr;
        if (std::filesystem::exists(db_dir) && std::filesystem::exists(db_dir / "data.stax")) {
            db_ptr = Database::open_existing(db_dir, num_threads, cpp_durability_level);
        } else {
            std::filesystem::create_directories(db_dir);
            db_ptr = Database::create_new(db_dir, num_threads, cpp_durability_level);
        }
        
        if (!db_ptr) {
            set_last_error("Failed to initialize database instance (db_ptr is null).");
            return NULL;
        }

        StaxDB_t* handle = new StaxDB_t{std::move(db_ptr)};
        return handle;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}

void staxdb_close(StaxDB db) {
    clear_last_error();
    try {
        if (db) {
            delete db;
        }
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_drop(const char* path) {
    clear_last_error();
    try {
        if (!path) {
            set_last_error("Database path for drop cannot be null.");
            return;
        }
        Database::drop(path);
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}


Database* staxdb_get_db_instance(StaxDB db) {
    clear_last_error();
    try {
        if (!db) return nullptr;
        return db->db.get();
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}


StaxCollection staxdb_get_collection(StaxDB db, StaxSlice name) {
    clear_last_error();
    const uint32_t ERROR_VAL = std::numeric_limits<uint32_t>::max();
    if (!db || !db->db) {
        set_last_error("Database handle is NULL in get_collection.");
        return ERROR_VAL;
    }
    if (!name.data && name.len > 0) {
        set_last_error("Collection name slice data is NULL.");
        return ERROR_VAL;
    }
    try {
        uint32_t col_idx = db->db->get_collection(to_string_view(name));
        return col_idx;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return ERROR_VAL;
    }
}


void staxdb_insert(StaxDB db, StaxCollection collection_idx, StaxSlice key, StaxSlice value) {
    clear_last_error();
    if (!db || !db->db) { set_last_error("Database handle is NULL."); return; }
    if (!key.data && key.len > 0) { set_last_error("Key slice data is NULL."); return; }
    if (!value.data && value.len > 0) { set_last_error("Value slice data is NULL."); return; }
    try {
        db->db->get_collection_by_idx(collection_idx).insert_sync_direct(to_string_view(key), to_string_view(value), 0);
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_remove(StaxDB db, StaxCollection collection_idx, StaxSlice key) {
    clear_last_error();
    if (!db || !db->db) { set_last_error("Database handle is NULL."); return; }
    if (!key.data && key.len > 0) { set_last_error("Key slice data is NULL."); return; }
    try {
        db->db->get_collection_by_idx(collection_idx).remove_sync_direct(to_string_view(key), 0);
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

StaxOptionalSlice staxdb_get(StaxDB db, StaxCollection collection_idx, StaxSlice key) {
    clear_last_error();
    static thread_local std::string value_buffer;
    if (!db || !db->db) {
        set_last_error("Database handle is NULL.");
        return {{nullptr, 0}, false};
    }
    if (!key.data && key.len > 0) {
        set_last_error("Key slice data is NULL.");
        return {{nullptr, 0}, false};
    }
    try {
        Collection& col = db->db->get_collection_by_idx(collection_idx);
        TxnContext ctx = col.begin_transaction_context(0, true);
        auto record = col.get(ctx, to_string_view(key));
        if (record.has_value()) {
            value_buffer.assign(record->value_ptr, record->value_len);
            return {{value_buffer.data(), value_buffer.length()}, true};
        }
        return {{nullptr, 0}, false};
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return {{nullptr, 0}, false};
    }
}


void staxdb_insert_batch(StaxDB db, StaxCollection collection_idx, const StaxKVPair* pairs, size_t num_pairs) {
    clear_last_error();
    if (!db || !db->db) { set_last_error("Database handle is NULL in insert_batch."); return; }
    if (!pairs && num_pairs > 0) { set_last_error("K/V pairs pointer is NULL for batch insert."); return; }
    try {
        Database* cpp_db = db->db.get();
        Collection& col = cpp_db->get_collection_by_idx(collection_idx);
        
        TxnContext ctx = col.begin_transaction_context(0, false);
        TransactionBatch batch;
        for (size_t i = 0; i < num_pairs; ++i) {
            col.insert(ctx, batch, to_string_view(pairs[i].key), to_string_view(pairs[i].value));
        }
        col.commit(ctx, batch);

    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}


StaxResultSet staxdb_execute_range_query(Database* db_instance, StaxCollection collection_idx, const StaxQueryOptions* options) {
    clear_last_error();
    try {
        if (!db_instance) {
            set_last_error("Database instance is NULL in execute_range_query.");
            return NULL;
        }

        auto kv_data = new StaxKVResultSetData_t();
        
        TxnContext ctx = db_instance->begin_transaction_context(0, true);
        Collection& col = db_instance->get_collection_by_idx(collection_idx);
        
        std::string_view start_key = (options && options->start_key.data) ? to_string_view(options->start_key) : "";
        std::optional<std::string_view> end_key;
        if (options && options->end_key.data && options->end_key.len > 0) {
            end_key = to_string_view(options->end_key);
        }

        
        for (auto cursor = col.seek(ctx, start_key, end_key); cursor->is_valid(); cursor->next()) {
            std::string_view key_sv = cursor->key();
            DataView value_dv = cursor->value();
            
            size_t key_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), key_sv.begin(), key_sv.end());
            
            size_t value_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), value_dv.data, value_dv.data + value_dv.len);

            
            kv_data->kv_pairs.push_back({
                {reinterpret_cast<const char*>(key_offset), key_sv.length()},
                {reinterpret_cast<const char*>(value_offset), value_dv.len}
            });
        }

        
        const char* base_ptr = kv_data->data_buffer.data();
        for (auto& pair : kv_data->kv_pairs) {
            pair.key.data = base_ptr + reinterpret_cast<size_t>(pair.key.data);
            pair.value.data = base_ptr + reinterpret_cast<size_t>(pair.value.data);
        }
        
        auto result_set = new StaxResultSet_t{KV_RESULT, nullptr, kv_data};
        return result_set;

    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}



StaxGraph staxdb_get_graph(StaxDB db) {
    clear_last_error();
    if (!db || !db->db) {
        set_last_error("Database handle is NULL in get_graph.");
        return NULL;
    }
    try {
        auto graph_handle = new StaxGraph_t();
        graph_handle->db_instance = db->db.get();
        return graph_handle;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}

void staxdb_graph_insert_fact_string(StaxGraph graph, uint32_t obj_id, StaxSlice field, StaxSlice value) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        txn.insert_fact_string(obj_id, to_string_view(field), to_string_view(value));
        txn.commit();
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_graph_insert_fact_numeric(StaxGraph graph, uint32_t obj_id, StaxSlice field, uint64_t value) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        txn.insert_fact_numeric(obj_id, to_string_view(field), value);
        txn.commit();
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_graph_insert_fact_geo(StaxGraph graph, uint32_t obj_id, StaxSlice field, double latitude, double longitude) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        txn.insert_fact_geo(obj_id, to_string_view(field), latitude, longitude);
        txn.commit();
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_graph_insert_relationship(StaxGraph graph, uint32_t source_id, StaxSlice rel_type, uint32_t target_id) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        txn.insert_fact(source_id, to_string_view(rel_type), target_id);
        txn.commit();
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}

void staxdb_graph_commit(StaxGraph graph) {
    clear_last_error();
}

uint32_t staxdb_graph_insert_object(StaxGraph graph, const StaxObjectProperty* properties, size_t num_properties) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return 0; }
    if (!properties && num_properties > 0) { set_last_error("Properties pointer is NULL for object insert."); return 0; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        DbGeneration* active_gen = graph->db_instance->get_active_generation();
        if (!active_gen) {
            set_last_error("No active database generation found.");
            return 0;
        }

        const uint32_t graph_collection_idx = graph->db_instance->get_collection("graph_ofv"); 
        CollectionEntry& graph_collection_entry = active_gen->get_collection_entry_ref(graph_collection_idx);
        uint32_t obj_id = graph_collection_entry.object_id_counter.fetch_add(1, std::memory_order_relaxed);

        for (size_t i = 0; i < num_properties; ++i) {
            const auto& prop = properties[i];
            std::string_view field_name_sv = to_string_view(prop.field);
            switch (prop.type) {
                case STAX_PROP_STRING:
                    txn.insert_fact_string(obj_id, field_name_sv, to_string_view(prop.value.string_val));
                    break;
                case STAX_PROP_NUMERIC:
                    txn.insert_fact_numeric(obj_id, field_name_sv, prop.value.numeric_val);
                    break;
                case STAX_PROP_GEO:
                    txn.insert_fact_geo(obj_id, field_name_sv, prop.value.geo_val.lat, prop.value.geo_val.lon);
                    break;
            }
        }
        txn.commit();
        return obj_id;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return 0;
    }
}

void staxdb_graph_update_object(StaxGraph graph, uint32_t obj_id, const StaxObjectProperty* properties, size_t num_properties) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    if (!properties && num_properties > 0) { set_last_error("Properties pointer is NULL for object update."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        std::vector<StaxObjectProperty> props_vec(properties, properties + num_properties);
        txn.update_object(obj_id, props_vec);
        txn.commit();
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}


StaxResultSet staxdb_graph_get_object(StaxGraph graph, uint32_t obj_id) {
    clear_last_error();
    if (!graph || !graph->db_instance) {
        set_last_error("Graph handle is invalid.");
        return NULL;
    }
    try {
        auto kv_data = new StaxKVResultSetData_t();
        TxnContext read_ctx = graph->db_instance->begin_transaction_context(0, true);
        
        GraphReader reader(graph->db_instance, read_ctx);
        auto all_facts = reader.get_properties_and_relationships(obj_id);

        
        std::string id_key_str = "__stax_id";
        std::string id_val_str = std::to_string(obj_id);
        size_t key_offset = kv_data->data_buffer.size();
        kv_data->data_buffer.insert(kv_data->data_buffer.end(), id_key_str.begin(), id_key_str.end());
        size_t val_offset = kv_data->data_buffer.size();
        kv_data->data_buffer.insert(kv_data->data_buffer.end(), id_val_str.begin(), id_val_str.end());
        kv_data->kv_pairs.push_back({{reinterpret_cast<const char*>(key_offset), id_key_str.length()}, {reinterpret_cast<const char*>(val_offset), id_val_str.length()}});


        for (const auto& fact : all_facts) {
            const auto& [subj, pred, obj] = fact;
            key_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), pred.begin(), pred.end());
            val_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), obj.begin(), obj.end());
            kv_data->kv_pairs.push_back({{reinterpret_cast<const char*>(key_offset), pred.length()}, {reinterpret_cast<const char*>(val_offset), obj.length()}});
        }

        const char* base_ptr = kv_data->data_buffer.data();
        for (auto& pair : kv_data->kv_pairs) {
            pair.key.data = base_ptr + reinterpret_cast<size_t>(pair.key.data);
            pair.value.data = base_ptr + reinterpret_cast<size_t>(pair.value.data);
        }
        
        return new StaxResultSet_t{KV_RESULT, nullptr, kv_data};

    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}

StaxResultSet staxdb_graph_get_object_properties(StaxGraph graph, uint32_t obj_id) {
    clear_last_error();
    if (!graph || !graph->db_instance) {
        set_last_error("Graph handle is invalid.");
        return NULL;
    }
    try {
        auto kv_data = new StaxKVResultSetData_t();
        TxnContext read_ctx = graph->db_instance->begin_transaction_context(0, true);
        
        GraphReader reader(graph->db_instance, read_ctx);
        auto all_facts = reader.get_properties(obj_id);

        std::string id_key_str = "__stax_id";
        std::string id_val_str = std::to_string(obj_id);
        size_t key_offset = kv_data->data_buffer.size();
        kv_data->data_buffer.insert(kv_data->data_buffer.end(), id_key_str.begin(), id_key_str.end());
        size_t val_offset = kv_data->data_buffer.size();
        kv_data->data_buffer.insert(kv_data->data_buffer.end(), id_val_str.begin(), id_val_str.end());
        kv_data->kv_pairs.push_back({{reinterpret_cast<const char*>(key_offset), id_key_str.length()}, {reinterpret_cast<const char*>(val_offset), id_val_str.length()}});


        for (const auto& fact : all_facts) {
            const auto& [subj, pred, obj] = fact;
            key_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), pred.begin(), pred.end());
            val_offset = kv_data->data_buffer.size();
            kv_data->data_buffer.insert(kv_data->data_buffer.end(), obj.begin(), obj.end());
            kv_data->kv_pairs.push_back({{reinterpret_cast<const char*>(key_offset), pred.length()}, {reinterpret_cast<const char*>(val_offset), obj.length()}});
        }

        const char* base_ptr = kv_data->data_buffer.data();
        for (auto& pair : kv_data->kv_pairs) {
            pair.key.data = base_ptr + reinterpret_cast<size_t>(pair.key.data);
            pair.value.data = base_ptr + reinterpret_cast<size_t>(pair.value.data);
        }
        
        return new StaxResultSet_t{KV_RESULT, nullptr, kv_data};

    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}


uint32_t staxdb_graph_compile_plan(StaxGraph graph, const StaxGraphQueryStep* steps, size_t num_steps) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return -1; }
    try {
        std::vector<CompiledQueryStep> compiled_steps;
        compiled_steps.reserve(num_steps);
        for (size_t i = 0; i < num_steps; ++i) {
            const auto& step = steps[i];
            compiled_steps.push_back({
                step.op_type,
                step.direction,
                std::string(to_string_view(step.field)),
                step.uses_numeric_range,
                step.has_filter,
                step.filter_property_count
            });
        }
        graph->compiled_plans.push_back(std::move(compiled_steps));
        return static_cast<uint32_t>(graph->compiled_plans.size() - 1);
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return -1;
    }
}

StaxResultSet staxdb_graph_execute_plan(StaxGraph graph, uint32_t plan_id, const StaxSlice* params, size_t num_params) {
    clear_last_error();
    printf("\n--- [C-API] staxdb_graph_execute_plan ---\n");
    printf("Plan ID: %u, Num Params: %zu\n", plan_id, num_params);
    for (size_t i = 0; i < num_params; ++i) {
        char prefix[32];
        snprintf(prefix, sizeof(prefix), "  Param %zu", i);
        hex_dump_slice(prefix, params[i]);
    }

    if (!graph || !graph->db_instance) { set_last_error("Graph handle is invalid."); return NULL; }
    if (plan_id >= graph->compiled_plans.size()) { set_last_error("Invalid query plan ID."); return NULL; }

    try {
        const auto& plan = graph->compiled_plans[plan_id];
        TxnContext read_ctx = graph->db_instance->begin_transaction_context(0, true);
        GraphReader reader(graph->db_instance, read_ctx);
        roaring_bitmap_t* current_results = roaring_bitmap_create();
        size_t param_idx = 0;

        for (const auto& step : plan) {
            roaring_bitmap_t* step_results = roaring_bitmap_create();
            auto get_filter_results = [&](roaring_bitmap_t* target_bitmap) {
                if (step.uses_numeric_range) {
                    if (param_idx + 1 >= num_params) { set_last_error("Insufficient params for numeric range."); return false; }
                    uint64_t gte = from_binary_key_u64(to_string_view(params[param_idx++]));
                    uint64_t lte = from_binary_key_u64(to_string_view(params[param_idx++]));
                    reader.get_objects_by_property_range_into_roaring(step.field_name, gte, lte, target_bitmap);
                } else {
                    if (param_idx >= num_params) { set_last_error("Insufficient params for property query."); return false; }
                    reader.get_objects_by_property_into_roaring(step.field_name, to_string_view(params[param_idx++]), target_bitmap);
                }
                return true;
            };
            switch (step.op_type) {
                case STAX_GRAPH_FIND_BY_PROPERTY:
                    roaring_bitmap_free(current_results);
                    if (!get_filter_results(step_results)) { roaring_bitmap_free(step_results); return NULL; }
                    current_results = step_results;
                    break;
                case STAX_GRAPH_TRAVERSE:
                    if (step.direction == STAX_GRAPH_OUTGOING) reader.get_outgoing_relationships_for_many_into_roaring(current_results, step.field_name, step_results);
                    else reader.get_incoming_relationships_for_many_into_roaring(current_results, step.field_name, step_results);
                    
                    if (step.has_filter) {
                        roaring_bitmap_t* final_filter_bitmap = roaring_bitmap_create();
                        for(uint8_t i = 0; i < step.filter_property_count; ++i) {
                            roaring_bitmap_t* prop_filter_bitmap = roaring_bitmap_create();
                            if (param_idx + 1 >= num_params) { set_last_error("Insufficient params for traverse filter."); break; }
                            std::string_view filter_field_name = to_string_view(params[param_idx++]);
                            std::string_view filter_value_str = to_string_view(params[param_idx++]);
                            reader.get_objects_by_property_into_roaring(filter_field_name, filter_value_str, prop_filter_bitmap);
                            if(i == 0) roaring_bitmap_or_inplace(final_filter_bitmap, prop_filter_bitmap);
                            else roaring_bitmap_and_inplace(final_filter_bitmap, prop_filter_bitmap);
                            roaring_bitmap_free(prop_filter_bitmap);
                        }
                        roaring_bitmap_and_inplace(step_results, final_filter_bitmap);
                        roaring_bitmap_free(final_filter_bitmap);
                    }

                    roaring_bitmap_free(current_results);
                    current_results = step_results;
                    break;
                case STAX_GRAPH_INTERSECT:
                    if (!get_filter_results(step_results)) { roaring_bitmap_free(step_results); roaring_bitmap_free(current_results); return NULL; }
                    roaring_bitmap_and_inplace(current_results, step_results);
                    roaring_bitmap_free(step_results);
                    break;
                case STAX_GRAPH_UNION:
                    if (!get_filter_results(step_results)) { roaring_bitmap_free(step_results); roaring_bitmap_free(current_results); return NULL; }
                    roaring_bitmap_or_inplace(current_results, step_results);
                    roaring_bitmap_free(step_results);
                    break;
            }
        }
        
        auto result_set = new StaxResultSet_t{GRAPH_ID_RESULT, current_results, graph};
        return result_set;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return NULL;
    }
}

void staxdb_graph_update_fact_string(StaxGraph graph, uint32_t obj_id, StaxSlice field, StaxSlice new_value) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        TxnContext read_ctx = {0, txn.get_read_snapshot_id(), 0};
        GraphReader reader(graph->db_instance, read_ctx);
        std::string_view field_name_sv = to_string_view(field);
        auto old_value_opt = reader.get_property_for_object_string(obj_id, field_name_sv);
        if (old_value_opt) txn.remove_fact(obj_id, field_name_sv, *old_value_opt);
        txn.insert_fact_string(obj_id, field_name_sv, to_string_view(new_value));
        txn.commit();
    } catch (const std::exception& e) { set_last_error(e.what()); }
}

void staxdb_graph_update_fact_numeric(StaxGraph graph, uint32_t obj_id, StaxSlice field, uint64_t new_value) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        TxnContext read_ctx = {0, txn.get_read_snapshot_id(), 0};
        GraphReader reader(graph->db_instance, read_ctx);
        std::string_view field_name_sv = to_string_view(field);
        auto old_value_opt = reader.get_property_for_object_numeric(obj_id, field_name_sv);
        if (old_value_opt) txn.remove_fact_numeric(obj_id, field_name_sv, *old_value_opt);
        txn.insert_fact_numeric(obj_id, field_name_sv, new_value);
        txn.commit();
    } catch (const std::exception& e) { set_last_error(e.what()); }
}

void staxdb_graph_delete_object(StaxGraph graph, uint32_t obj_id) {
    clear_last_error();
    if (!graph) { set_last_error("Graph handle is invalid."); return; }
    try {
        GraphTransaction txn(graph->db_instance, 0);
        txn.clear_object_facts(obj_id);
        txn.commit();
    } catch (const std::exception& e) { set_last_error(e.what()); }
}



StaxPageResult staxdb_resultset_get_page(StaxResultSet result_set, uint32_t page_number, uint32_t page_size) {
    clear_last_error();
    static thread_local std::vector<StaxKVPair> page_kv_pairs;
    static thread_local std::vector<char> page_data_buffer;
    
    StaxPageResult page = {};
    try {
        if (!result_set || page_size == 0 || page_number < 1) return page;

        if (result_set->result_type == KV_RESULT) {
            if (!result_set->kv_data) return page;
            StaxKVResultSetData_t* kv_data = reinterpret_cast<StaxKVResultSetData_t*>(result_set->kv_data);
            page.total_results = kv_data->kv_pairs.size();
            if (page.total_results == 0) return page;
            page.total_pages = static_cast<uint32_t>(std::ceil(static_cast<double>(page.total_results) / page_size));
            
            if (page_number > page.total_pages) return page;
            
            page.page_number = page_number;
            size_t start_index = (page.page_number - 1) * page_size;
            
            size_t end_index = std::min(start_index + page_size, static_cast<size_t>(page.total_results));
            page.results_in_page = static_cast<uint32_t>(end_index - start_index);
            page.results = &kv_data->kv_pairs[start_index];

        } else if (result_set->result_type == GRAPH_ID_RESULT) {
            if (!result_set->bitmap) return page;
            page.total_results = roaring_bitmap_get_cardinality(result_set->bitmap);
            if (page.total_results == 0) return page;
            page.total_pages = static_cast<uint32_t>(std::ceil(static_cast<double>(page.total_results) / page_size));

            if (page_number > page.total_pages) return page;

            page.page_number = page_number;
            size_t start_index = (page.page_number - 1) * page_size;

            roaring_uint32_iterator_t* it = roaring_create_iterator(result_set->bitmap);
            for(size_t i = 0; i < start_index; ++i) {
                 if (it->has_next) roaring_advance_uint32_iterator(it); else break;
            }

            page_kv_pairs.clear();
            page_data_buffer.clear();
            
            size_t count_in_page = 0;
            StaxGraph_t* graph_handle = (StaxGraph_t*)result_set->kv_data; 

            while(it->has_next && count_in_page < page_size) {
                uint32_t id;
                roaring_read_uint32(it, &id);
                
                StaxResultSet temp_obj_rs = staxdb_graph_get_object_properties(graph_handle, id);
                if(temp_obj_rs && temp_obj_rs->kv_data) {
                    StaxKVResultSetData_t* obj_kv_data = (StaxKVResultSetData_t*)temp_obj_rs->kv_data;
                    
                    if (obj_kv_data->kv_pairs.size() > 1) {
                        
                        std::string serialized_obj;
                        for(size_t i = 0; i < obj_kv_data->kv_pairs.size(); ++i) {
                            const auto& pair = obj_kv_data->kv_pairs[i];
                            serialized_obj.append(to_string_view(pair.key));
                            serialized_obj.append(":");
                            serialized_obj.append(to_string_view(pair.value));
                            if (i < obj_kv_data->kv_pairs.size() - 1) {
                            serialized_obj.append("|");
                            }
                        }
                        
                        size_t key_offset = page_data_buffer.size();
                        char id_key_buf[4];
                        to_binary_key_buf(id, id_key_buf, 4);
                        page_data_buffer.insert(page_data_buffer.end(), id_key_buf, id_key_buf + 4);

                        size_t val_offset = page_data_buffer.size();
                        page_data_buffer.insert(page_data_buffer.end(), serialized_obj.begin(), serialized_obj.end());
                        page_kv_pairs.push_back({{reinterpret_cast<const char*>(key_offset), 4}, {reinterpret_cast<const char*>(val_offset), serialized_obj.length()}});
                        count_in_page++;
                    }
                    staxdb_resultset_free(temp_obj_rs);
                }
                
                roaring_advance_uint32_iterator(it);
            }
            roaring_free_iterator(it);
            
            const char* base_ptr = page_data_buffer.data();
            for(auto& pair : page_kv_pairs) {
                pair.key.data = base_ptr + reinterpret_cast<size_t>(pair.key.data);
                if(pair.value.len > 0) {
                     pair.value.data = base_ptr + reinterpret_cast<size_t>(pair.value.data);
                }
            }

            page.results_in_page = static_cast<uint32_t>(page_kv_pairs.size());
            page.results = page_kv_pairs.data();
        }
    } catch (const std::exception& e) {
        set_last_error(e.what());
        page = {}; 
    }
    return page;
}

uint64_t staxdb_resultset_get_total_count(StaxResultSet result_set) {
    clear_last_error();
    try {
        if (!result_set) return 0;
        if (result_set->result_type == KV_RESULT) {
            StaxKVResultSetData_t* kv_data = reinterpret_cast<StaxKVResultSetData_t*>(result_set->kv_data);
            return kv_data ? kv_data->kv_pairs.size() : 0;
        } else if (result_set->result_type == GRAPH_ID_RESULT) {
            return result_set->bitmap ? roaring_bitmap_get_cardinality(result_set->bitmap) : 0;
        }
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
    return 0;
}

void staxdb_resultset_free(StaxResultSet result_set) {
    clear_last_error();
    try {
        if (result_set) {
            if (result_set->result_type == KV_RESULT && result_set->kv_data) {
                delete reinterpret_cast<StaxKVResultSetData_t*>(result_set->kv_data);
            } else if (result_set->result_type == GRAPH_ID_RESULT && result_set->bitmap) {
                roaring_bitmap_free(result_set->bitmap);
            }
            delete result_set;
        }
    } catch (const std::exception& e) {
        set_last_error(e.what());
    }
}


const char* staxdb_get_last_error() {
    return last_error_message.c_str();
}