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
#include <filesystem> 
#include <set>      
#include <cassert>  

#if defined(_WIN32)
#include <process.h> 
#else
#include <unistd.h> 
#endif



#include "stax_db/db.h"
#include "stax_graph/graph_engine.h" 
#include "stax_common/constants.h" 
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 

namespace GraphBench {


struct BenchResultsRow {
    std::string name;
    long long total_duration_ns;
    long long avg_latency_ns;
    size_t items_processed;
    std::string extra_info;
};

inline void print_graph_results_table(const std::vector<BenchResultsRow>& results_table) {
    std::cout << "\n--- Graph Benchmark Results (StaxDB) ---" << std::endl;
    std::cout << std::left 
              << std::setw(25) << "Benchmark Name"
              << std::setw(20) << "Total Time (ms)"
              << std::setw(20) << "Avg Latency (ns)"
              << std::setw(20) << "Items Processed"
              << std::setw(30) << "Extra Info"
              << std::endl;
    std::cout << std::string(115, '-') << std::endl;
    for (const auto& row : results_table) {
        std::cout << std::left << std::fixed << std::setprecision(3)
                  << std::setw(25) << row.name
                  << std::setw(20) << row.total_duration_ns / 1e6
                  << std::setw(20) << row.avg_latency_ns
                  << std::setw(20) << row.items_processed
                  << std::setw(30) << row.extra_info
                  << std::endl;
    }
}


struct NodeData {
    std::string type;
    std::string name;
    std::string email;
    uint32_t id; 
};

struct PropertyData {
    uint32_t obj_id;
    std::string field_name;
    std::string value_literal; 
    uint64_t numeric_value; 
    bool is_numeric; 
};

struct EdgeData {
    uint32_t source_id;
    std::string rel_type_name;
    uint32_t target_id;
};


inline std::vector<NodeData> generate_nodes(size_t num_nodes) {
    std::vector<NodeData> nodes;
    nodes.reserve(num_nodes);
    for (size_t i = 0; i < num_nodes; ++i) {
        NodeData node;
        node.name = "User_" + std::to_string(i);
        node.email = "user" + std::to_string(i) + "@example.com";
        node.type = "User";
        node.id = global_id_map.get_or_create_id(node.name); 
        nodes.push_back(node);
    }
    return nodes;
}

inline std::vector<PropertyData> generate_properties(const std::vector<NodeData>& nodes, std::mt19937& gen) {
    std::vector<PropertyData> properties;
    properties.reserve(nodes.size() * 3); 
    
    uint32_t type_field_id = global_id_map.get_or_create_id("type");
    uint32_t name_field_id = global_id_map.get_or_create_id("name");
    uint32_t email_field_id = global_id_map.get_or_create_id("email");
    uint32_t city_field_id = global_id_map.get_or_create_id("city");
    uint32_t age_field_id = global_id_map.get_or_create_id("age"); 
    uint32_t null_email_id = global_id_map.get_or_create_id("__NULL_EMAIL__"); 

    std::vector<std::string> cities = {"New York", "London", "Paris", "Tokyo", "Berlin", "Sydney", "Rome"};
    std::uniform_int_distribution<> city_dist(0, cities.size() - 1);
    std::uniform_int_distribution<> age_dist(18, 99); 
    
    for (size_t i = 0; i < nodes.size(); ++i) { 
        const auto& node = nodes[i];
        properties.push_back({node.id, "type", node.type, 0, false}); 
        properties.push_back({node.id, "name", node.name, 0, false});
        
        
        if (i % 10 != 0) { 
            properties.push_back({node.id, "email", node.email, 0, false});
        } else {
            
            properties.push_back({node.id, "email", "__NULL_EMAIL__", 0, false});
        }
        
        properties.push_back({node.id, "city", cities[city_dist(gen)], 0, false}); 
        
        uint64_t age_val = age_dist(gen);
        properties.push_back({node.id, "age", "", age_val, true}); 
    }
    return properties;
}


inline std::vector<EdgeData> generate_edges(const std::vector<NodeData>& nodes, size_t avg_follows_per_node, size_t avg_likes_per_node, std::mt19937& gen) {
    std::vector<EdgeData> edges;
    edges.reserve(nodes.size() * (avg_follows_per_node + avg_likes_per_node)); 

    std::uniform_int_distribution<size_t> node_dist(0, nodes.size() - 1);

    
    uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
    uint32_t likes_field_id = global_id_map.get_or_create_id("LIKES");
    
    for (const auto& node : nodes) {
        
        for (size_t i = 0; i < avg_follows_per_node; ++i) {
            uint32_t target_id = nodes[node_dist(gen)].id;
            if (target_id != node.id) { 
                edges.push_back({node.id, "FOLLOWS", target_id});
            }
        }
        
        for (size_t i = 0; i < avg_likes_per_node; ++i) {
            uint32_t target_id = nodes[node_dist(gen)].id;
            if (target_id != node.id) { 
                edges.push_back({node.id, "LIKES", target_id});
            }
        }
    }
    return edges;
}



inline void run_graph_benchmark() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- STAXDB GRAPH ENGINE BENCHMARK ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    const size_t NUM_NODES = 1000;       
    const size_t NUM_PROPERTIES_PER_NODE = 5; 
    const size_t AVG_FOLLOWS_PER_NODE = 5; 
    const size_t AVG_LIKES_PER_NODE = 10; 
    const size_t NUM_QUERY_OPS = 100000;    
    const unsigned int BENCH_SEED = 42; 

    std::filesystem::path db_base_dir = "./db_data_graph_bench";
    std::filesystem::path db_dir = db_base_dir / ("graph_db_" + std::to_string(Tests::get_process_id()));
    
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(db_base_dir);

    
    auto db = ::Database::create_new(db_dir, BENCHMARK_NUM_THREADS);

    std::vector<BenchResultsRow> results_table;
    
    
    std::cout << "Phase 1: Generating graph data..." << std::endl;
    auto start_data_gen = std::chrono::high_resolution_clock::now();
    std::mt19937 master_gen(BENCH_SEED);
    std::vector<NodeData> nodes = generate_nodes(NUM_NODES);
    std::vector<PropertyData> properties = generate_properties(nodes, master_gen);
    std::vector<EdgeData> edges = generate_edges(nodes, AVG_FOLLOWS_PER_NODE, AVG_LIKES_PER_NODE, master_gen); 
    auto end_data_gen = std::chrono::high_resolution_clock::now();
    std::cout << "  Data generation complete in " << std::chrono::duration_cast<std::chrono::milliseconds>(end_data_gen - start_data_gen).count() << " ms." << std::endl;
    std::cout << "    - Nodes: " << nodes.size() << std::endl;
    std::cout << "    - Properties: " << properties.size() << std::endl;
    std::cout << "    - Edges: " << edges.size() << std::endl;
    std::cout << "    - Total unique IDs generated: " << global_id_map.get_total_ids_generated() << " (rough estimate)" << std::endl; 

    
    std::cout << "\nPhase 2: Ingesting graph data into StaxDB..." << std::endl;
    auto start_ingestion = std::chrono::high_resolution_clock::now();
    std::atomic<size_t> total_facts_ingested = 0;
    std::vector<std::thread> ingest_threads;

    for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
        ingest_threads.emplace_back([&, t]() {
            GraphTransaction txn(db.get(), t); 
            
            
            for (size_t i = t; i < properties.size(); i += BENCHMARK_NUM_THREADS) {
                const auto& prop = properties[i];
                uint32_t field_id = global_id_map.get_or_create_id(prop.field_name);
                if (prop.is_numeric) {
                    txn.insert_fact_numeric(prop.obj_id, field_id, prop.field_name, prop.numeric_value);
                } else {
                    
                    txn.insert_fact_string(prop.obj_id, field_id, prop.field_name, prop.value_literal);
                }
                total_facts_ingested++;
            }

            
            for (size_t i = t; i < edges.size(); i += BENCHMARK_NUM_THREADS) {
                txn.insert_fact(edges[i].source_id, global_id_map.get_or_create_id(edges[i].rel_type_name), edges[i].target_id);
                total_facts_ingested++;
            }

            txn.commit();
        });
    }

    for (auto& t : ingest_threads) t.join();
    auto end_ingestion = std::chrono::high_resolution_clock::now();
    long long ingestion_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ingestion - start_ingestion).count();
    
    results_table.push_back({
        "Ingestion (per Object)",
        ingestion_duration_ns,
        static_cast<long long>(ingestion_duration_ns / NUM_NODES),
        NUM_NODES,
        "Facts: " + std::to_string(total_facts_ingested.load())
    });
    std::cout << "  Ingestion complete." << std::endl;

    
    std::cout << "\nPhase 2.5: Injecting deterministic test relationships..." << std::endl;
    {
        uint32_t user100_id = global_id_map.get_or_create_id("User_100");
        uint32_t user200_id = global_id_map.get_or_create_id("User_200");
        uint32_t user500_id = global_id_map.get_or_create_id("User_500");
        uint32_t user101_id = global_id_map.get_or_create_id("User_101");
        uint32_t user102_id = global_id_map.get_or_create_id("User_102");
        uint32_t user103_id = global_id_map.get_or_create_id("User_103");
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");

        GraphTransaction setup_txn(db.get(), 0); 
        
        setup_txn.insert_fact(user100_id, follows_field_id, user500_id);
        setup_txn.insert_fact(user200_id, follows_field_id, user500_id);
        
        
        setup_txn.insert_fact(user101_id, follows_field_id, user102_id);
        setup_txn.insert_fact(user102_id, follows_field_id, user101_id);

        
        setup_txn.insert_fact(user100_id, follows_field_id, user102_id);
        setup_txn.insert_fact(user102_id, follows_field_id, user103_id);
        setup_txn.insert_fact(user103_id, follows_field_id, user100_id);

        setup_txn.commit();
        std::cout << "  Deterministic relationships injected." << std::endl;
    }


    
    std::cout << "\nPhase 3: Running graph queries..." << std::endl;
    
    
    TxnContext read_ctx = db->begin_transaction_context(0, true);

    std::mt19937 query_gen(BENCH_SEED); 
    std::uniform_int_distribution<size_t> node_idx_dist(0, nodes.size() - 1);
    
    std::vector<BenchResultsRow> query_results;

    
    
    
    {
        std::atomic<size_t> total_props_retrieved = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                size_t local_props = 0;
                for (size_t i = 0; i < NUM_QUERY_OPS / BENCHMARK_NUM_THREADS; ++i) {
                    uint32_t query_node_id = nodes[node_idx_dist(query_gen)].id;
                    local_props += reader.get_properties_and_relationships(query_node_id).size();
                }
                total_props_retrieved += local_props;
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        query_results.push_back({
            "Get Node Props",
            duration,
            static_cast<long long>(duration / NUM_QUERY_OPS),
            NUM_QUERY_OPS,
            "Total props: " + std::to_string(total_props_retrieved.load())
        });
    }

    
    
    
    {
        uint32_t email_field_id = global_id_map.get_or_create_id("email");
        std::atomic<size_t> total_emails_found = 0;
        
        
        std::vector<uint32_t> query_node_ids(NUM_QUERY_OPS);
        for(size_t i = 0; i < NUM_QUERY_OPS; ++i) {
            query_node_ids[i] = nodes[node_idx_dist(query_gen)].id;
        }

        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                size_t local_emails = 0;
                size_t ops_per_thread = NUM_QUERY_OPS / BENCHMARK_NUM_THREADS;
                size_t start_idx = t * ops_per_thread;
                size_t end_idx = start_idx + ops_per_thread;

                for (size_t i = start_idx; i < end_idx; ++i) {
                    if(reader.get_property_for_object_string(query_node_ids[i], email_field_id)) {
                        local_emails++;
                    }
                }
                total_emails_found += local_emails;
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        query_results.push_back({
            "Point Lookup (email)",
            duration,
            static_cast<long long>(duration / NUM_QUERY_OPS),
            NUM_QUERY_OPS,
            "Emails found: " + std::to_string(total_emails_found.load())
        });
    }

    
    
    
    {
        uint32_t city_field_id = global_id_map.get_or_create_id("city");
        uint32_t london_id = global_id_map.get_or_create_id("London");
        std::atomic<size_t> total_nodes_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                roaring_bitmap_t* london_users_bm = roaring_bitmap_create();
                reader.get_objects_by_property_into_roaring(city_field_id, london_id, london_users_bm);
                total_nodes_found += roaring_bitmap_get_cardinality(london_users_bm);
                roaring_bitmap_free(london_users_bm);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_nodes_found.load() > 0 ? total_nodes_found.load() : 1;
        query_results.push_back({
            "Nodes By City='London'",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_nodes_found.load(),
            "Total nodes found: " + std::to_string(total_nodes_found.load())
        });
    }
    
    
    
    {
        uint32_t age_field_id = global_id_map.get_or_create_id("age");
        auto start = std::chrono::high_resolution_clock::now();
        GraphReader reader(db.get(), read_ctx); 
        roaring_bitmap_t* results_bm = roaring_bitmap_create();
        reader.get_objects_by_property_range_into_roaring(age_field_id, 25, 35, results_bm);
        size_t count = roaring_bitmap_get_cardinality(results_bm);
        roaring_bitmap_free(results_bm);
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = count > 0 ? count : 1;
        query_results.push_back({ "Range (age 25-35)", duration, static_cast<long long>(duration / items_processed), count, "Nodes: " + std::to_string(count) });
    }
    
    {
        uint32_t age_field_id = global_id_map.get_or_create_id("age");
        auto start = std::chrono::high_resolution_clock::now();
        GraphReader reader(db.get(), read_ctx); 
        roaring_bitmap_t* results_bm = roaring_bitmap_create();
        reader.get_objects_by_property_range_into_roaring(age_field_id, 0, 29, results_bm);
        size_t count = roaring_bitmap_get_cardinality(results_bm);
        roaring_bitmap_free(results_bm);
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = count > 0 ? count : 1;
        query_results.push_back({ "Range (age < 30)", duration, static_cast<long long>(duration / items_processed), count, "Nodes: " + std::to_string(count) });
    }

    {
        uint32_t age_field_id = global_id_map.get_or_create_id("age");
        auto start = std::chrono::high_resolution_clock::now();
        GraphReader reader(db.get(), read_ctx); 
        roaring_bitmap_t* results_bm = roaring_bitmap_create();
        reader.get_objects_by_property_range_into_roaring(age_field_id, 75, 99, results_bm);
        size_t count = roaring_bitmap_get_cardinality(results_bm);
        roaring_bitmap_free(results_bm);
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = count > 0 ? count : 1;
        query_results.push_back({ "Range (age > 75)", duration, static_cast<long long>(duration / items_processed), count, "Nodes: " + std::to_string(count) });
    }

    
    
    
    {
        uint32_t age_field_id = global_id_map.get_or_create_id("age");
        
        uint32_t city_field_id = global_id_map.get_or_create_id("city");
        uint32_t london_id = global_id_map.get_or_create_id("London");

        std::atomic<size_t> total_nodes_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                roaring_bitmap_t* users_age_30_bm = roaring_bitmap_create();
                
                reader.get_objects_by_property_range_into_roaring(age_field_id, 30, 30, users_age_30_bm);
                
                roaring_bitmap_t* users_london_bm = roaring_bitmap_create();
                reader.get_objects_by_property_into_roaring(city_field_id, london_id, users_london_bm);

                roaring_bitmap_and_inplace(users_age_30_bm, users_london_bm); 
                
                total_nodes_found += roaring_bitmap_get_cardinality(users_age_30_bm);
                roaring_bitmap_free(users_age_30_bm);
                roaring_bitmap_free(users_london_bm);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_nodes_found.load() > 0 ? total_nodes_found.load() : 1;
        query_results.push_back({
            "Filter (Age30 & London)", 
            duration,
            static_cast<long long>(duration / items_processed), 
            total_nodes_found.load(),
            "Total nodes: " + std::to_string(total_nodes_found.load())
        });
    }
    
    
    
    
    {
        uint32_t age_field_id = global_id_map.get_or_create_id("age");
        uint32_t age_30_id = global_id_map.get_or_create_id("30"); 
        uint32_t city_field_id = global_id_map.get_or_create_id("city");
        uint32_t london_id = global_id_map.get_or_create_id("London");

        std::atomic<size_t> total_nodes_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                roaring_bitmap_t* users_age_30_bm = roaring_bitmap_create();
                
                
                reader.get_objects_by_property_into_roaring(age_field_id, age_30_id, users_age_30_bm);
                
                roaring_bitmap_t* users_london_bm = roaring_bitmap_create();
                reader.get_objects_by_property_into_roaring(city_field_id, london_id, users_london_bm);

                roaring_bitmap_or_inplace(users_age_30_bm, users_london_bm); 
                
                total_nodes_found += roaring_bitmap_get_cardinality(users_age_30_bm);
                roaring_bitmap_free(users_age_30_bm);
                roaring_bitmap_free(users_london_bm);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_nodes_found.load() > 0 ? total_nodes_found.load() : 1;
        query_results.push_back({
            "Filter (Age30 | London)", 
            duration,
            static_cast<long long>(duration / items_processed), 
            total_nodes_found.load(),
            "Total nodes: " + std::to_string(total_nodes_found.load())
        });
    }

    
    
    
    {
        uint32_t email_field_id = global_id_map.get_or_create_id("email");
        uint32_t null_email_id = global_id_map.get_or_create_id("__NULL_EMAIL__");

        std::atomic<size_t> total_nodes_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                total_nodes_found += reader.count_objects_by_property(email_field_id, null_email_id);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_nodes_found.load() > 0 ? total_nodes_found.load() : 1;
        query_results.push_back({
            "Nodes without Email",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_nodes_found.load(),
            "Total nodes: " + std::to_string(total_nodes_found.load())
        });
    }


    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        std::atomic<size_t> total_followers_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                size_t local_followers = 0;
                for (size_t i = 0; i < NUM_QUERY_OPS / BENCHMARK_NUM_THREADS; ++i) {
                    uint32_t query_node_id = nodes[node_idx_dist(query_gen)].id;
                    roaring_bitmap_t* followers_bm = roaring_bitmap_create();
                    reader.get_outgoing_relationships_into_roaring(query_node_id, follows_field_id, followers_bm);
                    local_followers += roaring_bitmap_get_cardinality(followers_bm);
                    roaring_bitmap_free(followers_bm);
                }
                total_followers_found += local_followers;
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        query_results.push_back({
            "1-Hop Out (FOLLOWS)",
            duration,
            static_cast<long long>(duration / NUM_QUERY_OPS),
            NUM_QUERY_OPS,
            "Total followers: " + std::to_string(total_followers_found.load())
        });
    }
    
    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        std::atomic<size_t> total_incoming_followers = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                size_t local_incoming = 0;
                for (size_t i = 0; i < NUM_QUERY_OPS / BENCHMARK_NUM_THREADS; ++i) {
                    uint32_t query_node_id = nodes[node_idx_dist(query_gen)].id;
                    local_incoming += reader.get_incoming_relationships(query_node_id, follows_field_id).size();
                }
                total_incoming_followers += local_incoming;
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        query_results.push_back({
            "1-Hop In (FOLLOWS)",
            duration,
            static_cast<long long>(duration / NUM_QUERY_OPS),
            NUM_QUERY_OPS,
            "Total incoming: " + std::to_string(total_incoming_followers.load())
        });
    }

    
    
    
    
    {
        uint32_t city_field_id = global_id_map.get_or_create_id("city");
        uint32_t london_id = global_id_map.get_or_create_id("London");
        uint32_t paris_id = global_id_map.get_or_create_id("Paris");
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");

        std::atomic<size_t> total_matches_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                
                roaring_bitmap_t* london_users_bm = roaring_bitmap_create();
                reader.get_objects_by_property_into_roaring(city_field_id, london_id, london_users_bm);
                
                roaring_bitmap_t* paris_users_bm = roaring_bitmap_create();
                reader.get_objects_by_property_into_roaring(city_field_id, paris_id, paris_users_bm);
                
                roaring_bitmap_t* london_follows = roaring_bitmap_create();
                reader.get_outgoing_relationships_for_many_into_roaring(london_users_bm, follows_field_id, london_follows);

                roaring_bitmap_and_inplace(london_follows, paris_users_bm);
                
                total_matches_found += roaring_bitmap_get_cardinality(london_follows);
                roaring_bitmap_free(london_users_bm);
                roaring_bitmap_free(paris_users_bm);
                roaring_bitmap_free(london_follows);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_matches_found.load() > 0 ? total_matches_found.load() : 1;
        query_results.push_back({
            "London->FOLLOWS->Paris",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_matches_found.load(),
            "Total matches: " + std::to_string(total_matches_found.load())
        });
    }
    
    
    
    
    {
        uint32_t type_field_id = global_id_map.get_or_create_id("type");
        uint32_t user_type_id = global_id_map.get_or_create_id("User");
        std::atomic<size_t> total_users_count = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                total_users_count += reader.count_objects_by_property(type_field_id, user_type_id);
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_users_count.load() > 0 ? total_users_count.load() : 1;
        query_results.push_back({
            "Count Users",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_users_count.load(),
            "Total users: " + std::to_string(total_users_count.load())
        });
    }

    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        std::atomic<size_t> total_follows_count = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                total_follows_count += reader.count_relationships_by_type(follows_field_id); 
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_follows_count.load() > 0 ? total_follows_count.load() : 1;
        query_results.push_back({
            "Count FOLLOWS Rels",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_follows_count.load(),
            "Total FOLLOWS: " + std::to_string(total_follows_count.load())
        });
    }

    
    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        uint32_t test_node1_id = global_id_map.get_or_create_id("User_100"); 
        uint32_t test_node2_id = global_id_map.get_or_create_id("User_200");
        
        std::atomic<size_t> total_common_neighbors_found = 0;
        auto start = std::chrono::high_resolution_clock::now();
        
        GraphReader reader(db.get(), read_ctx); 
        auto pipeline = reader.get_common_neighbors(test_node1_id, test_node2_id, follows_field_id);
        uint32_t id;
        while(pipeline->next(id)) {
            total_common_neighbors_found++;
        }

        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = total_common_neighbors_found.load() > 0 ? total_common_neighbors_found.load() : 1;
        query_results.push_back({
            "Common Neighbors (AND)",
            duration,
            static_cast<long long>(duration / items_processed), 
            total_common_neighbors_found.load(),
            "Total common: " + std::to_string(total_common_neighbors_found.load())
        });
    }

    
    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        
        uint32_t test_node1_id = global_id_map.get_or_create_id("User_101"); 
        uint32_t test_node2_id = global_id_map.get_or_create_id("User_102"); 

        std::atomic<size_t> total_mutual_pairs = 0;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> query_threads;
        for (size_t t = 0; t < BENCHMARK_NUM_THREADS; ++t) {
            query_threads.emplace_back([&, t]() {
                GraphReader reader(db.get(), read_ctx); 
                
                for(size_t i = 0; i < NUM_QUERY_OPS / BENCHMARK_NUM_THREADS; ++i) {
                    if (reader.has_relationship(test_node1_id, follows_field_id, test_node2_id) &&
                        reader.has_relationship(test_node2_id, follows_field_id, test_node1_id)) {
                        total_mutual_pairs++;
                    }
                }
            });
        }
        for (auto& t : query_threads) t.join();
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        query_results.push_back({
            "Mutual Rels (A<->B)",
            duration,
            static_cast<long long>(duration / NUM_QUERY_OPS),
            NUM_QUERY_OPS,
            "Total mutual pairs: " + std::to_string(total_mutual_pairs.load())
        });
    }

    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        uint32_t start_node = global_id_map.get_or_create_id("User_100");
        uint32_t end_node = global_id_map.get_or_create_id("User_500");
        
        auto start = std::chrono::high_resolution_clock::now();
        GraphReader reader(db.get(), read_ctx); 
        std::vector<uint32_t> path = reader.find_shortest_path(start_node, end_node, follows_field_id);
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = path.size() > 0 ? path.size() : 1;
        query_results.push_back({
            "Shortest Path (BFS)",
            duration,
            static_cast<long long>(duration / items_processed),
            path.size(),
            "Path len: " + std::to_string(path.size())
        });
    }

    
    
    
    {
        uint32_t follows_field_id = global_id_map.get_or_create_id("FOLLOWS");
        auto start = std::chrono::high_resolution_clock::now();
        GraphReader reader(db.get(), read_ctx); 
        uint64_t triangle_count = reader.count_triangles(follows_field_id);
        auto end = std::chrono::high_resolution_clock::now();
        long long duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        size_t items_processed = triangle_count > 0 ? triangle_count : 1;
        query_results.push_back({
            "Triangle Counting",
            duration,
            static_cast<long long>(duration / items_processed),
            triangle_count,
            "Triangles: " + std::to_string(triangle_count)
        });
    }

    print_graph_results_table(results_table);
    print_graph_results_table(query_results); 

    
    db.reset(); 
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::cout << "\nStaxDB Graph Engine Benchmark Finished!" << std::endl;
}

}