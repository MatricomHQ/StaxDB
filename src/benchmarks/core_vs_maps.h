
#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <memory>
#include <iostream>
#include <atomic>
#include <thread>
#include <random>
#include <iomanip>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <mutex> 


#if defined(_WIN32)
#include <process.h>
#include <windows.h> 
#else
#include <unistd.h> 
#endif



#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <fstream>
#include <string_view>
#endif


#include "stax_db/db.h"          
#include "stax_core/stax_tree.hpp"
#include "stax_core/node_allocator.hpp"
#include "stax_core/value_store.hpp"

#include "stax_common/constants.h"
#include "benchmarks/throughput_bench.h" 
#include "stax_common/os_file_extensions.h" 
#include "stax_db/statistics.h" 
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 


using TestData = ThroughputBench::TestData;




namespace CoreVsMapsBench {


void run_stax_vs_maps_suite();

struct BenchResults {
    std::string map_name;
    std::chrono::nanoseconds insert_duration;
    double insert_avg_latency_ns;
    double insert_throughput_mbps;
    std::chrono::nanoseconds get_duration;
    double get_avg_latency_ns;
    size_t get_hits;
    size_t get_misses;
    double get_throughput_mbps;
    std::chrono::nanoseconds get_nonexistent_duration;
    double get_nonexistent_avg_latency_ns;
    std::chrono::nanoseconds update_duration;
    double update_avg_latency_ns;
    double update_throughput_mbps;

    size_t size_in_bytes;
    size_t size_in_bytes_phys; 
};





template<typename K, typename V>
inline size_t calculate_unordered_map_size(const std::unordered_map<K, V>& map_instance) {
    size_t total_size = 0;

    
    
    total_size += map_instance.bucket_count() * sizeof(void*);

    
    
    
    const size_t node_size = sizeof(std::pair<const K, V>) + sizeof(void*); 
    total_size += map_instance.size() * node_size;

    
    
    for (const auto& pair : map_instance) {
        total_size += pair.first.capacity();
        total_size += pair.second.capacity();
    }
    return total_size;
}


template<typename K, typename V>
inline size_t calculate_map_size(const std::map<K, V>& map_instance) {
    size_t total_size = 0;
    
    
    
    const size_t node_overhead = sizeof(void*) * 3 + sizeof(int) + sizeof(std::pair<const K, V>);
    total_size += map_instance.size() * node_overhead;

    
    
    for (const auto& pair : map_instance) {
        total_size += pair.first.capacity();
        total_size += pair.second.capacity();
    }
    return total_size;
}


inline void print_final_results(const std::vector<BenchResults>& all_results, size_t num_items, size_t num_threads, const std::string& key_type_name) {
    std::cout << "\n================================================================================================================" << std::endl;
    std::cout << "--- STAXDB VS MAPS BENCHMARK RESULTS (" << key_type_name << " keys, " << num_items << " items, " << num_threads << " thread(s)) ---" << std::endl;
    std::cout << "================================================================================================================" << std::endl;
    std::cout << std::left
              << std::setw(25) << "Map Type"
              << std::setw(18) << "Insert (ms)"
              << std::setw(18) << "Get-Hit (ms)"
              << std::setw(18) << "Get-Miss (ms)"
              << std::setw(18) << "Avg Insert (ns)"
              << std::setw(18) << "Avg Get-Hit (ns)"
              << std::setw(15) << "Size (Log MB)"
              << std::setw(15) << "Size (Phys MB)"
              << std::endl;
    std::cout << std::string(170, '-') << std::endl;

    for (const auto& r : all_results) {
        std::cout << std::left << std::fixed << std::setprecision(3)
                  << std::setw(25) << r.map_name
                  << std::setw(18) << r.insert_duration.count() / 1e6
                  << std::setw(18) << r.get_duration.count() / 1e6
                  << std::setw(18) << r.get_nonexistent_duration.count() / 1e6
                  << std::setw(18) << r.insert_avg_latency_ns
                  << std::setw(18) << r.get_avg_latency_ns
                  << std::setw(15) << std::fixed << std::setprecision(2) << r.size_in_bytes / (1024.0 * 1024.0)
                  << std::setw(15) << std::fixed << std::setprecision(2) << r.size_in_bytes_phys / (1024.0 * 1024.0)
                  << std::endl;
    }
    std::cout << "================================================================================================================" << std::endl;
}


template<typename MapType, typename SizeCalculator>
void run_map_benchmark(BenchResults& results, const std::string& map_name, const std::vector<std::vector<TestData>>& thread_data, size_t num_threads, SizeCalculator size_calc) {
    results.map_name = (num_threads > 1) ? (map_name + " (locked)") : map_name;

    MapType map_instance;
    std::mutex map_mutex;
    size_t total_items = 0;
    for(const auto& td : thread_data) total_items += td.size();

    std::cout << "\n--- Running Benchmark for: " << results.map_name << " (" << num_threads << " thread(s)) ---" << std::endl;

    
    auto start_insert = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> insert_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        insert_threads.emplace_back([&, i]() {
            for (const auto& item : thread_data[i]) {
                if (num_threads > 1) {
                    std::lock_guard<std::mutex> lock(map_mutex);
                    map_instance[item.key] = item.value;
                } else {
                    map_instance[item.key] = item.value;
                }
            }
        });
    }
    for (auto& t : insert_threads) t.join();
    results.insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_insert);
    results.insert_avg_latency_ns = results.insert_duration.count() / static_cast<double>(total_items);
    results.insert_throughput_mbps = 0.0; 

    
    results.size_in_bytes = size_calc(map_instance);
    
    results.size_in_bytes_phys = results.size_in_bytes;

    
    std::atomic<size_t> total_hits = 0;
    auto start_get = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        get_threads.emplace_back([&, i]() {
            for (const auto& item : thread_data[i]) {
                if (num_threads > 1) {
                    std::lock_guard<std::mutex> lock(map_mutex);
                    if (map_instance.count(item.key)) {
                        total_hits.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    if (map_instance.count(item.key)) {
                        total_hits.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (auto& t : get_threads) t.join();
    results.get_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_get);
    results.get_hits = total_hits.load();
    results.get_avg_latency_ns = results.get_duration.count() / static_cast<double>(total_items);
    results.get_throughput_mbps = 0.0; 

    
    auto start_get_nonexistent = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_miss_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        get_miss_threads.emplace_back([&, i]() {
            
            volatile size_t miss_accumulator = 0;
            for (const auto& item : thread_data[i]) {
                if (num_threads > 1) {
                    std::lock_guard<std::mutex> lock(map_mutex);
                    miss_accumulator += (size_t)map_instance.count(item.miss_key); 
                } else {
                    miss_accumulator += (size_t)map_instance.count(item.miss_key); 
                }
            }
        });
    }
    for (auto& t : get_miss_threads) t.join();
    results.get_nonexistent_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_get_nonexistent);
    results.get_nonexistent_avg_latency_ns = results.get_nonexistent_duration.count() / static_cast<double>(total_items);

    std::cout << "  - Insert: " << results.insert_duration.count() / 1e6 << " ms" << std::endl;
    std::cout << "  - Get (Hits): " << results.get_duration.count() / 1e6 << " ms" << std::endl;
}


inline void run_transactional_stax_benchmark(BenchResults& results, const std::string& map_name, std::unique_ptr<::Database>& db, const std::vector<std::vector<TestData>>& thread_data, size_t num_threads) {
    results.map_name = map_name;
    size_t total_items = 0;
    for(const auto& td : thread_data) total_items += td.size();

    std::cout << "\n--- Running DB Benchmark for: " << map_name << " (" << num_threads << " thread(s)) ---" << std::endl;
    
    uint32_t collection_idx = db->get_collection("transactional_bench"); 

    
    auto start_insert = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> insert_threads;
    std::atomic<uint64_t> total_insert_bytes = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        insert_threads.emplace_back([&db, &collection_idx, &total_insert_bytes, &thread_data, thread_idx = i]() { 
            ::Collection& col = db->get_collection_by_idx(collection_idx); 
            for (const auto& item : thread_data[thread_idx]) {
                TxnContext ctx = col.begin_transaction_context(thread_idx, false);
                TransactionBatch batch;
                total_insert_bytes.fetch_add(item.actual_stored_size_bytes, std::memory_order_relaxed);
                col.insert(ctx, batch, item.key, item.value); 
                col.commit(ctx, batch);
            }
        });
    }
    for (auto& t : insert_threads) t.join();
    results.insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_insert);
    results.insert_avg_latency_ns = total_items > 0 ? static_cast<double>(results.insert_duration.count()) / total_items : 0.0;
    double insert_duration_s = results.insert_duration.count() / 1e9;
    results.insert_throughput_mbps = (insert_duration_s > 0) ? (total_insert_bytes.load() / (1024.0 * 1024.0)) / insert_duration_s : 0.0;
    std::cout << "  - Insert: " << results.insert_duration.count() / 1e6 << " ms" << std::endl;

    
    std::atomic<size_t> total_hits = 0;
    std::atomic<uint64_t> total_get_bytes = 0;
    auto start_get = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        get_threads.emplace_back([&db, &collection_idx, &total_hits, &total_get_bytes, &thread_data, thread_idx = i]() { 
            ::Collection& col = db->get_collection_by_idx(collection_idx); 
            for (const auto& item : thread_data[thread_idx]) {
                TxnContext ctx = col.begin_transaction_context(thread_idx, true);
                auto res = col.get(ctx, item.key); 
                if (res) { 
                    total_hits.fetch_add(1, std::memory_order_relaxed);
                    total_get_bytes.fetch_add(item.actual_stored_size_bytes, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& t : get_threads) if(t.joinable()) t.join();
    results.get_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_get);
    results.get_hits = total_hits.load();
    results.get_misses = total_items - results.get_hits;
    results.get_avg_latency_ns = total_items > 0 ? static_cast<double>(results.get_duration.count()) / total_items : 0.0;
    double get_duration_s = results.get_duration.count() / 1e9;
    results.get_throughput_mbps = (get_duration_s > 0) ? (total_get_bytes.load() / (1024.0 * 1024.0)) / get_duration_s : 0.0;
    std::cout << "  - Get (Hits): " << results.get_duration.count() / 1e6 << " ms" << std::endl;

    
    auto start_get_nonexistent = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_nonexistent_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        get_nonexistent_threads.emplace_back([&db, &collection_idx, &thread_data, thread_idx = i]() { 
            ::Collection& col = db->get_collection_by_idx(collection_idx); 
            volatile size_t miss_accumulator = 0;
            for (const auto& item : thread_data[thread_idx]) {
                TxnContext ctx = col.begin_transaction_context(thread_idx, true);
                auto res = col.get(ctx, item.miss_key);
                if (res) miss_accumulator += 1;
            }
        });
    }
    for (auto& t : get_nonexistent_threads) if(t.joinable()) t.join();
    results.get_nonexistent_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_get_nonexistent);
    results.get_nonexistent_avg_latency_ns = total_items > 0 ? static_cast<double>(results.get_nonexistent_duration.count()) / total_items : 0.0;
    std::cout << "  - Get (Misses): " << results.get_nonexistent_duration.count() / 1e6 << " ms" << std::endl;
    
    
    auto collector = db->get_statistics_collector();
    auto summary = collector.get_database_summary_stats(true);
    results.size_in_bytes = summary.total_allocated_disk_bytes;
    results.size_in_bytes_phys = summary.total_resident_memory_bytes;
}



inline void run_stax_vs_maps_suite_for_threads(size_t num_threads, ThroughputBench::KeyType key_type, const std::string& key_type_name) {
    const size_t num_items = 1000000;

    auto test_data_pool = ThroughputBench::generate_throughput_test_data(num_items, 16, 128, key_type);

    std::vector<std::vector<TestData>> thread_data(num_threads);
    if (num_threads > 0) {
        size_t items_per_thread = num_items / num_threads;
        for (size_t i = 0; i < num_threads; ++i) {
            size_t start_idx = i * items_per_thread;
            size_t end_idx = (i == num_threads - 1) ? num_items : start_idx + items_per_thread;
            if (start_idx < end_idx) {
                 thread_data[i].assign(test_data_pool.begin() + start_idx, test_data_pool.begin() + end_idx);
            }
        }
    }

    std::vector<BenchResults> all_results;


    BenchResults unordered_map_results;
    run_map_benchmark<std::unordered_map<std::string, std::string>>(unordered_map_results, "std::unordered_map", thread_data, num_threads, calculate_unordered_map_size<std::string, std::string>);
    all_results.push_back(unordered_map_results);

    BenchResults map_results;
    run_map_benchmark<std::map<std::string, std::string>>(map_results, "std::map", thread_data, num_threads, calculate_map_size<std::string, std::string>);
    all_results.push_back(map_results);
    
    
    std::filesystem::path txn_stax_db_base_dir = "./db_data_txn_stax_bench";
    std::filesystem::path txn_stax_db_dir = txn_stax_db_base_dir / ("txn_stax_db_" + std::to_string(::Tests::get_process_id())); 
    if (std::filesystem::exists(txn_stax_db_base_dir)) {
        std::filesystem::remove_all(txn_stax_db_base_dir);
    }
    std::filesystem::create_directories(txn_stax_db_base_dir);
    std::unique_ptr<::Database> txn_stax_db_instance = ::Database::create_new(txn_stax_db_dir, num_threads);
    BenchResults transactional_stax_results;
    run_transactional_stax_benchmark(transactional_stax_results, "StaxDB (Transactional)", txn_stax_db_instance, thread_data, num_threads);
    all_results.push_back(transactional_stax_results);
    txn_stax_db_instance.reset(); 
    if (std::filesystem::exists(txn_stax_db_base_dir)) {
        std::filesystem::remove_all(txn_stax_db_base_dir);
    }

    print_final_results(all_results, num_items, num_threads, key_type_name);
}


inline void run_stax_vs_maps_suite() {
    std::cout << "\n\n******************************************************************************************" << std::endl;
    std::cout << "                      RUNNING STAXDB VS. MAPS BENCHMARK SUITE" << std::endl;
    std::cout << "******************************************************************************************" << std::endl;

    run_stax_vs_maps_suite_for_threads(1, ThroughputBench::KeyType::SEQUENTIAL, "Sequential");
    run_stax_vs_maps_suite_for_threads(1, ThroughputBench::KeyType::LONG_SEQUENTIAL, "Long Sequential");
    run_stax_vs_maps_suite_for_threads(1, ThroughputBench::KeyType::RANDOM, "Random");

    run_stax_vs_maps_suite_for_threads(MAX_CONCURRENT_THREADS, ThroughputBench::KeyType::SEQUENTIAL, "Sequential");
    run_stax_vs_maps_suite_for_threads(MAX_CONCURRENT_THREADS, ThroughputBench::KeyType::LONG_SEQUENTIAL, "Long Sequential");
    run_stax_vs_maps_suite_for_threads(MAX_CONCURRENT_THREADS, ThroughputBench::KeyType::RANDOM, "Random");
}


} 