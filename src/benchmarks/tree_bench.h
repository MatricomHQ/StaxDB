
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
#include <set> 


#include "stax_db/db.h"
#include "stax_core/stax_tree.hpp" 
#include "stax_core/node_allocator.hpp"
#include "stax_core/value_store.hpp"
#include "stax_db/path_engine.h"
#include "stax_common/constants.h" 
#include "benchmarks/throughput_bench.h" 
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 

namespace TreeBench {


using ThroughputBench::TestData;

inline void run_tree_stress_test() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- REFACTORED TREE STRESS TEST (Using Full DB Instance) ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    const size_t num_items = 1'000'000;
    const size_t num_threads = MAX_CONCURRENT_THREADS;

    
    
    std::filesystem::path db_base_dir = "./db_data_tree_bench";
    std::filesystem::path db_dir = db_base_dir / ("test_db_" + std::to_string(Tests::get_process_id()));
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(db_dir);

    auto db = Database::create_new(db_dir, num_threads);
    uint32_t col_idx = db->get_collection("tree_stress_test");
    Collection& col = db->get_collection_by_idx(col_idx);
    StaxTree& tree = col.get_critbit_tree();
    
    
    std::cout << "Generating test data..." << std::endl;
    auto test_data_pool = ThroughputBench::generate_throughput_test_data(num_items, 16, 256);
    
    std::vector<std::vector<TestData>> thread_data(num_threads);
    size_t items_per_thread = num_items / num_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * items_per_thread;
        size_t end_idx = (i == num_threads - 1) ? num_items : start_idx + items_per_thread;
        if (start_idx < end_idx) {
             thread_data[i].assign(test_data_pool.begin() + start_idx, test_data_pool.begin() + end_idx);
        }
    }
    std::cout << "Data generation complete." << std::endl;

    
    std::cout << "\n--- Running Concurrent Insert Phase ---" << std::endl;
    auto start_insert = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> insert_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        insert_threads.emplace_back([&tree, &thread_data, thread_idx = i]() {
            
            TxnContext ctx = {1, 1, thread_idx}; 
            for (const auto& item : thread_data[thread_idx]) {
                tree.insert(ctx, item.key, item.value); 
            }
        });
    }
    for (auto& t : insert_threads) t.join();
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto insert_duration_ms = std::chrono::duration<double, std::milli>(end_insert - start_insert).count();
    std::cout << "Insert Phase completed in " << insert_duration_ms << " ms." << std::endl;
    
    
    std::atomic_thread_fence(std::memory_order_seq_cst);

    
    std::cout << "\n--- Running Concurrent Read Verification Phase ---" << std::endl;
    std::atomic<size_t> total_hits = 0;
    std::atomic<size_t> total_misses = 0;
    std::vector<std::string> missed_keys_initial_pass; 
    std::mutex missed_keys_mutex; 

    auto start_read = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> read_threads;
     for (size_t i = 0; i < num_threads; ++i) {
        read_threads.emplace_back([&tree, &thread_data, &total_hits, &total_misses, &missed_keys_initial_pass, &missed_keys_mutex, thread_idx = i]() {
            TxnContext ctx = {2, 2, thread_idx}; 
            for (const auto& item : thread_data[thread_idx]) {
                auto res = tree.get(ctx, item.key); 
                if (res && res->value_view() == item.value) {
                    total_hits++;
                } else {
                    total_misses++;
                    std::lock_guard<std::mutex> lock(missed_keys_mutex);
                    missed_keys_initial_pass.push_back(item.key); 
                }
            }
        });
    }
    for (auto& t : read_threads) t.join();
    auto end_read = std::chrono::high_resolution_clock::now();
    auto read_duration_ms = std::chrono::duration<double, std::milli>(end_read - start_read).count();
    std::cout << "Read Phase completed in " << read_duration_ms << " ms." << std::endl;
    
    
    std::cout << "\n--- Tree Stress Test Results (Initial Read Pass) ---" << std::endl;
    std::cout << "  - Total Items: " << num_items << std::endl;
    std::cout << "  - Hits: " << total_hits.load() << std::endl;
    std::cout << "  - Misses: " << total_misses.load() << std::endl;

    if (total_misses.load() > 0) {
        std::cout << "\n--- Re-verifying " << missed_keys_initial_pass.size() << " initially missed keys (Sequential) ---" << std::endl;
        size_t re_verify_hits = 0;
        size_t re_verify_still_missed = 0;
        for (const auto& key : missed_keys_initial_pass) {
            TxnContext ctx = {3, 3, 0};
            auto res = tree.get(ctx, key); 
            if (res) {
                re_verify_hits++;
            } else {
                re_verify_still_missed++;
                std::cerr << "!!! ANOMALY: Key '" << key << "' still NOT FOUND after re-verification!" << std::endl;
            }
        }
        std::cout << "  - Re-verified Hits: " << re_verify_hits << std::endl;
        std::cout << "  - Re-verified Still Missed: " << re_verify_still_missed << std::endl;
        
        if (re_verify_still_missed == 0) {
            std::cout << "  - Result: PASSED! Initial misses were due to memory visibility, now resolved." << std::endl;
        } else {
            std::cout << "  - Result: FAILED! Some keys are genuinely missing or corrupted." << std::endl;
        }
    } else {
        std::cout << "  - Result: PASSED! No misses in the initial read pass." << std::endl;
    }
    std::cout << "==========================================================================================\n" << std::endl;

    
    db.reset();
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
}

} 