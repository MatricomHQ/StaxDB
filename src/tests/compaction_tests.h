//

//
#pragma once

#include <iostream>
#include <exception>
#include <thread>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif
#include <iomanip>
#include <filesystem>
#include <set>      
#include <map>      
#include <string>
#include <vector>
#include <tuple>    
#include <chrono>   
#include <atomic>   
#include <mutex>    
#include <random>   
#include <memory>   


#include "stax_db/db.h"
#include "stax_tx/transaction.h"
#include "stax_tx/db_cursor.hpp" 
#include "stax_db/path_engine.h"
#include "stax_common/constants.h"
#include "stax_db/query.h"
#include "stax_graph/graph_engine.h"
#include "benchmarks/throughput_bench.h"
#include "tests/common_test_utils.h" 

namespace Tests {

void run_hot_compaction_stress_test() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- HOT COMPACTION STRESS TEST (10-Second Mixed Workload) ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    std::atomic<bool> test_passed = true;
    const size_t num_threads = 4;
    const size_t num_initial_keys = 10000;
    const std::chrono::seconds test_duration = std::chrono::seconds(10);

    std::filesystem::path old_gen_dir = "./db_data_compaction_old_gen";
    std::filesystem::path new_gen_dir = "./db_data_compaction_new_gen";

    for(const auto& dir : {old_gen_dir, new_gen_dir}) {
        if (std::filesystem::exists(dir)) std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
    }

    std::map<std::string, std::string> expected_state;
    std::mutex expected_state_mutex;

    
    std::cout << "Phase 1: Populating 'old generation' database with " << num_initial_keys << " keys..." << std::endl;
    {
        auto db = Database::create_new(old_gen_dir, num_threads);
        uint32_t col_idx = db->get_collection("hot_test");
        Collection& col = db->get_collection_by_idx(col_idx);
        
        
        TxnContext ctx = col.begin_transaction_context(0, false);
        TransactionBatch batch;
        for (size_t i = 0; i < num_initial_keys; ++i) {
            std::string key = "key:" + std::to_string(i);
            std::string value = "value:" + std::to_string(i);
            col.insert(ctx, batch, key, value);
            expected_state[key] = value;
        }
        col.commit(ctx, batch);
    }
    std::cout << "  Phase 1: Initial population complete." << std::endl;

    
    std::cout << "\nPhase 2: Starting concurrent workload and compaction for " << test_duration.count() << " seconds..." << std::endl;
    
    auto old_gen_db = Database::open_existing(old_gen_dir, num_threads);
    auto new_gen_db = Database::create_new(new_gen_dir, num_threads);
    uint32_t old_gen_col_idx = old_gen_db->get_collection("hot_test");
    uint32_t new_gen_col_idx = new_gen_db->get_collection("hot_test");
    
    std::atomic<bool> stop_flag = false;
    std::vector<std::thread> worker_threads;

    std::thread compaction_thread([&]() {
        Database::compact(old_gen_dir, num_threads, false);
        std::cout << "  Compaction thread finished." << std::endl;
    });

    for (size_t i = 0; i < num_threads; ++i) {
        worker_threads.emplace_back([&, thread_id = i]() {
            std::mt19937 gen{static_cast<std::mt19937::result_type>(thread_id)};
            std::uniform_int_distribution<size_t> key_dist(0, num_initial_keys * 1.1);
            std::uniform_int_distribution<int> op_dist(0, 99);

            Collection& old_col = old_gen_db->get_collection_by_idx(old_gen_col_idx);
            Collection& new_col = new_gen_db->get_collection_by_idx(new_gen_col_idx);

            while (!stop_flag.load()) {
                size_t key_idx = key_dist(gen);
                std::string key = "key:" + std::to_string(key_idx);
                int op_type = op_dist(gen);

                if (op_type < 70) {
                    
                    TxnContext read_ctx_new = new_col.begin_transaction_context(thread_id, true);
                    auto res = new_col.get(read_ctx_new, key);
                    
                    if (!res) {
                        TxnContext read_ctx_old = old_col.begin_transaction_context(thread_id, true);
                        res = old_col.get(read_ctx_old, key);
                    }

                } else {
                    
                    TxnContext write_ctx = new_col.begin_transaction_context(thread_id, false);
                    TransactionBatch write_batch;
                    std::lock_guard<std::mutex> lock(expected_state_mutex);

                    if (op_type < 90) {
                        if (expected_state.count(key)) {
                             if (op_type % 2 == 0) {
                                std::string value = "updated_val_t" + std::to_string(thread_id);
                                new_col.insert(write_ctx, write_batch, key, value);
                                expected_state[key] = value;
                             } else {
                                new_col.remove(write_ctx, write_batch, key);
                                expected_state.erase(key);
                             }
                        }
                    } else {
                        std::string value = "inserted_val_t" + std::to_string(thread_id);
                        new_col.insert(write_ctx, write_batch, key, value);
                        expected_state[key] = value;
                    }
                    new_col.commit(write_ctx, write_batch);
                }
            }
        });
    }

    std::this_thread::sleep_for(test_duration);
    stop_flag.store(true);

    for (auto& t : worker_threads) t.join();
    compaction_thread.join();
    std::cout << "  Phase 2: Concurrent workload and compaction finished." << std::endl;

    
    std::cout << "\nPhase 3: Merging and verifying final state..." << std::endl;
    
    old_gen_db.reset();
    new_gen_db.reset();

    auto compacted_db = Database::open_existing(old_gen_dir, num_threads);
    auto final_new_db = Database::open_existing(new_gen_dir, num_threads);
    uint32_t compacted_col_idx = compacted_db->get_collection("hot_test");
    uint32_t final_new_col_idx = final_new_db->get_collection("hot_test");
    Collection& compacted_col = compacted_db->get_collection_by_idx(compacted_col_idx);
    Collection& final_new_col = final_new_db->get_collection_by_idx(final_new_col_idx);

    std::map<std::string, std::string> final_db_state;
    {
        
        TxnContext ctx = compacted_col.begin_transaction_context(0, true);
        for (auto cursor = compacted_col.seek_first(ctx); cursor->is_valid(); cursor->next()) {
            final_db_state[std::string(cursor->key())] = std::string(cursor->value());
        }
    }
    {
        
        TxnContext ctx = final_new_col.begin_transaction_context(0, true);
        for (auto cursor = final_new_col.seek_first(ctx); cursor->is_valid(); cursor->next()) {
             final_db_state[std::string(cursor->key())] = std::string(cursor->value());
        }
    }

    size_t errors = 0;
    for (const auto& pair : expected_state) {
        auto it = final_db_state.find(pair.first);
        if (it == final_db_state.end()) {
            errors++;
            std::cerr << "  -> FAIL: Key '" << pair.first << "' exists in expectation but NOT FOUND in final DB state." << std::endl;
        } else if (it->second != pair.second) {
            errors++;
            std::cerr << "  -> FAIL: Key '" << pair.first << "'. Expected: '" << pair.second << "'. Got: '" << it->second << "'." << std::endl;
        }
    }
    for (const auto& pair : final_db_state) {
        if (expected_state.find(pair.first) == expected_state.end()) {
            errors++;
            std::cerr << "  -> FAIL: Key '" << pair.first << "' found in final DB state but should have been deleted." << std::endl;
        }
    }


    if (errors == 0) {
        test_passed = true;
        std::cout << "  Phase 3: Final state verified correctly. All data is consistent." << std::endl;
    } else {
        test_passed = false;
        std::cout << "  Phase 3: FAILED! Found " << errors << " discrepancies in final state." << std::endl;
    }


    if (test_passed) {
        std::cout << "\nHOT COMPACTION STRESS TEST PASSED!" << std::endl;
    } else {
        std::cout << "\nHOT COMPACTION STRESS TEST FAILED!" << std::endl;
    }

    compacted_db.reset();
    final_new_db.reset();
    for(const auto& dir : {old_gen_dir, new_gen_dir}) {
        if (std::filesystem::exists(dir)) std::filesystem::remove_all(dir);
    }
}


} 
//

//