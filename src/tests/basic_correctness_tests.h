
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
#include <random>   


#include "stax_db/db.h"
#include "stax_db/path_engine.h"
#include "stax_common/constants.h" 
#include "stax_db/query.h"
#include "stax_graph/graph_engine.h"
#include "benchmarks/throughput_bench.h"
#include "tests/common_test_utils.h" 
#include "stax_db/statistics.h" 
#include "stax_tx/transaction.h" 

namespace Tests { 


void run_durability_test() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- DURABILITY TEST (Multi-Collection) ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    std::atomic<bool> test_passed = true;
    std::filesystem::path db_base_dir = "./db_data_durability";
    std::filesystem::path db_dir = db_base_dir / ("test_db_" + std::to_string(::Tests::get_process_id()));
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    
    {
        std::cout << "Phase 1: Creating DB, inserting into two collections, and closing..." << std::endl;
        auto db = Database::create_new(db_dir, 1); 
        
        uint32_t users_collection_idx = db->get_collection("users");
        uint32_t orders_collection_idx = db->get_collection("orders");

        Collection& users_col = db->get_collection_by_idx(users_collection_idx);
        TxnContext users_ctx = users_col.begin_transaction_context(0, false); 
        TransactionBatch users_batch;
        users_col.insert(users_ctx, users_batch, "user:1", "kris");
        users_col.commit(users_ctx, users_batch);

        Collection& orders_col = db->get_collection_by_idx(orders_collection_idx);
        TxnContext orders_ctx = orders_col.begin_transaction_context(0, false); 
        TransactionBatch orders_batch;
        orders_col.insert(orders_ctx, orders_batch, "order:101", "product_a");
        orders_col.commit(orders_ctx, orders_batch);

        db.reset();
        std::cout << "Phase 1: Data inserted and DB closed. (Implies durable write)" << std::endl;
    }

    {
        std::cout << "Phase 2: Re-opening DB and verifying initial data..." << std::endl;
        auto db = Database::open_existing(db_dir, 1);
        
        uint32_t users_collection_idx = db->get_collection("users");
        uint32_t orders_collection_idx = db->get_collection("orders");

        Collection& users_col_read = db->get_collection_by_idx(users_collection_idx);
        TxnContext users_read_ctx = users_col_read.begin_transaction_context(0, true); 
        auto user_res = users_col_read.get(users_read_ctx, "user:1");
        if (!user_res.has_value() || user_res->value_view() != "kris") {
            std::cerr << "FAIL: Durability Phase 2 - user:1 value mismatch. Expected 'kris', got '" << (user_res ? std::string(user_res->value_view()) : "NOT_FOUND") << "'." << std::endl;
            test_passed = false;
        }
        if (users_col_read.get(users_read_ctx, "order:101").has_value()) {
            std::cerr << "FAIL: Durability Phase 2 - order:101 found in users collection." << std::endl;
            test_passed = false;
        }
        

        Collection& orders_col_read = db->get_collection_by_idx(orders_collection_idx);
        TxnContext orders_read_ctx = orders_col_read.begin_transaction_context(0, true); 
        auto order_res = orders_col_read.get(orders_read_ctx, "order:101");
        if (!order_res.has_value() || order_res->value_view() != "product_a") { 
            std::cerr << "FAIL: Durability Phase 2 - order:101 value mismatch. Expected 'product_a', got '" << (order_res ? std::string(order_res->value_view()) : "NOT_FOUND") << "'." << std::endl;
            test_passed = false;
        }
        if (orders_col_read.get(orders_read_ctx, "user:1").has_value()) {
            std::cerr << "FAIL: Durability Phase 2 - user:1 found in orders collection." << std::endl;
            test_passed = false;
        }
        

        db.reset();
        std::cout << "Phase 2: Initial data verified. DB closed." << std::endl;
    }

    {
        std::cout << "Phase 3: Re-opening, updating one collection, closing..." << std::endl;
        auto db = Database::open_existing(db_dir, 1);

        uint32_t users_collection_idx = db->get_collection("users");

        Collection& users_col_update = db->get_collection_by_idx(users_collection_idx);
        TxnContext users_update_ctx = users_col_update.begin_transaction_context(0, false); 
        TransactionBatch users_update_batch;
        users_col_update.insert(users_update_ctx, users_update_batch, "user:1", "kris_updated");
        users_col_update.commit(users_update_ctx, users_update_batch);

        db.reset();
        std::cout << "Phase 3: Update performed. DB closed." << std::endl;
    }

    {
        std::cout << "Phase 4: Re-opening and verifying final state..." << std::endl;
        auto db = Database::open_existing(db_dir, 1);
        
        uint32_t users_collection_idx = db->get_collection("users");
        uint32_t orders_collection_idx = db->get_collection("orders");

        Collection& users_col_final = db->get_collection_by_idx(users_collection_idx);
        TxnContext users_final_ctx = users_col_final.begin_transaction_context(0, true); 
        auto user_res = users_col_final.get(users_final_ctx, "user:1");
        if (!user_res.has_value() || user_res->value_view() != "kris_updated") {
            std::cerr << "FAIL: Durability Phase 4 - user:1 value mismatch. Expected 'kris_updated', got '" << (user_res ? std::string(user_res->value_view()) : "NOT_FOUND") << "'." << std::endl;
            test_passed = false;
        }
        

        Collection& orders_col_final = db->get_collection_by_idx(orders_collection_idx);
        TxnContext orders_final_ctx = orders_col_final.begin_transaction_context(0, true); 
        auto order_res = orders_col_final.get(orders_final_ctx, "order:101");
        if (!order_res.has_value() || order_res->value_view() != "product_a") { 
            std::cerr << "FAIL: Durability Phase 4 - order:101 not found or value mismatch. Expected 'product_a', got '" << (order_res ? std::string(order_res->value_view()) : "NOT_FOUND") << "'." << std::endl;
            test_passed = false;
        }
        
        
        db.reset();
        if (test_passed) {
            std::cout << "Phase 4: Final state verified. Durability test PASSED!" << std::endl;
        } else {
            std::cout << "Phase 4: Durability test FAILED!" << std::endl;
        }
    }
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
}


void run_basic_correctness_test() {
    std::cout << "\n--- Running Basic Correctness Test (Multi-Collection) ---" << std::endl;
    std::atomic<bool> test_passed = true;
    std::filesystem::path db_base_dir = "./db_data_basic_correctness";
    std::filesystem::path db_dir = db_base_dir / ("test_db_" + std::to_string(::Tests::get_process_id()));
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    
    auto db = Database::create_new(db_dir, BENCHMARK_NUM_THREADS); 
    
    uint32_t col1_idx = db->get_collection("users_table");
    uint32_t col2_idx = db->get_collection("orders_table");
    
    Collection& col1_ref = db->get_collection_by_idx(col1_idx);
    TxnContext ctx1 = col1_ref.begin_transaction_context(0, false); 
    TransactionBatch batch1;
    col1_ref.insert(ctx1, batch1, "users:kris", "kris_payload");
    col1_ref.commit(ctx1, batch1);

    Collection& col2_ref = db->get_collection_by_idx(col2_idx);
    TxnContext ctx2 = col2_ref.begin_transaction_context(0, false); 
    TransactionBatch batch2;
    col2_ref.insert(ctx2, batch2, "orders:101", "order_101_payload");
    col2_ref.commit(ctx2, batch2);

    Collection& col1_ref_read = db->get_collection_by_idx(col1_idx);
    TxnContext ctx3 = col1_ref_read.begin_transaction_context(0, true); 
    auto res_user = col1_ref_read.get(ctx3, "users:kris");
    if (!res_user.has_value() || res_user->value_view() != "kris_payload") {
        std::cerr << "FAIL: Basic Correctness - users:kris value mismatch. Expected 'kris_payload', got '" << (res_user ? std::string(res_user->value_view()) : "NOT_FOUND") << "'." << std::endl;
        test_passed = false;
    }
    if (col1_ref_read.get(ctx3, "orders:101").has_value()) {
        std::cerr << "FAIL: Basic Correctness - orders:101 found in users collection." << std::endl;
        test_passed = false;
    }
    

    Collection& col2_ref_read = db->get_collection_by_idx(col2_idx);
    TxnContext ctx4 = col2_ref_read.begin_transaction_context(0, true); 
    auto res_order = col2_ref_read.get(ctx4, "orders:101");
    if (!res_order.has_value() || res_order->value_view() != "order_101_payload") {
        std::cerr << "FAIL: Basic Correctness - orders:101 value mismatch. Expected 'order_101_payload', got '" << (res_order ? std::string(res_order->value_view()) : "NOT_FOUND") << "'." << std::endl;
        test_passed = false;
    }
    if (col2_ref_read.get(ctx4, "users:kris").has_value()) {
        std::cerr << "FAIL: Basic Correctness - users:kris found in orders collection." << std::endl;
        test_passed = false;
    }
    
    
    if (test_passed) {
        std::cout << "Basic Correctness Test Passed!" << std::endl;
    } else {
        std::cout << "Basic Correctness Test FAILED!" << std::endl;
    }

    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
}


void run_compaction_effectiveness_test() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- COMPACTION EFFECTIVENESS TEST ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    const size_t num_items = 50000;
    const size_t num_updates = 100000; 
    const size_t num_deletes = 10000;
    const size_t num_threads = 4;

    auto generate_and_fragment_db = [&](const std::filesystem::path& db_dir) {
        auto db = Database::create_new(db_dir, num_threads);
        uint32_t col_idx = db->get_collection("compaction_test");
        Collection& col = db->get_collection_by_idx(col_idx);

        
        TxnContext ctx_init = col.begin_transaction_context(0, false);
        TransactionBatch batch_init;
        for (size_t i = 0; i < num_items; ++i) {
            col.insert(ctx_init, batch_init, "key:" + std::to_string(i), "initial_value_" + std::to_string(i));
        }
        col.commit(ctx_init, batch_init);

        
        TxnContext ctx_update = col.begin_transaction_context(0, false);
        TransactionBatch batch_update;
        std::mt19937 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<size_t> key_dist(0, num_items - 1);
        for (size_t i = 0; i < num_updates; ++i) {
            col.insert(ctx_update, batch_update, "key:" + std::to_string(key_dist(rng)), "updated_value_" + std::to_string(i));
        }
        col.commit(ctx_update, batch_update);

        
        TxnContext ctx_delete = col.begin_transaction_context(0, false);
        TransactionBatch batch_delete;
        for (size_t i = 0; i < num_deletes; ++i) {
            col.remove(ctx_delete, batch_delete, "key:" + std::to_string(i)); 
        }
        col.commit(ctx_delete, batch_delete);
        return db;
    };

    auto print_stats = [](const std::string& title, StaxStats::DatabaseStatisticsCollector& collector) {
        StaxStats::DatabaseStats summary = collector.get_database_summary_stats(true);
        auto col_stats_map = collector.get_all_collection_stats();
        if (col_stats_map.empty()) {
            std::cout << "  " << title << ": No collections found." << std::endl;
            return;
        }
        StaxStats::CollectionStats stats = col_stats_map.begin()->second;

        double total_mb = static_cast<double>(summary.total_allocated_disk_bytes) / (1024.0 * 1024.0);
        double live_mb = static_cast<double>(stats.live_record_bytes) / (1024.0 * 1024.0);
        
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "  " << title << ":" << std::endl;
        std::cout << "    - Total DB Size on Disk: " << total_mb << " MB" << std::endl;
        std::cout << "    - Collection Live Data:  " << live_mb << " MB" << std::endl;
        std::cout << "    - Reclaimable Space:     " << stats.value_store_reclaimable_space_ratio * 100.0 << "%" << std::endl;
        std::cout << "    - Logical Item Count:    " << stats.logical_item_count << std::endl;
    };

    
    std::cout << "\n--- SCENARIO 1: Standard Compaction (Defragmentation) ---" << std::endl;
    std::filesystem::path db1_dir = "./db_data_compaction_test/db1_defrag";
    if (std::filesystem::exists(db1_dir)) std::filesystem::remove_all(db1_dir);
    std::filesystem::create_directories(db1_dir);
    
    {
        auto db1 = generate_and_fragment_db(db1_dir);
        auto collector_before = db1->get_statistics_collector();
        print_stats("Before Compaction", collector_before);
        db1.reset(); 

        Database::compact(db1_dir, num_threads, false); 

        auto db1_after = Database::open_existing(db1_dir, num_threads);
        auto collector_after = db1_after->get_statistics_collector();
        print_stats("After Compaction", collector_after);
    }

    
    std::cout << "\n--- SCENARIO 2: Flattening Compaction (Space Reclamation) ---" << std::endl;
    std::filesystem::path db2_dir = "./db_data_compaction_test/db2_flatten";
    if (std::filesystem::exists(db2_dir)) std::filesystem::remove_all(db2_dir);
    std::filesystem::create_directories(db2_dir);

    {
        auto db2 = generate_and_fragment_db(db2_dir);
        auto collector_before = db2->get_statistics_collector();
        print_stats("Before Compaction", collector_before);
        db2.reset(); 

        Database::compact(db2_dir, num_threads, true); 

        auto db2_after = Database::open_existing(db2_dir, num_threads);
        auto collector_after = db2_after->get_statistics_collector();
        print_stats("After Compaction", collector_after);
    }
    
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "Compaction Effectiveness Test Finished!" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    
    if (std::filesystem::exists("./db_data_compaction_test")) {
        std::filesystem::remove_all("./db_data_compaction_test");
    }
}


} 