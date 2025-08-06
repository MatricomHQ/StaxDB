
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
#include <numeric>  
#include <filesystem> 

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h> 
#endif


#include "stax_db/db.h"          
#include "stax_db/path_engine.h" 
#include "stax_common/constants.h"   
#include "stax_core/value_store.hpp" 
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 

namespace ThroughputBench {


enum class KeyType {
    SEQUENTIAL,
    LONG_SEQUENTIAL,
    RANDOM
};

struct TestData { 
    std::string key; 
    std::string value; 
    size_t actual_stored_size_bytes; 
    std::string miss_key; 
};

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
};




namespace { 




inline std::string generate_random_value(size_t length, std::mt19937& gen) {
    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string res;
    if (length == 0) return res;
    res.reserve(length);
    std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
    for (size_t i = 0; i < length; ++i) res += alphanum[dist(gen)];
    return res;
}


inline std::vector<TestData> generate_throughput_test_data(size_t num_items, size_t min_size, size_t max_size, KeyType key_type = KeyType::SEQUENTIAL) {
    std::vector<TestData> data;
    data.reserve(num_items);
    std::mt19937 gen(1338);
    ::PathEngine pe; 
    std::uniform_int_distribution<size_t> size_dist(min_size, max_size);
    std::uniform_int_distribution<uint64_t> random_key_dist(0, std::numeric_limits<uint64_t>::max());

    for (size_t i = 0; i < num_items; ++i) {
        std::string key_str;
        switch (key_type) {
            case KeyType::SEQUENTIAL: {
                std::string numeric_id_part = pe.create_numeric_sortable_key("", i);
                key_str = "users:id:" + numeric_id_part.substr(1) + '\0' + "payload";
                break;
            }
            case KeyType::LONG_SEQUENTIAL: {
                std::string numeric_id_part = pe.create_numeric_sortable_key("", i);
                key_str = "partition:A/users:id:" + numeric_id_part.substr(1) + '\0' + "payload-with-some-extra-long-suffix-data";
                break;
            }
            case KeyType::RANDOM: {
                uint64_t random_val1 = random_key_dist(gen);
                uint64_t random_val2 = random_key_dist(gen);
                key_str.resize(16);
                memcpy(&key_str[0], &random_val1, sizeof(uint64_t));
                memcpy(&key_str[8], &random_val2, sizeof(uint64_t));
                break;
            }
        }
        
        std::string value_str = generate_random_value(size_dist(gen), gen);
        
        
        std::string miss_key_str = "nonexistent:" + key_str;
        
        
        
        const size_t total_record_size_allocated = 
            CollectionRecordAllocator::get_allocated_record_size(key_str.length(), value_str.length());

        data.push_back({
            key_str,
            value_str,
            total_record_size_allocated, 
            miss_key_str 
        });
    }
    return data;
}



static std::vector<std::vector<ThroughputBench::TestData>> s_thread_data;



inline void print_final_results(const BenchResults& results) {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- FINAL THROUGHPUT BENCHMARK RESULTS (" << results.map_name << ") ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
    std::cout << "Insert Total Time:         " << std::fixed << std::setprecision(3) << results.insert_duration.count() / 1e6 << " ms" << std::endl;
    std::cout << "Insert Avg Latency:        " << std::fixed << std::setprecision(2) << results.insert_avg_latency_ns << " ns/op" << std::endl;
    std::cout << "Insert Throughput:         " << std::fixed << std::setprecision(2) << results.insert_throughput_mbps << " MB/s" << std::endl;
    std::cout << "------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "Update Total Time:         " << std::fixed << std::setprecision(3) << results.update_duration.count() / 1e6 << " ms" << std::endl;
    std::cout << "Update Avg Latency:        " << std::fixed << std::setprecision(2) << results.update_avg_latency_ns << " ns/op" << std::endl;
    std::cout << "Update Throughput:         " << std::fixed << std::setprecision(2) << results.update_throughput_mbps << " MB/s" << std::endl;
    std::cout << "------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "Get (Hits) Total Time:     " << std::fixed << std::setprecision(3) << results.get_duration.count() / 1e6 << " ms" << std::endl;
    std::cout << "Get (Hits) Avg Latency:    " << std::fixed << std::setprecision(2) << results.get_avg_latency_ns << " ns/op" << std::endl;
    std::cout << "Get (Hits) Throughput:     " << std::fixed << std::setprecision(2) << results.get_throughput_mbps << " MB/s" << std::endl;
    std::cout << "Get (Hits) Success/Fail:   " << results.get_hits << " / " << results.get_misses << std::endl;
    std::cout << "------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "Get (Misses) Total Time:   " << std::fixed << std::setprecision(3) << results.get_nonexistent_duration.count() / 1e6 << " ms" << std::endl;
    std::cout << "Get (Misses) Avg Latency:  " << std::fixed << std::setprecision(2) << results.get_nonexistent_avg_latency_ns << " ns/op" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
}

} 



inline void run_throughput_suite(const std::string& suite_name, size_t num_items, size_t min_size, size_t max_size, size_t num_threads) {
    BenchResults results;
    results.map_name = "StaxDB Transactional";
    size_t total_items = num_items;
    
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- STAXDB THROUGHPUT SUITE (" << suite_name << ") ---" << std::endl;
    std::cout << "Items: " << num_items << ", Threads: " << num_threads << ", Value Size: " << min_size << "-" << max_size << " bytes" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
    
    std::filesystem::path db_base_dir = "./db_data_throughput";
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
    auto test_data_pool = generate_throughput_test_data(num_items, min_size, max_size);
    
    s_thread_data.assign(num_threads, {});
    size_t items_per_thread = num_items / num_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * items_per_thread;
        size_t end_idx = (i == num_threads - 1) ? num_items : start_idx + items_per_thread;
        if (start_idx < end_idx) {
             s_thread_data[i].assign(test_data_pool.begin() + start_idx, test_data_pool.begin() + end_idx);
        }
    }

    uint32_t collection_idx = db->get_collection("throughput_bench"); 

    auto start_insert = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> insert_threads;
    std::atomic<uint64_t> total_insert_bytes = 0; 
    for (size_t i = 0; i < num_threads; ++i) {
        insert_threads.emplace_back([&db, &collection_idx, &total_insert_bytes, thread_idx = i]() { 
            Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(thread_idx, false); 
            TransactionBatch batch; 

            for (const auto& item : s_thread_data[thread_idx]) { 
                total_insert_bytes.fetch_add(item.actual_stored_size_bytes, std::memory_order_relaxed); 
                col.insert(ctx, batch, item.key, item.value); 
            }
            col.commit(ctx, batch); 
        });
    }
    for (auto& t : insert_threads) if(t.joinable()) t.join();
    results.insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_insert);
    results.insert_avg_latency_ns = total_items > 0 ? static_cast<double>(results.insert_duration.count()) / total_items : 0.0;
    double insert_duration_s = results.insert_duration.count() / 1e9;
    results.insert_throughput_mbps = (insert_duration_s > 0) ? (total_insert_bytes.load() / (1024.0 * 1024.0)) / insert_duration_s : 0.0;
    std::cout << "Insert Phase: " << total_items << " items in " << insert_duration_s * 1000 << " ms. "
              << "Avg Latency: " << std::fixed << std::setprecision(2) << results.insert_avg_latency_ns << " ns. "
              << "Throughput: " << results.insert_throughput_mbps << " MB/s."
              << std::endl;
    
    {
        Collection& col = db->get_collection_by_idx(collection_idx); 
        TxnContext barrier_ctx = col.begin_transaction_context(0, false); 
        TransactionBatch barrier_batch;
        col.insert(barrier_ctx, barrier_batch, "~barrier_key~", "sync");
        col.commit(barrier_ctx, barrier_batch);
    }

    auto start_get = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_threads;
    std::atomic<size_t> total_hits = 0;
    std::atomic<uint64_t> total_get_bytes = 0; 
    for (size_t i = 0; i < num_threads; ++i) {
        get_threads.emplace_back([&db, &collection_idx, &total_hits, &total_get_bytes, thread_idx = i]() { 
            Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(thread_idx, true); 
            for (const auto& item : s_thread_data[thread_idx]) {
                auto res = col.get(ctx, item.key); 
                
                if (res) {
                    if (item.key.length() != res->key_len || std::string_view(res->key_ptr, res->key_len) != item.key) {
                        std::cerr << "!!! ERROR: Key mismatch for queried key: '" << item.key << "'. "
                                  << "Retrieved key: '" << std::string_view(res->key_ptr, res->key_len) << "'" << std::endl;
                    }
                    total_hits.fetch_add(1, std::memory_order_relaxed);
                    total_get_bytes.fetch_add(item.actual_stored_size_bytes, std::memory_order_relaxed);
                } else {
                    std::cerr << "!!! ANOMALY: Key '" << item.key << "' NOT FOUND in Get Phase (Hits)! Thread: " << thread_idx << ", ReadTxnID: " << ctx.read_snapshot_id << std::endl;
                    //db->dump_state(std::cerr);
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
    results.get_throughput_mbps = (get_duration_s > 0) ?
        (total_get_bytes.load() / (1024.0 * 1024.0)) / get_duration_s : 0.0;
    std::cout << "Get Phase (Hits): " << results.get_hits << " hits, " << results.get_misses << " misses in "
              << get_duration_s * 1000 << " ms. " << "Avg Latency: " << results.get_avg_latency_ns << " ns. "
              << "Throughput: " << results.get_throughput_mbps << " MB/s."
              << std::endl;

    auto start_get_nonexistent = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> get_nonexistent_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        get_nonexistent_threads.emplace_back([&db, &collection_idx, thread_idx = i]() { 
            Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(thread_idx, true); 
            volatile size_t miss_accumulator = 0; 
            for (const auto& item : s_thread_data[thread_idx]) {
                auto res = col.get(ctx, item.miss_key);
                if (res) miss_accumulator += 1; 
            }
        });
    }
    for (auto& t : get_nonexistent_threads) if(t.joinable()) t.join();
    results.get_nonexistent_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_get_nonexistent);
    results.get_nonexistent_avg_latency_ns = total_items > 0 ? static_cast<double>(results.get_nonexistent_duration.count()) / total_items : 0.0;
    double get_nonexistent_duration_s = results.get_nonexistent_duration.count() / 1e9;
    std::cout << "Get Phase (Misses): " << total_items << " items in "
              << get_nonexistent_duration_s * 1000 << " ms. " << "Avg Latency: " << results.get_nonexistent_avg_latency_ns << " ns. " << std::endl;
    
    auto start_update = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> update_threads;
    std::atomic<uint64_t> total_update_bytes = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        update_threads.emplace_back([&db, &collection_idx, &total_update_bytes, thread_idx = i]() { 
            Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(thread_idx, false); 
            TransactionBatch batch;
            for (const auto& item : s_thread_data[thread_idx]) {
                total_update_bytes.fetch_add(item.actual_stored_size_bytes, std::memory_order_relaxed);
                col.insert(ctx, batch, item.key, item.value); 
            }
            col.commit(ctx, batch); 
        });
    }
    for (auto& t : update_threads) if(t.joinable()) t.join();
    results.update_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_update);
    results.update_avg_latency_ns = total_items > 0 ? static_cast<double>(results.update_duration.count()) / total_items : 0.0;
    double update_duration_s = results.update_duration.count() / 1e9;
    results.update_throughput_mbps = (update_duration_s > 0) ? (total_update_bytes.load() / (1024.0 * 1024.0)) / update_duration_s : 0.0;
    std::cout << "Update Phase (Overwrite): " << total_items << " items in " << update_duration_s * 1000 << " ms. "
              << "Avg Latency: " << std::fixed << std::setprecision(2) << results.update_avg_latency_ns << " ns. "
              << "Throughput: " << results.update_throughput_mbps << " MB/s."
              << std::endl;
              
    std::cout << "Final Verification Phase: Verifying all keys after updates..." << std::endl;
    std::atomic<size_t> verification_errors = 0;
    std::vector<std::thread> verification_threads;
     {
        Collection& col = db->get_collection_by_idx(collection_idx); 
        TxnContext barrier_ctx = col.begin_transaction_context(0, false); 
        TransactionBatch barrier_batch;
        col.insert(barrier_ctx, barrier_batch, "~barrier_key_2~", "sync");
        col.commit(barrier_ctx, barrier_batch);
    }
    for (size_t i = 0; i < num_threads; ++i) {
        verification_threads.emplace_back([&db, &collection_idx, &verification_errors, thread_idx = i]() { 
            Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(thread_idx, true); 
            for (const auto& item : s_thread_data[thread_idx]) {
                auto res = col.get(ctx, item.key);
                if (!res || res->value_view() != item.value) {
                    verification_errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& t : verification_threads) if (t.joinable()) t.join();
    if (verification_errors.load() == 0) {
        std::cout << "  Verification PASSED. All keys have correct values." << std::endl;
    } else {
        std::cout << "  !!! Verification FAILED. Found " << verification_errors.load() << " incorrect values. !!!" << std::endl;
    }
}


} 