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
#include "stax_db/path_engine.h" 
#include "stax_common/constants.h"   
#include "stax_db/query.h"       
#include "stax_common/roaring.h"     
#include "stax_core/value_store.hpp" 
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 

namespace ComplexQueryBench {

struct BenchResultsRow {
    std::string name;
    long long total_duration_ns;
    long long avg_latency_ns;
    size_t items_processed;
    std::string extra_info;
};


namespace { 





struct WideUser {
    uint64_t user_id;
    uint16_t f1_region, f2_category, f3_status;
    uint16_t f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    std::string f16_notes;

    uint64_t pack_z_order_payload() const; 
    std::string serialize_doc() const {
        return "id:" + std::to_string(user_id) + "|f1:" + std::to_string(f1_region);
    }
};


inline uint64_t spread_bits_16_local(uint16_t value) {
    uint64_t x = value;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8))  & 0x00FF00FF00FF00FF;
    x = (x | (x << 4))  & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2))  & 0x3333333333333333;
    x = (x | (x << 1))  & 0x5555555555555555;
    return x;
}

inline uint64_t z_order_encode_3x16_local(uint16_t val1, uint16_t val2, uint16_t val3) {
    return (spread_bits_16_local(val1) << 2) | (spread_bits_16_local(val2) << 1) | spread_bits_16_local(val3);
}

inline uint64_t WideUser::pack_z_order_payload() const {
    return z_order_encode_3x16_local(f1_region, f2_category, f3_status);
}


inline void insert_wide_user_local(::Collection& col, const TxnContext& ctx, TransactionBatch& batch, const WideUser& user, const ::PathEngine& pe) {
    std::string doc_key = "doc:wide_user:" + std::to_string(user.user_id);
    col.insert(ctx, batch, doc_key, user.serialize_doc());
    
    uint64_t z_payload = user.pack_z_order_payload();
    std::string idx_key_prefix = pe.create_numeric_sortable_key("idx:wide_user", z_payload);
    std::string full_idx_key = idx_key_prefix + ":" + std::to_string(user.user_id);
    col.insert(ctx, batch, full_idx_key, "1");
}

struct ComplexData {
    std::vector<std::string> user_payloads;
    std::vector<std::string> order_payloads;
    std::vector<std::pair<size_t, size_t>> user_order_pairs;
    std::vector<std::vector<size_t>> adj;
    std::vector<size_t> premium_customer_ids;
    std::vector<size_t> forum_active_ids;
    std::vector<std::pair<size_t, double>> order_line_amounts;
};

inline ComplexData generate_complex_test_data(size_t num_users, size_t num_orders_per_user, size_t friends_per_user) {
    ComplexData data;
    std::mt19937 gen(1337);
    std::uniform_real_distribution<> amount_dist(10.0, 5000.0);
    data.adj.resize(num_users);
    for (size_t i = 0; i < num_users; ++i) {
        data.user_payloads.push_back("user_payload_" + std::to_string(i));
        if (i % 10 == 0) data.premium_customer_ids.push_back(i);
        if (i % 25 == 0) data.forum_active_ids.push_back(i);
        for (size_t j = 0; j < num_orders_per_user; ++j) {
            size_t order_idx = i * num_orders_per_user + j;
            data.order_payloads.push_back("order_payload_" + std::to_string(order_idx));
            
            data.order_line_amounts.push_back({ (size_t)i % 10, amount_dist(gen) });
        }
        std::uniform_int_distribution<size_t> friend_dist(0, num_users - 1);
        for (size_t k = 0; k < friends_per_user; ++k) {
            size_t friend_idx = friend_dist(gen);
            if (i != friend_idx) { data.adj[i].push_back(friend_idx); }
        }
    }
    return data;
}

inline void print_advanced_results_table(const std::vector<BenchResultsRow>& results_table) {
    std::cout << "\n--- Advanced Query Suite Results (StaxDB) ---" << std::endl;
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

inline void run_multi_get_benchmark(std::unique_ptr<::Database>& db, const ComplexData& data, BenchResultsRow& results) {
    const size_t batch_size = 10000;
    const size_t num_threads = BENCHMARK_NUM_THREADS;
    std::atomic<size_t> found_count = 0;
    
    uint32_t users_col_idx = db->get_collection("users"); 

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for(size_t t=0; t<num_threads; ++t){
        threads.emplace_back([&, t]() {
            ::Collection& users_col = db->get_collection_by_idx(users_col_idx);
            TxnContext ctx = users_col.begin_transaction_context(t, true); 
            size_t local_found = 0;
            for(size_t i = t; i < batch_size; i+=num_threads) {
                size_t user_id_to_query = i * (data.user_payloads.size() / batch_size);
                if(users_col.get(ctx, "users:" + std::to_string(user_id_to_query))) {
                    local_found++;
                }
            }
            
            found_count += local_found;
        });
    }
    for(auto& th : threads) th.join();
    auto end = std::chrono::high_resolution_clock::now();
    
    results.name = "Multi-Get";
    results.items_processed = batch_size;
    results.total_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    results.avg_latency_ns = results.items_processed > 0 ? results.total_duration_ns / results.items_processed : 0;
    results.extra_info = "Found " + std::to_string(found_count);
}

inline void run_intersection_benchmark(std::unique_ptr<::Database>& db, const ComplexData& data, BenchResultsRow& results) {
    const size_t num_threads = BENCHMARK_NUM_THREADS;
    std::atomic<size_t> intersection_size = 0;
    
    uint32_t active_col_idx = db->get_collection("sets_forum_active"); 

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for(size_t t=0; t<num_threads; ++t){
        threads.emplace_back([&, t]() {
            ::Collection& thread_active_col = db->get_collection_by_idx(active_col_idx); 
            TxnContext ctx = thread_active_col.begin_transaction_context(t, true); 
            size_t local_found = 0;
            for(size_t i = t; i < data.premium_customer_ids.size(); i+=num_threads) {
                 if(thread_active_col.get(ctx, "sets:forum_active:" + std::to_string(data.premium_customer_ids[i]))) {
                    local_found++;
                }
            }
            
            intersection_size += local_found;
        });
    }
    for(auto& th : threads) th.join();
    auto end = std::chrono::high_resolution_clock::now();

    results.name = "Set Intersection";
    results.items_processed = data.premium_customer_ids.size();
    results.total_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    results.avg_latency_ns = results.items_processed > 0 ? results.total_duration_ns / results.items_processed : 0;
    results.extra_info = "Size: " + std::to_string(intersection_size);
}

inline void run_aggregation_benchmark(std::unique_ptr<::Database>& db, const ComplexData& data, BenchResultsRow& results) {
    const size_t num_threads = BENCHMARK_NUM_THREADS;
    std::vector<std::map<size_t, double>> thread_local_totals(num_threads);
    
    uint32_t orders_col_idx = db->get_collection("order_lines"); 

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
     for(size_t t=0; t<num_threads; ++t){
        threads.emplace_back([&, t]() {
            ::Collection& thread_orders_col = db->get_collection_by_idx(orders_col_idx); 
            TxnContext ctx = thread_orders_col.begin_transaction_context(t, true); 
            for(size_t i = t; i < data.order_line_amounts.size(); i+=num_threads) {
                std::string key_str = "order_lines:" + std::to_string(i);
                if (auto res = thread_orders_col.get(ctx, key_str)) {
                    double amount = std::stod(std::string(res->value_view()));
                    thread_local_totals[t][data.order_line_amounts[i].first] += amount;
                }
            }
            
        });
    }
    for(auto& th : threads) th.join();

    std::map<size_t, double> warehouse_totals;
    for(const auto& local_map : thread_local_totals){
        for(const auto& pair : local_map){
            warehouse_totals[pair.first] += pair.second;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();

    results.name = "Aggregation (GROUP BY)";
    results.items_processed = data.order_line_amounts.size();
    results.total_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    results.avg_latency_ns = results.items_processed > 0 ? results.total_duration_ns / results.items_processed : 0;
    results.extra_info = "Groups: " + std::to_string(warehouse_totals.size());
}

inline void run_delete_scan_benchmark(std::unique_ptr<::Database>& db, const ComplexData& data, BenchResultsRow& results) {
    const size_t num_to_delete = 1000;
    const size_t num_threads = BENCHMARK_NUM_THREADS;
    
    uint32_t orders_col_idx = db->get_collection("order_lines"); 

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for(size_t t=0; t<num_threads; ++t){
        threads.emplace_back([&, t]() {
            ::Collection& thread_orders_col = db->get_collection_by_idx(orders_col_idx); 
            TxnContext ctx = thread_orders_col.begin_transaction_context(t, false); 
            TransactionBatch batch; 
            for(size_t i = t; i < num_to_delete; i+=num_threads) {
                thread_orders_col.remove(ctx, batch, "order_lines:" + std::to_string(i));
            }
            thread_orders_col.commit(ctx, batch);
        });
    }
    for(auto& th : threads) th.join();
    auto end = std::chrono::high_resolution_clock::now();

    results.name = "Delete Scan";
    results.items_processed = num_to_delete;
    results.total_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    results.avg_latency_ns = results.items_processed > 0 ? results.total_duration_ns / results.items_processed : 0;
    results.extra_info = "";
}

inline void run_join_benchmark(std::unique_ptr<::Database>& db, const ComplexData& data, BenchResultsRow& results) {
    results.name = "Composite Key 'Join'";
    results.items_processed = 0;
    results.total_duration_ns = 0;
    results.avg_latency_ns = 0;
    results.extra_info = "SKIPPED";
}

} 

inline void run_complex_query_suite() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- ADVANCED QUERY BENCHMARK SUITE (Transactional) ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
    const size_t NUM_USERS = 10000;
    const size_t NUM_ORDERS_PER_USER = 10;
    const size_t FRIENDS_PER_USER = 5;
    const size_t NUM_THREADS = BENCHMARK_NUM_THREADS;
    const int TOTAL_RANGE_ITEMS = 10000;

    std::filesystem::path db_base_dir = "./db_data";
    std::filesystem::path db_dir = db_base_dir / ("complex_db_" + std::to_string(Tests::get_process_id()));
    
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(db_base_dir);

    auto data = ComplexQueryBench::generate_complex_test_data(NUM_USERS, NUM_ORDERS_PER_USER, FRIENDS_PER_USER);
    std::unique_ptr<::Database> db = ::Database::create_new(db_dir, NUM_THREADS);
    
    std::vector<ComplexQueryBench::BenchResultsRow> results_table;
    std::cout << "Ingesting data into StaxDB..." << std::endl;
    auto start_ingest = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> ingest_threads;

    
    uint32_t users_col_idx = db->get_collection("users");
    uint32_t orders_col_idx = db->get_collection("order_lines");
    uint32_t premium_col_idx = db->get_collection("sets_premium");
    uint32_t active_col_idx = db->get_collection("sets_forum_active");
    uint32_t friends_col_idx = db->get_collection("links_friends");
    uint32_t products_col_idx = db->get_collection("products_by_price");

    auto ingest_work = [&](size_t thread_id){
        
        ::Collection& users_col = db->get_collection_by_idx(users_col_idx);
        TxnContext users_ctx = users_col.begin_transaction_context(thread_id, false);
        TransactionBatch users_batch;
        for(size_t i = thread_id; i < data.user_payloads.size(); i+= NUM_THREADS) users_col.insert(users_ctx, users_batch, "users:" + std::to_string(i), data.user_payloads[i]);
        users_col.commit(users_ctx, users_batch);

        ::Collection& orders_col = db->get_collection_by_idx(orders_col_idx);
        TxnContext orders_ctx = orders_col.begin_transaction_context(thread_id, false);
        TransactionBatch orders_batch;
        for(size_t i = thread_id; i < data.order_line_amounts.size(); i+= NUM_THREADS) orders_col.insert(orders_ctx, orders_batch, "order_lines:" + std::to_string(i), std::to_string(data.order_line_amounts[i].second));
        orders_col.commit(orders_ctx, orders_batch);

        ::Collection& premium_col = db->get_collection_by_idx(premium_col_idx);
        TxnContext premium_ctx = premium_col.begin_transaction_context(thread_id, false);
        TransactionBatch premium_batch;
        for(size_t i = thread_id; i < data.premium_customer_ids.size(); i+= NUM_THREADS) premium_col.insert(premium_ctx, premium_batch, "sets:premium:" + std::to_string(data.premium_customer_ids[i]), "1");
        premium_col.commit(premium_ctx, premium_batch);

        ::Collection& active_col = db->get_collection_by_idx(active_col_idx);
        TxnContext active_ctx = active_col.begin_transaction_context(thread_id, false);
        TransactionBatch active_batch;
        for(size_t i = thread_id; i < data.forum_active_ids.size(); i+= NUM_THREADS) active_col.insert(active_ctx, active_batch, "sets:forum_active:" + std::to_string(data.forum_active_ids[i]), "1");
        active_col.commit(active_ctx, active_batch);

        ::Collection& friends_col = db->get_collection_by_idx(friends_col_idx);
        TxnContext friends_ctx = friends_col.begin_transaction_context(thread_id, false);
        TransactionBatch friends_batch;
        for(size_t i = thread_id; i < data.adj.size(); i+= NUM_THREADS) {
            for(size_t friend_idx : data.adj[i]) {
                friends_col.insert(friends_ctx, friends_batch, "links/friends/" + std::to_string(i) + "/" + std::to_string(friend_idx), "1");
            }
        }
        friends_col.commit(friends_ctx, friends_batch);

        ::Collection& products_col = db->get_collection_by_idx(products_col_idx);
        TxnContext products_ctx = products_col.begin_transaction_context(thread_id, false);
        TransactionBatch products_batch;
        for (int i = thread_id; i < TOTAL_RANGE_ITEMS; i += NUM_THREADS) {
            products_col.insert(products_ctx, products_batch, "products_by_price:" + std::to_string(5000 + (i * 10)), "product_payload_" + std::to_string(i));
        }
        products_col.commit(products_ctx, products_batch);
    };
    for(size_t i = 0; i < NUM_THREADS; ++i) ingest_threads.emplace_back(ingest_work, i);
    for(auto& t : ingest_threads) t.join();

    auto ingest_duration_ms = std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - start_ingest).count();
    std::cout << "Ingestion complete in " << ingest_duration_ms << " ms." << std::endl;
    std::cout << "\n--- Starting Benchmarks ---\n" << std::endl;
    
    ComplexQueryBench::BenchResultsRow res;
    ComplexQueryBench::run_multi_get_benchmark(db, data, res);
    results_table.push_back(res);

    ComplexQueryBench::run_intersection_benchmark(db, data, res);
    results_table.push_back(res);
    
    ComplexQueryBench::run_aggregation_benchmark(db, data, res);
    results_table.push_back(res);
    
    ComplexQueryBench::run_join_benchmark(db, data, res);
    results_table.push_back(res);

    ComplexQueryBench::run_delete_scan_benchmark(db, data, res);
    results_table.push_back(res);

    ComplexQueryBench::print_advanced_results_table(results_table);
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
}
} 