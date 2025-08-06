
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
#include <cmath>    
#include <iomanip>  
#include <filesystem> 
#include <mutex>    
#include <map>      

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h> 
#endif


#include "stax_db/db.h"          
#include "stax_common/constants.h"   
#include "benchmarks/throughput_bench.h" 
#include "stax_common/spin_locks.h" 
#include "stax_tx/transaction.h" 
#include "stax_db/statistics.h" 
#include "tests/common_test_utils.h" 

namespace MixedWorkloadBench {

namespace { 



class ZipfianGenerator {
private:
    uint64_t n;
    double theta;
    double alpha;
    double zetan;
    double eta;
    std::mt19937 gen;
    std::uniform_real_distribution<> dis;
    double zeta(uint64_t n_val, double theta_val) {
        double sum = 0;
        for (uint64_t i = 1; i <= n_val; i++) {
            sum += 1.0 / std::pow((double)i, theta_val);
        }
        return sum;
    }

public:
    ZipfianGenerator(uint64_t n_items, double theta_val = 0.99)
        : n(n_items), theta(theta_val),
          alpha(1.0 / (1.0 - theta)), zetan(zeta(n, theta)),
          eta((1.0 - std::pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan)),
          gen(std::chrono::high_resolution_clock::now().time_since_epoch().count()), dis(0.0, 1.0) {}

    uint64_t next() {
        double u = dis(gen);
        double uz = u * zetan;
        if (uz < 1.0) return 0;
        if (uz < 1.0 + std::pow(0.5, theta)) return 1;
        return (uint64_t)(n * std::pow(eta * u - eta + 1.0, alpha));
    }
};
enum class OpType { READ, INSERT, UPDATE };
struct WorkloadOperation { OpType type; size_t key_idx; std::string value_payload_str; };


inline std::vector<std::vector<WorkloadOperation>> generate_mixed_workload(size_t num_threads, size_t total_ops, size_t initial_db_size, double read_ratio, double insert_ratio, std::mt19937& value_gen) {
    std::vector<std::vector<WorkloadOperation>> thread_ops(num_threads);
    std::atomic<size_t> current_db_size = initial_db_size;
    std::mt19937 op_type_gen(1337);
    std::uniform_real_distribution<> op_dist(0.0, 1.0);
    ZipfianGenerator zipf(initial_db_size);
    
    for (size_t i = 0; i < total_ops; ++i) {
        double op_roll = op_dist(op_type_gen);
        size_t thread_idx = i % num_threads;
        if (op_roll < read_ratio) {
            thread_ops[thread_idx].push_back({OpType::READ, zipf.next(), ""});
        } else if (op_roll < read_ratio + insert_ratio) {
            size_t new_key_idx = current_db_size.fetch_add(1);
            thread_ops[thread_idx].push_back({OpType::INSERT, new_key_idx, ::ThroughputBench::generate_random_value(128, value_gen)});
        } else {
            thread_ops[thread_idx].push_back({OpType::UPDATE, zipf.next(), ::ThroughputBench::generate_random_value(128, value_gen)});
        }
    }
    return thread_ops;
}



static std::map<std::string, std::string> s_final_key_state;
static SpinLock s_final_key_state_mutex;


inline void run_db_mixed_workload(std::unique_ptr<::Database>& db, size_t num_threads, const std::vector<std::vector<WorkloadOperation>>& thread_ops) {
    std::atomic<size_t> read_ops = 0, write_ops = 0;
    std::atomic<long long> total_read_latency_ns = 0, total_write_latency_ns = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    
    
    uint32_t collection_idx = db->get_collection("mixed_workload");

    for(size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() { 
            ::Collection& col = db->get_collection_by_idx(collection_idx); 
            
            TxnContext ctx = col.begin_transaction_context(i, false); 
            TransactionBatch batch; 

            for (const auto& op : thread_ops[i]) {
                std::string key = "workload:" + std::to_string(op.key_idx);
                auto op_start = std::chrono::high_resolution_clock::now();

                if (op.type == OpType::READ) {
                    
                    col.get(ctx, key);
                    read_ops++;
                } else { 
                    col.insert(ctx, batch, key, op.value_payload_str); 
                    
                    {
                        UniqueSpinLockGuard lock(s_final_key_state_mutex);
                        s_final_key_state[key] = op.value_payload_str;
                    }
                    write_ops++;
                }
                
                auto op_end = std::chrono::high_resolution_clock::now();
                if (op.type == OpType::READ) {
                    total_read_latency_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
                } else {
                    total_write_latency_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
                }
            }
            col.commit(ctx, batch); 
        });
    }
    for(auto& t : threads) t.join();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration_s = std::chrono::duration<double>(end_time - start_time).count();
    size_t total_ops = read_ops + write_ops;
    double ops_per_second = total_duration_s > 0 ? total_ops / total_duration_s : 0;
    std::cout << "--- Mixed Workload (StaxDB) ---" << std::endl;
    std::cout << "  - Total Duration: " << std::fixed << std::setprecision(2) << total_duration_s << " s" << std::endl;
    std::cout << "  - Throughput: " << (int)ops_per_second << " ops/sec" << std::endl;
    std::cout << "  - Avg Read Latency: " << (read_ops > 0 ? total_read_latency_ns / read_ops : 0) << " ns" << std::endl;
    std::cout << "  - Avg Write Latency: " << (write_ops > 0 ? total_write_latency_ns / write_ops : 0) << " ns" << std::endl;
              
    
    std::cout << "Final Verification Phase: Verifying " << s_final_key_state.size() << " keys after mixed workload..." << std::endl;
    std::atomic<size_t> verification_errors = 0;
    std::vector<std::thread> verification_threads;
    std::vector<std::pair<std::string, std::string>> failed_verifications; 
    std::mutex failed_verifications_mutex;


    
    
    {
        ::Collection& col = db->get_collection_by_idx(collection_idx); 
        TxnContext barrier_ctx = col.begin_transaction_context(0, false); 
        TransactionBatch barrier_batch;
        col.insert(barrier_ctx, barrier_batch, "~verification_barrier_key~", "sync");
        col.commit(barrier_ctx, barrier_batch);
    }

    
    
    std::vector<std::vector<std::map<std::string, std::string>::const_iterator>> thread_key_ranges(num_threads);
    size_t keys_per_thread = s_final_key_state.size() / num_threads;
    auto map_it = s_final_key_state.begin();
    for(size_t t = 0; t < num_threads; ++t) {
        thread_key_ranges[t].push_back(map_it);
        if (t == num_threads - 1) {
            map_it = s_final_key_state.end();
        } else {
            std::advance(map_it, keys_per_thread);
        }
        thread_key_ranges[t].push_back(map_it); 
    }

    for (size_t i = 0; i < num_threads; ++i) {
        verification_threads.emplace_back([&, i]() { 
            ::Collection& col = db->get_collection_by_idx(collection_idx); 
            TxnContext ctx = col.begin_transaction_context(i, true); 
            auto start_range = thread_key_ranges[i][0];
            auto end_range = thread_key_ranges[i][1];

            for (auto it = start_range; it != end_range; ++it) {
                const std::string& key = it->first;
                const std::string& expected_value = it->second;
                auto res = col.get(ctx, key);
                if (!res || res->value_view() != expected_value) {
                    verification_errors.fetch_add(1, std::memory_order_relaxed);
                    std::lock_guard<std::mutex> lock(failed_verifications_mutex);
                    failed_verifications.push_back({key, expected_value});
                }
            }
            
        });
    }
    for (auto& t : verification_threads) if (t.joinable()) t.join();

    if (verification_errors.load() == 0) {
        std::cout << "  Verification PASSED. All keys have correct values." << std::endl;
    } else {
        std::cout << "  !!! Verification FAILED. Found " << verification_errors.load() << " incorrect values. !!!" << std::endl;
        
        
        std::cout << "\n--- Re-verifying " << failed_verifications.size() << " failed keys (Single-Threaded) ---" << std::endl;
        size_t still_failed_count = 0;
        ::Collection& col = db->get_collection_by_idx(collection_idx);
        TxnContext re_verify_ctx = col.begin_transaction_context(0, true); 
        
        for (const auto& failed_pair : failed_verifications) {
            const std::string& key = failed_pair.first;
            const std::string& expected_value = failed_pair.second;
            auto res = col.get(re_verify_ctx, key);
            if (!res || res->value_view() != expected_value) {
                still_failed_count++;
                std::cerr << "    [RE-VERIFY FAILED] Key: '" << key << "'. Expected: '" << expected_value << "'. Got: '" << (res ? std::string(res->value_view()) : "NOT_FOUND") << "'" << std::endl;
            }
        }
        
        
        if (still_failed_count == 0) {
            std::cout << "  All initially failed keys were found correctly on second pass. Issue is likely MVCC visibility." << std::endl;
        } else {
            std::cout << "  " << still_failed_count << " keys are still incorrect. This indicates DATA CORRUPTION or a race condition in the tree." << std::endl;
        }
    }
}

} 

inline void run_mixed_workload_suite() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- REAL-WORLD MIXED WORKLOAD SUITE ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
    const size_t INITIAL_DB_SIZE = 1'000'000;
    const size_t TOTAL_OPS = 2'000'000;
    const double READ_RATIO = 0.8;
    const double INSERT_RATIO = 0.2;
    const size_t NUM_THREADS = BENCHMARK_NUM_THREADS;
    
    std::filesystem::path db_base_dir = "./db_data";
    
    std::filesystem::path db_dir = db_base_dir / ("mixed_db_" + std::to_string(Tests::get_process_id()));

    if(std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(db_base_dir);

    std::mt19937 shared_value_gen(1338); 
    auto workload = MixedWorkloadBench::generate_mixed_workload(NUM_THREADS, TOTAL_OPS, INITIAL_DB_SIZE, READ_RATIO, INSERT_RATIO, shared_value_gen); 
    
    std::unique_ptr<::Database> db = ::Database::create_new(db_dir, NUM_THREADS);
    std::cout << "Pre-populating StaxDB with " << INITIAL_DB_SIZE << " items..." << std::endl;

    uint32_t stax_collection_idx = db->get_collection("mixed_workload"); 
    const size_t batch_size = INITIAL_DB_SIZE / NUM_THREADS; 

    
    std::vector<std::thread> prepop_threads;
    for (size_t t = 0; t < NUM_THREADS; ++t) {
        prepop_threads.emplace_back([&, t]() {
            ::Collection& stax_collection = db->get_collection_by_idx(stax_collection_idx);
            TxnContext ctx = stax_collection.begin_transaction_context(t, false); 
            TransactionBatch batch; 
            
            std::mt19937 thread_local_value_gen(42 + t); 
            for (size_t j = 0; j < batch_size; ++j) { 
                size_t current_idx = t * batch_size + j;
                if (current_idx >= INITIAL_DB_SIZE) break; 
                std::string key = "workload:" + std::to_string(current_idx);
                std::string value = ::ThroughputBench::generate_random_value(128, thread_local_value_gen);
                stax_collection.insert(ctx, batch, key, value);
                
                {
                    UniqueSpinLockGuard lock(s_final_key_state_mutex);
                    s_final_key_state[key] = value;
                }
            }
            stax_collection.commit(ctx, batch);
        });
    }
    for (auto& t : prepop_threads) t.join();

    std::cout << "StaxDB Pre-population complete." << std::endl;
        
    std::cout << "\n--- Starting StaxDB Mixed Workload ---" << std::endl;
    MixedWorkloadBench::run_db_mixed_workload(db, NUM_THREADS, workload);

    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
}
} 