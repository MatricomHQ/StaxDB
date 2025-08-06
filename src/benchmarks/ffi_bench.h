
#pragma once

#include <string>
#include <vector>
#include <memory>   
#include <iostream> 
#include <chrono>   


#include "stax_db/db.h"          
#include "stax_common/constants.h"   
#include "stax_core/value_store.hpp" 
#include "stax_tx/transaction.h" 

namespace FFIBench {


struct CppTestData {
    std::string Key;
    std::string Value;
};


inline std::vector<CppTestData> generate_cpp_test_data(int count) {
    std::vector<CppTestData> data;
    data.reserve(count);
    for (int i = 0; i < count; ++i) {
        data.push_back({
            "user" + std::to_string(i),
            "data" + std::to_string(i) + "blahblahblah"
        });
    }
    return data;
}


inline void run_ffi_style_benchmark(::Database* db) {
    const int num_items = 1'000'000;
    
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- StaxDB C++ FFI-Style Benchmark ---" << std::endl;
    std::cout << "Number of items: " << num_items << std::endl;

    if (!db) {
        std::cerr << "Database pointer is null!" << std::endl;
        return;
    }

    
    std::cout << "Generating test data..." << std::endl;
    std::vector<CppTestData> data = generate_cpp_test_data(num_items);
    std::cout << "Data generation complete." << std::endl;

    
    uint32_t collection_idx = db->get_collection("cpp_benchmark_collection");
    
    ::Collection& col = db->get_collection_by_idx(collection_idx);

    
    std::cout << "\n--- Benchmarking Inserts (C++) ---" << std::endl;
    
    TxnContext insert_ctx = col.begin_transaction_context(0, false); 
    TransactionBatch insert_batch; 

    auto start_insert = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_items; ++i) {
        col.insert(insert_ctx, insert_batch, data[i].Key, data[i].Value); 
    }
    
    col.commit(insert_ctx, insert_batch); 
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insert - start_insert);

    std::cout << "Total Insert Time: " << insert_duration.count() / 1'000'000 << " ms" << std::endl;
    std::cout << "Avg Insert Latency: " << insert_duration.count() / num_items << " ns/op" << std::endl;

    
    std::cout << "\n--- Benchmarking Gets (C++) ---" << std::endl;
    
    TxnContext get_ctx = col.begin_transaction_context(0, true); 
    
    auto start_get = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_items; ++i) {
        auto res = col.get(get_ctx, data[i].Key); 
        if (!res || res->value_view() != data[i].Value) {
            std::cerr << "Get failed or value mismatch for key: " << data[i].Key << std::endl;
        }
    }

    auto end_get = std::chrono::high_resolution_clock::now();
    
    auto get_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_get - start_get);

    std::cout << "Total Get Time: " << get_duration.count() / 1'000'000 << " ms" << std::endl;
    std::cout << "Avg Get Latency: " << get_duration.count() / num_items << " ns/op" << std::endl;
    std::cout << "==========================================================================================\n" << std::endl;
}

} 