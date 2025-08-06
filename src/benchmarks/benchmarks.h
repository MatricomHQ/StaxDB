
#pragma once


#include <iostream>
#include <filesystem>
#include <string>

#include <set> 
#include <vector> 
#include <map> 
#include <chrono> 
#include <atomic> 
#include <thread> 
#include <random> 
#include <iomanip> 
#include <algorithm> 
#include <tuple> 
#include <cmath> 
#include <mutex> 
#include <unordered_map> 


#include "stax_db/db.h"
#include "stax_common/constants.h"


#include "benchmarks/ffi_bench.h"
#include "benchmarks/mixed_workload_bench.h"
#include "benchmarks/throughput_bench.h" 
#include "benchmarks/graph_bench.h"
#include "benchmarks/complex_query_bench.h"
#include "benchmarks/core_vs_maps.h" 
#include "benchmarks/tree_bench.h"
#include "tpcc.h" 



void run_all_benchmarks() { 
    std::cout << "\n\n\n******************************************************************************************" << std::endl;
    std::cout << "                              RUNNING ALL BENCHMARKS" << std::endl;
    std::cout << "******************************************************************************************" << std::endl;


        
    GraphBench::run_graph_benchmark();

    
    TreeBench::run_tree_stress_test();

    
    CoreVsMapsBench::run_stax_vs_maps_suite();
    
    
    
    {
        std::filesystem::path ffi_db_base_dir = "./db_data_ffi_bench";
        std::filesystem::path ffi_db_dir = ffi_db_base_dir / ("test_db_" + std::to_string(::Tests::get_process_id()));

        if (std::filesystem::exists(ffi_db_base_dir)) {
            try {
                std::filesystem::remove_all(ffi_db_base_dir);
            } catch (const std::filesystem::filesystem_error& e) {
                std::cerr << "Warning: Could not remove directory " << ffi_db_base_dir << ": " << e.what() << std::endl;
            }
        }
        std::filesystem::create_directories(ffi_db_base_dir);

        auto db_for_ffi_bench = Database::create_new(ffi_db_dir, 1);
        FFIBench::run_ffi_style_benchmark(db_for_ffi_bench.get());
        db_for_ffi_bench.reset();

        if (std::filesystem::exists(ffi_db_base_dir)) {
            try {
                std::filesystem::remove_all(ffi_db_base_dir);
            } catch (const std::filesystem::filesystem_error& e) {
                std::cerr << "Warning: Could not remove directory " << ffi_db_base_dir << " after FFI benchmark: " << e.what() << std::endl;
            }
        }
    }

    
    MixedWorkloadBench::run_mixed_workload_suite();
    
    
    
    ThroughputBench::run_throughput_suite("SMALL VALUES", BENCHMARK_NUM_ENTRIES_TOTAL, 16, 256, BENCHMARK_NUM_THREADS);
    ThroughputBench::run_throughput_suite("SMALL VALUES", BENCHMARK_NUM_ENTRIES_TOTAL, 16, 256, BENCHMARK_NUM_THREADS);
    ThroughputBench::run_throughput_suite("SMALL VALUES", BENCHMARK_NUM_ENTRIES_TOTAL, 16, 256, BENCHMARK_NUM_THREADS);
    ThroughputBench::run_throughput_suite("SMALL VALUES", BENCHMARK_NUM_ENTRIES_TOTAL, 16, 256, BENCHMARK_NUM_THREADS);

    ThroughputBench::run_throughput_suite("MED VALUES", BENCHMARK_NUM_ENTRIES_TOTAL, 256, 512, BENCHMARK_NUM_THREADS);


    
    run_tpcc_benchmark(5, BENCHMARK_NUM_THREADS);

    
    ComplexQueryBench::run_complex_query_suite();
}