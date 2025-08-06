
#pragma once

#include <iostream>
#include <vector>
#include <thread>
#include <future> 
#include <string>
#include <filesystem>
#include <atomic>

#include "stax_db/db.h"
#include "tests/common_test_utils.h"

namespace Tests {

inline void run_concurrent_init_close_test() {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- CONCURRENT INIT & CLOSE STRESS TEST ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    const size_t num_threads = 8;
    const size_t cycles_per_thread = 125; 
    const size_t num_shared_paths = 2; 

    std::atomic<bool> test_passed = true;
    std::filesystem::path base_test_dir = "./db_data_init_test";

    
    if (std::filesystem::exists(base_test_dir)) {
        try {
            std::filesystem::remove_all(base_test_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << base_test_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(base_test_dir);
    std::cout << "Test environment prepared at: " << base_test_dir.string() << std::endl;

    
    std::vector<std::filesystem::path> shared_paths;
    for (size_t i = 0; i < num_shared_paths; ++i) {
        shared_paths.push_back(base_test_dir / ("shared_db_" + std::to_string(i)));
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    
    auto thread_worker = [&](size_t thread_id) -> bool {
        for (size_t i = 0; i < cycles_per_thread; ++i) {
            try {
                
                const auto& shared_path = shared_paths[i % num_shared_paths];
                {
                    auto db1 = Database::create_new(shared_path, 1);
                    if (db1) {
                         uint32_t col_idx = db1->get_collection("test_col");
                    }
                    
                }

                
                auto unique_path = base_test_dir / ("thread_" + std::to_string(thread_id) + "_db_" + std::to_string(i));
                {
                    auto db2 = Database::create_new(unique_path, 1);
                    
                }

                
                {
                    auto db3 = Database::open_existing(shared_path, 1);
                    if (db3) {
                        uint32_t col_idx = db3->get_collection("test_col");
                    }
                    
                }

            } catch (const std::exception& e) {
                std::cerr << "!!! Thread " << thread_id << ", cycle " << i << " caught an exception: " << e.what() << std::endl;
                return false; 
            }
        }
        return true; 
    };

    
    std::vector<std::future<bool>> futures;
    std::cout << "Launching " << num_threads << " threads, each running " << cycles_per_thread << " cycles..." << std::endl;
    for (size_t i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, thread_worker, i));
    }

    for (auto& f : futures) {
        if (!f.get()) {
            test_passed = false; 
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    std::cout << "\n--- Test Summary ---" << std::endl;
    std::cout << "Total cycles executed: " << (num_threads * cycles_per_thread) << std::endl;
    std::cout << "Total execution time: " << std::fixed << std::setprecision(3) << duration_ms << " ms" << std::endl;

    if (test_passed) {
        std::cout << "CONCURRENT INIT & CLOSE STRESS TEST PASSED!" << std::endl;
    } else {
        std::cout << "CONCURRENT INIT & CLOSE STRESS TEST FAILED! See exceptions above." << std::endl;
        throw std::runtime_error("Concurrent init/close test failed.");
    }

    
    if (std::filesystem::exists(base_test_dir)) {
        try {
            std::filesystem::remove_all(base_test_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove test directory after completion: " << e.what() << std::endl;
        }
    }
}

} 