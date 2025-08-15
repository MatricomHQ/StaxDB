#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <chrono>
#include <random>
#include <algorithm>
#include <memory>
#include <map>
#include <stdexcept>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <stack>
#include <limits>
#include <new>
#include <functional>
#include <iomanip>
#include <iterator>
#include <optional>
#include <array>
#include <thread>
#include <set>
#include <filesystem>
#include <mutex>

#include "stax_db/db.h"
#include "stax_core/stax_tree.hpp"
#include "stax_tx/transaction.h"
#include "stax_common/constants.h"
#include "tests/common_test_utils.h"

namespace RangeScanBench {

// Utility to convert uint64_t to a big-endian string for lexicographical ordering
inline std::string uint64_to_big_endian_str(uint64_t val) {
    std::string s(8, '\0');
    s[0] = (val >> 56) & 0xFF;
    s[1] = (val >> 48) & 0xFF;
    s[2] = (val >> 40) & 0xFF;
    s[3] = (val >> 32) & 0xFF;
    s[4] = (val >> 24) & 0xFF;
    s[5] = (val >> 16) & 0xFF;
    s[6] = (val >> 8) & 0xFF;
    s[7] = val & 0xFF;
    return s;
}

// Helper function to convert big-endian string back to uint64_t
inline uint64_t big_endian_str_to_uint64(std::string_view s) {
    if (s.length() != 8) {
        return std::numeric_limits<uint64_t>::max();
    }
    uint64_t val = 0;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[0])) << 56;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[1])) << 48;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[2])) << 40;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[3])) << 32;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[4])) << 24;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[5])) << 16;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[6])) << 8;
    val |= static_cast<uint64_t>(static_cast<uint8_t>(s[7]));
    return val;
}

inline void generate_url_like_keys(std::vector<std::string>& keys, size_t count) {
    keys.clear();
    keys.reserve(count);
    std::mt19937 g(42); // Fixed seed for reproducibility

    std::vector<std::string> domains = {"com", "org", "net", "io", "ai", "co.uk"};
    std::vector<std::string> tlds = {"example", "test", "benchmark", "stax", "phtrie", "database"};
    std::vector<std::string> paths = {"/home", "/index.html", "/api/v1/data", "/assets/img.png", "/users/profile", "/about"};

    std::uniform_int_distribution<> domain_dist(0, (int)domains.size() - 1);
    std::uniform_int_distribution<> tld_dist(0, (int)tlds.size() - 1);
    std::uniform_int_distribution<> path_dist(0, (int)paths.size() - 1);

    std::set<std::string> unique_keys;
    while(unique_keys.size() < count) {
        std::string key = "https://" + tlds[tld_dist(g)] + "." + domains[domain_dist(g)] + paths[path_dist(g)] + "/" + std::to_string(unique_keys.size());
        unique_keys.insert(key);
    }
    keys.assign(unique_keys.begin(), unique_keys.end());
}

inline void run_prefix_range_benchmark() {
    std::cout << "\n" << std::string(80, '-') << std::endl;
    std::cout << "--- StaxTree Concurrent Prefix Range Scan Benchmark ---" << std::endl;

    const int NUM_RANGE_KEYS = 500000;
    const int NUM_WORKER_THREADS = MAX_CONCURRENT_THREADS;
    std::cout << "Items: " << NUM_RANGE_KEYS << ", Threads: " << NUM_WORKER_THREADS << std::endl;

    std::filesystem::path db_base_dir = "./db_data_prefix_bench";
    if (std::filesystem::exists(db_base_dir)) {
        std::filesystem::remove_all(db_base_dir);
    }
    std::filesystem::create_directories(db_base_dir);

    auto db = Database::create_new(db_base_dir, NUM_WORKER_THREADS);
    uint32_t col_idx = db->get_collection("prefix_range_test");
    Collection& col = db->get_collection_by_idx(col_idx);
    StaxTree& tree = col.get_critbit_tree();

    std::vector<std::pair<std::string, std::string>> all_kvs;
    std::vector<std::string> keys;
    generate_url_like_keys(keys, NUM_RANGE_KEYS);
    all_kvs.reserve(NUM_RANGE_KEYS);
    for(size_t i = 0; i < keys.size(); ++i) {
        all_kvs.push_back({keys[i], "v" + std::to_string(i)});
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<std::pair<std::string, std::string>>> thread_kvs(NUM_WORKER_THREADS);
    for(int i=0; i < NUM_RANGE_KEYS; ++i) {
        thread_kvs[i % NUM_WORKER_THREADS].push_back(all_kvs[i]);
    }

    std::cout << "Inserting data concurrently..." << std::endl;
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        threads.emplace_back([&, i]() {
            TxnContext ctx = { (uint64_t)i, (uint64_t)i, (TxnID)(i + 1) };
            for (const auto& kv : thread_kvs[i]) {
                tree.insert(db->get_thread_local_allocator(i), ctx, kv.first, kv.second);
            }
        });
    }
    for (auto& t : threads) { t.join(); }
    std::cout << "Data insertion complete." << std::endl;

    std::vector<std::string> prefixes = {
        "https://example.com/api",
        "https://stax.net/users",
        "https://benchmark.io",
        "https://test.co.uk/home",
        "https://database.ai/assets",
        "https://non-existent-prefix"
    };

    std::atomic<long long> total_found_concurrent = 0;
    std::atomic<bool> verification_passed = true;
    std::mutex error_mutex;

    std::map<std::string, size_t> ground_truth_counts;
    for(const auto& prefix : prefixes) {
        size_t count = 0;
        for(const auto& kv : all_kvs) {
            if (kv.first.rfind(prefix, 0) == 0) {
                count++;
            }
        }
        ground_truth_counts[prefix] = count;
        std::cout << "Ground truth for '" << prefix << "': " << count << " items" << std::endl;
    }

    std::cout << "Starting concurrent range scans..." << std::endl;
    threads.clear();
    auto start = std::chrono::high_resolution_clock::now();
    TxnContext scan_ctx = { 0, std::numeric_limits<uint64_t>::max(), (TxnID)(NUM_WORKER_THREADS + 1) };

    for (const auto& prefix : prefixes) {
        threads.emplace_back([&, prefix]() {
            size_t found_count = 0;
            for (const auto& record : tree.range(scan_ctx, prefix)) {
                found_count++;
            }
            total_found_concurrent += found_count;
            if (found_count != ground_truth_counts.at(prefix)) {
                verification_passed = false;
                std::lock_guard<std::mutex> lock(error_mutex);
                std::cerr << "Verification FAILED for prefix '" << prefix << "'. Expected "
                          << ground_truth_counts.at(prefix) << ", got " << found_count << std::endl;
            }
        });
    }

    for (auto& t : threads) { t.join(); }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Concurrent scans finished in " << duration.count() << " ms." << std::endl;
    std::cout << "Total items found across all scans: " << total_found_concurrent << std::endl;

    if (verification_passed) {
        std::cout << "Verification: PASS" << std::endl;
    } else {
        std::cout << "Verification: FAIL" << std::endl;
        throw std::runtime_error("Concurrent prefix range scan verification failed!");
    }

    db.reset();
    std::filesystem::remove_all(db_base_dir);
}


inline void run_numerical_range_benchmark() {
    std::cout << "\n" << std::string(80, '-') << std::endl;
    std::cout << "--- StaxTree Numerical Range Scan Benchmark ---" << std::endl;

    const int NUM_ITEMS = 500000;
    const int NUM_WORKER_THREADS = MAX_CONCURRENT_THREADS;
    const std::string key_prefix = "sensor_data_";
    const uint64_t start_time = 1704067200; // Approx Jan 1, 2024
    const uint64_t time_range = 31536000; // One year in seconds

    std::cout << "Items: " << NUM_ITEMS << ", Threads: " << NUM_WORKER_THREADS << std::endl;

    std::filesystem::path db_base_dir = "./db_data_numerical_bench";
    if (std::filesystem::exists(db_base_dir)) {
        std::filesystem::remove_all(db_base_dir);
    }
    std::filesystem::create_directories(db_base_dir);

    auto db = Database::create_new(db_base_dir, NUM_WORKER_THREADS);
    uint32_t col_idx = db->get_collection("numerical_range_test");
    Collection& col = db->get_collection_by_idx(col_idx);
    StaxTree& tree = col.get_critbit_tree();

    std::vector<std::pair<std::string, std::string>> all_kvs;
    std::map<std::string, std::string> unique_kvs;
    all_kvs.reserve(NUM_ITEMS);
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<uint64_t> time_dist(0, time_range);

    std::cout << "Generating " << NUM_ITEMS << " unique composite keys (prefix + timestamp)..." << std::endl;
    while (unique_kvs.size() < NUM_ITEMS) {
        uint64_t ts = start_time + time_dist(rng);
        std::string key = key_prefix + uint64_to_big_endian_str(ts);
        unique_kvs[key] = "val" + std::to_string(unique_kvs.size());
    }

    for (const auto& kv : unique_kvs) {
        all_kvs.push_back(kv);
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<std::pair<std::string, std::string>>> thread_kvs(NUM_WORKER_THREADS);
    for(int i=0; i < NUM_ITEMS; ++i) {
        thread_kvs[i % NUM_WORKER_THREADS].push_back(all_kvs[i]);
    }

    std::cout << "Inserting data concurrently..." << std::endl;
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        threads.emplace_back([&, i]() {
            TxnContext ctx = { (uint64_t)i, (uint64_t)i, (TxnID)(i + 1) };
            for (const auto& kv : thread_kvs[i]) {
                tree.insert(db->get_thread_local_allocator(i), ctx, kv.first, kv.second);
            }
        });
    }
    for (auto& t : threads) { t.join(); }
    std::cout << "Data insertion complete." << std::endl;

    std::vector<std::pair<uint64_t, uint64_t>> test_ranges;
    uint64_t one_day = 86400;
    test_ranges.push_back({start_time, start_time + one_day});
    test_ranges.push_back({start_time + (time_range / 2), start_time + (time_range / 2) + (one_day * 7)});
    test_ranges.push_back({start_time + time_range - one_day, start_time + time_range});
    test_ranges.push_back({0, 1}); // A range likely to be empty

    std::vector<size_t> ground_truth_counts;
    std::cout << "\nCalculating ground truth for test ranges..." << std::endl;
    for (const auto& range : test_ranges) {
        size_t count = 0;
        for (const auto& kv : all_kvs) {
            if (kv.first.rfind(key_prefix, 0) == 0) {
                std::string_view ts_sv = std::string_view(kv.first).substr(key_prefix.length());
                if (ts_sv.length() == 8) {
                    uint64_t ts = big_endian_str_to_uint64(ts_sv);
                     if (ts >= range.first && ts <= range.second) {
                        count++;
                    }
                }
            }
        }
        ground_truth_counts.push_back(count);
        std::cout << "Ground truth for range [" << range.first << " - " << range.second << "]: " << count << " items" << std::endl;
    }

    std::cout << "\nStarting concurrent numerical range scans..." << std::endl;
    threads.clear();
    std::atomic<long long> total_found_concurrent = 0;
    std::atomic<bool> verification_passed = true;
    std::mutex error_mutex;
    TxnContext scan_ctx = { 0, std::numeric_limits<uint64_t>::max(), (TxnID)(NUM_WORKER_THREADS + 1) };

    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < test_ranges.size(); ++i) {
        threads.emplace_back([&, i]() {
            const auto& range = test_ranges[i];
            size_t found_count = 0;
            for (const auto& record : tree.range(scan_ctx, key_prefix, range.first, range.second)) {
                found_count++;
            }
            total_found_concurrent += found_count;
            if (found_count != ground_truth_counts[i]) {
                verification_passed = false;
                std::lock_guard<std::mutex> lock(error_mutex);
                std::cerr << "Verification FAILED for range [" << range.first << " - " << range.second
                          << "]. Expected " << ground_truth_counts[i] << ", got " << found_count << std::endl;
            }
        });
    }

    for (auto& t : threads) { t.join(); }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Concurrent scans finished in " << duration.count() << " ms." << std::endl;
    std::cout << "Total items found across all scans: " << total_found_concurrent << std::endl;

    if (verification_passed) {
        std::cout << "Verification: PASS" << std::endl;
    } else {
        std::cout << "Verification: FAIL" << std::endl;
        throw std::runtime_error("Concurrent numerical range scan verification failed!");
    }

    db.reset();
    std::filesystem::remove_all(db_base_dir);
}

} // namespace RangeScanBench
