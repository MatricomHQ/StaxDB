#include "stax_internal.h"
#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <atomic>
#include <chrono>
#include <random>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdexcept>
#include <limits>
#include <utility>
#include <cstring>
#include <iomanip>
#include <map>
#include <unordered_map>
#include <cassert>
#include <unistd.h>
#include <future>

#if defined(WASM_BUILD)
const int total_items = 1000000;
#else
const int total_items = 1000000;
#endif

// =================================================================================================
// --- FractalTreeWrapper for Benchmark & Tests ---
// =================================================================================================
#if defined(WASM_BUILD)
class FractalTreeWrapper {
private:
    std::vector<uint8_t> memory_buffer_;
    FileHeader* file_header_;
    std::unique_ptr<StaxAllocator> global_allocator_;
    std::unique_ptr<ThreadLocalAllocator> st_local_allocator_;
    std::unique_ptr<StaxTree16> tree_;
public:
    FractalTreeWrapper() : memory_buffer_(DB_MAX_VIRTUAL_SIZE) {
        file_header_ = new (memory_buffer_.data()) FileHeader();
        file_header_->global_alloc_offset.store(sizeof(FileHeader), std::memory_order_relaxed);
        file_header_->root_ptr.store(0, std::memory_order_relaxed);
        global_allocator_ = std::make_unique<StaxAllocator>(file_header_, memory_buffer_.data());
        st_local_allocator_ = std::make_unique<ThreadLocalAllocator>(*global_allocator_);
        tree_ = std::make_unique<StaxTree16>(*global_allocator_, file_header_->root_ptr);
    }
    ~FractalTreeWrapper() {}
    StaxTree16* get_tree() { return tree_.get(); }
    void st_insert(std::string_view key, std::string_view value) {
        TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
        tree_->insert(*st_local_allocator_, ctx, key, value, false);
    }
};
#else
class FractalTreeWrapper {
private:
    int fd_ = -1; void* mmap_ptr_ = nullptr; size_t mmap_size_; std::string mmap_filepath_;
    FileHeader* file_header_; std::unique_ptr<StaxAllocator> global_allocator_;
    std::unique_ptr<ThreadLocalAllocator> st_local_allocator_; std::unique_ptr<StaxTree16> tree_;
public:
    FractalTreeWrapper() : mmap_size_(DB_MAX_VIRTUAL_SIZE) {
        mmap_filepath_ = "/tmp/stax_tree_bench_" + std::to_string(getpid()) + ".db";
        fd_ = open(mmap_filepath_.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd_ == -1) throw std::runtime_error("Could not open mmap file");
        if (ftruncate(fd_, mmap_size_) == -1) throw std::runtime_error("Could not truncate file");
        mmap_ptr_ = mmap(nullptr, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mmap_ptr_ == MAP_FAILED) throw std::runtime_error("mmap failed");
        file_header_ = new (mmap_ptr_) FileHeader();
        file_header_->global_alloc_offset.store(sizeof(FileHeader), std::memory_order_relaxed);
        file_header_->root_ptr.store(0, std::memory_order_relaxed);
        global_allocator_ = std::make_unique<StaxAllocator>(file_header_, static_cast<uint8_t*>(mmap_ptr_));
        st_local_allocator_ = std::make_unique<ThreadLocalAllocator>(*global_allocator_);
        tree_ = std::make_unique<StaxTree16>(*global_allocator_, file_header_->root_ptr);
    }
    ~FractalTreeWrapper() {
        if (mmap_ptr_ != MAP_FAILED && mmap_ptr_ != nullptr) munmap(mmap_ptr_, mmap_size_);
        if (fd_ != -1) close(fd_);
        unlink(mmap_filepath_.c_str());
    }
    StaxTree16* get_tree() { return tree_.get(); }
    void st_insert(std::string_view key, std::string_view value) {
        TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
        tree_->insert(*st_local_allocator_, ctx, key, value, false);
    }
};
#endif

// =================================================================================================
// --- RIGOROUS RANGE SCAN CORRECTNESS TESTS (NO SORTING) ---
// =================================================================================================
void verify_scan_results_in_order(const std::vector<StaxRecord*>& results, const std::vector<std::string>& expected_keys_sorted) {
    if (results.size() != expected_keys_sorted.size()) {
        std::cerr << "Test failed: result size mismatch. Expected " << expected_keys_sorted.size() << ", got " << results.size() << std::endl;
        std::cerr << "Expected: "; for(const auto& k : expected_keys_sorted) std::cerr << k << ", ";
        std::cerr << "\nGot: "; for(const auto& r : results) std::cerr << r->get_key() << ", "; std::cerr << std::endl;
        assert(false);
    }
    for(size_t i = 0; i < results.size(); ++i) {
        if (results[i]->get_key() != expected_keys_sorted[i]) {
            std::cerr << "Test failed: key mismatch or order error at index " << i << ". Expected " << expected_keys_sorted[i] << ", got " << results[i]->get_key() << std::endl;
            assert(false);
        }
    }
}
void run_range_scan_correctness_tests() {
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "--- Running Rigorous Range Scan Correctness Tests ---" << std::endl;
    {
        FractalTreeWrapper tree; TxnContext ctx = {0, 1, 1}; std::vector<StaxRecord*> results;
        tree.get_tree()->range_scan(ctx, "a", "b", results); assert(results.empty());
        std::cout << "Test 1 (Empty Tree): PASSED" << std::endl;
    }
    FractalTreeWrapper tree; TxnContext ctx = {0, 1, 1};
    std::vector<std::string> keys = {"d", "a", "c", "e", "bb", "ba", "b", "bc"};
    for(const auto& k : keys) tree.st_insert(k, "v");
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "", "z", results);
        verify_scan_results_in_order(results, {"a", "b", "ba", "bb", "bc", "c", "d", "e"});
        std::cout << "Test 2 (Full Scan): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "b", "d", results);
        verify_scan_results_in_order(results, {"b", "ba", "bb", "bc", "c", "d"});
        std::cout << "Test 3 (Middle Subset): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "ax", "c", results);
        verify_scan_results_in_order(results, {"b", "ba", "bb", "bc", "c"});
        std::cout << "Test 4 (Non-existent Start): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "bb", "cz", results);
        verify_scan_results_in_order(results, {"bb", "bc", "c"});
        std::cout << "Test 5 (Non-existent End): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "ax", "cz", results);
        verify_scan_results_in_order(results, {"b", "ba", "bb", "bc", "c"});
        std::cout << "Test 6 (Non-existent Both): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "bb", "bb", results);
        verify_scan_results_in_order(results, {"bb"});
        std::cout << "Test 7 (Single Item): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "c", "b", results);
        assert(results.empty());
        std::cout << "Test 8 (Empty Range): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "b", "bb", results);
        verify_scan_results_in_order(results, {"b", "ba", "bb"});
        std::cout << "Test 9 (Prefix Keys): PASSED" << std::endl;
    }
    {
        std::vector<StaxRecord*> results; tree.get_tree()->range_scan(ctx, "ca", "cb", results);
        assert(results.empty());
        std::cout << "Test 10 (No Results): PASSED" << std::endl;
    }
    {
        FractalTreeWrapper deep_tree; deep_tree.st_insert("apple/banana/cantaloupe", "v");
        deep_tree.st_insert("apple/banana/date", "v"); deep_tree.st_insert("apple/blueberry/date", "v");
        deep_tree.st_insert("apricot/blueberry/date", "v");
        std::vector<StaxRecord*> results; deep_tree.get_tree()->range_scan(ctx, "apple/banana/c", "apple/banana/e", results);
        verify_scan_results_in_order(results, {"apple/banana/cantaloupe", "apple/banana/date"});
        std::cout << "Test 11 (Deeply Nested Scan): PASSED" << std::endl;
    }
    std::cout << "--- All Range Scan Correctness Tests Passed ---" << std::endl;
}

// =================================================================================================
// --- BENCHMARK HARNESS ---
// =================================================================================================
template <typename T>
void do_not_optimize(T const& value) {
    asm volatile("" : : "r,m"(value) : "memory");
}
void verify_tree_contents(StaxTree16* tree, const std::vector<std::string>& expected_keys_sorted) {
    std::cout << "[VERIFY] Performing full scan to check " << expected_keys_sorted.size() << " items..." << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();
    TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
    std::vector<StaxRecord*> results;
    tree->range_scan(ctx, "", std::string(256, '\xFF'), results);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::cout << "[VERIFY] Scan found " << results.size() << " items in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count()
              << " ms." << std::endl;
    if (results.size() != expected_keys_sorted.size()) {
        std::cerr << "!!! VERIFICATION FAILED: Size mismatch. Expected: " << expected_keys_sorted.size()
                  << ", Got: " << results.size() << std::endl;
        assert(false);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i]->get_key() != expected_keys_sorted[i]) {
            std::cerr << "!!! VERIFICATION FAILED: Mismatch or order error at index " << i
                      << ". Expected: " << expected_keys_sorted[i]
                      << ", Got: " << results[i]->get_key() << std::endl;
            assert(false);
        }
    }
    std::cout << "[VERIFY] All " << results.size() << " items successfully verified." << std::endl;
}
void run_latency_test_for_keyset(const std::string& name, const std::vector<std::string>& keys, const std::vector<std::string>& shuffled_keys) {
    const int total_items = keys.size();
    std::cout << "\n" << std::string(60, '-') << std::endl;
    std::cout << "--- Latency Test: " << name << " (" << total_items << " items) ---" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    std::vector<std::string> miss_keys;
    miss_keys.reserve(total_items);
    for (const auto& key : keys) miss_keys.push_back("miss_" + key);
    {
        std::cout << "\n[StaxTree16]" << std::endl;
        FractalTreeWrapper tree; TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) tree.st_insert(key, "v");
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        long long avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Insert Latency" << ": " << avg_ns << " ns/op" << std::endl;
        verify_tree_contents(tree.get_tree(), keys);
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) {
            StaxRecord* rec = tree.get_tree()->get(ctx, key, key.length()); do_not_optimize(rec); assert(rec != nullptr);
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Hit) Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : miss_keys) {
            StaxRecord* rec = tree.get_tree()->get(ctx, key, key.length()); do_not_optimize(rec); assert(rec == nullptr);
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Miss) Latency" << ": " << avg_ns << " ns/op" << std::endl;
    }
    {
        std::cout << "\n[std::map]" << std::endl;
        std::map<std::string, std::string> stl_map;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) stl_map[key] = "v";
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        long long avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Insert Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) {
            auto it = stl_map.find(key); do_not_optimize(it); assert(it != stl_map.end());
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Hit) Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : miss_keys) {
            auto it = stl_map.find(key); do_not_optimize(it); assert(it == stl_map.end());
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Miss) Latency" << ": " << avg_ns << " ns/op" << std::endl;
    }
    {
        std::cout << "\n[std::unordered_map]" << std::endl;
        std::unordered_map<std::string, std::string> stl_umap;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) stl_umap[key] = "v";
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        long long avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Insert Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) {
            auto it = stl_umap.find(key); do_not_optimize(it); assert(it != stl_umap.end());
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Hit) Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : miss_keys) {
            auto it = stl_umap.find(key); do_not_optimize(it); assert(it == stl_umap.end());
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Miss) Latency" << ": " << avg_ns << " ns/op" << std::endl;
    }
}
void run_in_depth_latency_benchmarks();
void run_range_scan_benchmarks();

void run_all_benchmarks() {
    try {
        run_range_scan_correctness_tests();
        run_range_scan_benchmarks();
        run_in_depth_latency_benchmarks();
    } catch (const std::exception& e) {
        std::cerr << "\n\n*** An error occurred: " << e.what() << " ***" << std::endl;
        exit(1); // Exit from the thread
    }
}

int main() {
    run_all_benchmarks();
    std::cout << "\nBenchmarks completed." << std::endl;
    return 0;
}

void run_in_depth_latency_benchmarks() {
    std::mt19937_64 rng(1337);
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "--- In-Depth Point Operation Latency Benchmarks (vs STL) ---" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    {
        std::vector<std::string> keys; keys.reserve(total_items);
        for (uint64_t i = 0; i < total_items; ++i) keys.push_back(std::string(reinterpret_cast<const char*>(&i), sizeof(i)));
        std::vector<std::string> shuffled_keys = keys; std::shuffle(shuffled_keys.begin(), shuffled_keys.end(), rng);
        std::sort(keys.begin(), keys.end());
        run_latency_test_for_keyset("Short Sequential Keys (8 bytes)", keys, shuffled_keys);
    }
    {
        std::vector<std::string> keys; keys.reserve(total_items);
        for (int i = 0; i < total_items; ++i) {
            std::string key(128, '\0'); snprintf(&key[0], 128, "lonsequential_key_%0100d", i);
            key.resize(strlen(key.c_str())); keys.push_back(key);
        }
        std::vector<std::string> shuffled_keys = keys; std::shuffle(shuffled_keys.begin(), shuffled_keys.end(), rng);
        std::sort(keys.begin(), keys.end());
        run_latency_test_for_keyset("Long Sequential Keys (128 bytes)", keys, shuffled_keys);
    }
    {
        std::vector<std::string> keys; keys.reserve(total_items);
        std::mt19937_64 key_rng(1337);
        for (int i = 0; i < total_items; ++i) {
            char key_buf[64];
            for(int j=0; j<8; ++j) reinterpret_cast<uint64_t*>(key_buf)[j] = key_rng();
            keys.push_back(std::string(key_buf, 64));
        }
        std::vector<std::string> shuffled_keys = keys; std::shuffle(shuffled_keys.begin(), shuffled_keys.end(), rng);
        std::sort(keys.begin(), keys.end());
        run_latency_test_for_keyset("Hash-like Keys (64 bytes)", keys, shuffled_keys);
    }
}
void run_range_scan_benchmarks() {
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "--- Range Scan Benchmarks (vs std::map) ---" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    const int num_scans = 5000;
    struct ScanProfile { std::string name; int range_size; };
    std::vector<ScanProfile> profiles = {
        {"Small Scans (10 items) ", 10}, {"Medium Scans (100 items)", 100}, {"Large Scans (1000 items)", 1000}
    };
    TxnContext scan_ctx = {0, std::numeric_limits<uint64_t>::max(), 99};
    std::vector<std::string> keys_sorted; keys_sorted.reserve(total_items);
    for (int i = 0; i < total_items; ++i) {
        char key_buf[16]; snprintf(key_buf, sizeof(key_buf), "user%07d", i);
        keys_sorted.push_back(std::string(key_buf));
    }
    std::vector<std::string> keys_shuffled = keys_sorted;
    std::mt19937 rng(42);
    std::shuffle(keys_shuffled.begin(), keys_shuffled.end(), rng);
    {
        std::cout << "\n--- Structure: StaxTree16 ---" << std::endl;
        FractalTreeWrapper rand_tree;
        std::cout << "[SETUP] Inserting " << total_items << " random-order keys into StaxTree16..." << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();
        for(const auto& key : keys_shuffled) rand_tree.st_insert(key, "v_rand");
        auto end_time = std::chrono::high_resolution_clock::now();
        std::cout << "[SETUP] Insertion complete in " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms." << std::endl;
        verify_tree_contents(rand_tree.get_tree(), keys_sorted);
        std::cout << "[SETUP] Warming up memory by scanning all keys..." << std::endl;
        std::vector<StaxRecord*> scan_results; scan_results.reserve(total_items);
        rand_tree.get_tree()->range_scan(scan_ctx, keys_sorted.front(), keys_sorted.back(), scan_results);
        std::cout << "[SETUP] Warm-up complete. Found " << scan_results.size() << " items." << std::endl;
        for (const auto& profile : profiles) {
            start_time = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < num_scans; ++i) {
                int start_idx = (i * 31337) % (total_items - profile.range_size);
                scan_results.clear();
                rand_tree.get_tree()->range_scan(scan_ctx, keys_sorted[start_idx], keys_sorted[start_idx + profile.range_size - 1], scan_results);
                assert(scan_results.size() == static_cast<size_t>(profile.range_size));
            }
            end_time = std::chrono::high_resolution_clock::now();
            auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
            long long avg_ns = total_duration.count() / num_scans;
            std::cout << std::left << std::setw(30) << profile.name << ": " << avg_ns << " ns/op" << std::endl;
        }
    }
    {
        std::cout << "\n--- Structure: std::map ---" << std::endl;
        std::map<std::string, std::string> stl_map;
        std::cout << "[SETUP] Inserting " << total_items << " random-order keys into std::map..." << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : keys_shuffled) stl_map[key] = "v_rand";
        auto end_time = std::chrono::high_resolution_clock::now();
        std::cout << "[SETUP] Insertion complete in " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms." << std::endl;
        std::cout << "[SETUP] Warming up memory by scanning all keys..." << std::endl;
        volatile size_t count = 0;
        for(auto it = stl_map.begin(); it != stl_map.end(); ++it) count++;
        std::cout << "[SETUP] Warm-up complete. Found " << count << " items." << std::endl;
        for (const auto& profile : profiles) {
            start_time = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < num_scans; ++i) {
                int start_idx = (i * 31337) % (total_items - profile.range_size);
                const std::string& start_key = keys_sorted[start_idx];
                const std::string& end_key = keys_sorted[start_idx + profile.range_size - 1];
                size_t items_found = 0;
                auto it_start = stl_map.lower_bound(start_key);
                for(auto it = it_start; it != stl_map.end(); ++it) {
                    if(it->first > end_key) break;
                    items_found++;
                }
                do_not_optimize(items_found);
            }
            end_time = std::chrono::high_resolution_clock::now();
            auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
            long long avg_ns = total_duration.count() / num_scans;
            std::cout << std::left << std::setw(30) << profile.name << ": " << avg_ns << " ns/op" << std::endl;
        }
    }
}
