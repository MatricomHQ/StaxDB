#include "stax_core/stax_new_tree.hpp"
#include <iostream>
#include <optional>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
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
    StaxRecord* st_get(std::string_view key) {
        TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
        return tree_->get(ctx, key);
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
        file_header_->collection_array_offset = 0;
        global_allocator_ = std::make_unique<StaxAllocator>(file_header_, static_cast<uint8_t*>(mmap_ptr_));
        st_local_allocator_ = std::make_unique<ThreadLocalAllocator>(*global_allocator_);
        tree_ = std::make_unique<StaxTree16>(*global_allocator_, reinterpret_cast<std::atomic<uint64_t>&>(file_header_->collection_array_offset));
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
    StaxRecord* st_get(std::string_view key) {
        TxnContext ctx = {0, std::numeric_limits<uint64_t>::max(), 1};
        return tree_->get(ctx, key);
    }
};
#endif

// =================================================================================================
// --- Transparent Comparators for std::unordered_map ---
// =================================================================================================
struct StringViewHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view sv) const {
        return std::hash<std::string_view>{}(sv);
    }
};
struct StringViewEqual {
    using is_transparent = void;
    bool operator()(std::string_view lhs, std::string_view rhs) const {
        return lhs == rhs;
    }
};

// =================================================================================================
// --- LEX BENCHMARKS ---
// =================================================================================================

std::map<std::string, long long> benchmark_results;

#if defined(__GNUC__) || defined(__clang__)
#define BSWAP64(x) __builtin_bswap64(x)
#else
#define BSWAP64(x) _byteswap_uint64(x)
#endif

void make_lex_key(uint64_t val, char* key_buf, size_t key_bytes) {
    uint64_t n = BSWAP64(val);
    memcpy(key_buf, &n, std::min(sizeof(n), key_bytes));
    if (key_bytes > sizeof(n)) {
        memset(key_buf + sizeof(n), 0, key_bytes - sizeof(n));
    }
}

void run_lex_benchmark(size_t key_bytes) {
    constexpr bool is_random = true;
    constexpr size_t NUM_OPS = 1000000;

    std::string name = "Lexicographical " + std::to_string(key_bytes) + "-byte";

    std::vector<uint64_t> keys(NUM_OPS);
    for(size_t i=0; i<NUM_OPS; ++i) keys[i] = i;
    if (is_random) {
        std::mt19937 g(123);
        std::shuffle(keys.begin(), keys.end(), g);
    }

    // --- StaxTree Insert ---
    FractalTreeWrapper tree;
    std::vector<char> key_buf(key_bytes);
    auto start_insert = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key(keys[i], key_buf.data(), key_bytes);
        tree.st_insert(std::string_view(key_buf.data(), key_bytes), "v");
    }
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto dur_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insert - start_insert);
    long long ns_per_op_insert = dur_insert.count() / NUM_OPS;
    std::string bench_name_insert = name + " Insert (Random)";
    std::cout << std::left << std::setw(45) << bench_name_insert << ": " << std::right << std::setw(10) << ns_per_op_insert << " ns/op" << std::endl;
    benchmark_results[bench_name_insert] = ns_per_op_insert;

    // --- StaxTree Get ---
    auto start_get = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key(keys[i], key_buf.data(), key_bytes);
        tree.st_get(std::string_view(key_buf.data(), key_bytes));
    }
    auto end_get = std::chrono::high_resolution_clock::now();
    auto dur_get = std::chrono::duration_cast<std::chrono::nanoseconds>(end_get - start_get);
    long long ns_per_op_get = dur_get.count() / NUM_OPS;
    std::string bench_name_get = name + " Get (Random)";
    std::cout << std::left << std::setw(45) << bench_name_get << ": " << std::right << std::setw(10) << ns_per_op_get << " ns/op" << std::endl;
    benchmark_results[bench_name_get] = ns_per_op_get;
}

void run_unordered_map_benchmark(size_t key_bytes) {
    constexpr bool is_random = true;
    constexpr size_t NUM_OPS = 1000000;
    std::string name = "Unordered_Map " + std::to_string(key_bytes) + "-byte";

    std::unordered_map<std::string, std::string, StringViewHash, StringViewEqual> umap;
    std::vector<uint64_t> keys(NUM_OPS);
    for(size_t i=0; i<NUM_OPS; ++i) keys[i] = i;
    if(is_random) {
        std::mt19937 g(123);
        std::shuffle(keys.begin(), keys.end(), g);
    }
    std::vector<char> key_buf(key_bytes);

    // --- Unordered Map Insert ---
    auto start_insert = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key(keys[i], key_buf.data(), key_bytes);
        umap.emplace(std::string(key_buf.data(), key_bytes), "v");
    }
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto dur_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insert - start_insert);
    std::string bench_name_insert = name + " Insert (Random)";
    long long ns_per_op_insert = dur_insert.count()/NUM_OPS;
    benchmark_results[bench_name_insert] = ns_per_op_insert;


    // --- Unordered Map Get ---
    auto start_get = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key(keys[i], key_buf.data(), key_bytes);
        if(umap.find(std::string_view(key_buf.data(), key_bytes)) == umap.end()) { /* error */ }
    }
    auto end_get = std::chrono::high_resolution_clock::now();
    auto dur_get = std::chrono::duration_cast<std::chrono::nanoseconds>(end_get - start_get);
    std::string bench_name_get = name + " Get (Random)";
    long long ns_per_op_get = dur_get.count()/NUM_OPS;
    benchmark_results[bench_name_get] = ns_per_op_get;
}

void run_all_lex_benchmarks() {
    std::cout << "\n--- Performance Benchmarks (ns/op) ---" << std::endl;
    run_lex_benchmark(3);
    run_lex_benchmark(4);
    run_lex_benchmark(7);
    run_lex_benchmark(8);
    run_lex_benchmark(16);
    run_lex_benchmark(17);
    run_lex_benchmark(32);
    run_lex_benchmark(35);

    // Run corresponding unordered_map benchmarks to populate results for comparison table
    run_unordered_map_benchmark(3);
    run_unordered_map_benchmark(4);
    run_unordered_map_benchmark(7);
    run_unordered_map_benchmark(8);
    run_unordered_map_benchmark(16);
    run_unordered_map_benchmark(17);
    run_unordered_map_benchmark(32);
    run_unordered_map_benchmark(35);
}

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
void verify_tree_contents(StaxTree16* tree, const std::vector<std::string_view>& expected_keys_sorted) {
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

void run_latency_test_for_keyset(
    const std::string& name,
    const std::vector<std::string_view>& keys,
    const std::vector<std::string_view>& shuffled_keys)
{
    const int total_items = keys.size();
    std::cout << "\n" << std::string(60, '-') << std::endl;
    std::cout << "--- Latency Test: " << name << " (" << total_items << " items) ---" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    // --- Prepare Miss Keys ---
    size_t miss_keys_total_size = 0;
    for (const auto& key : keys) {
        miss_keys_total_size += (5 + key.size());
    }
    std::vector<char> miss_keys_buffer;
    miss_keys_buffer.reserve(miss_keys_total_size);
    std::vector<std::string_view> miss_keys;
    miss_keys.reserve(total_items);
    for (const auto& key : keys) {
        const char prefix[] = "miss_";
        size_t current_pos = miss_keys_buffer.size();
        miss_keys_buffer.insert(miss_keys_buffer.end(), prefix, prefix + 5);
        miss_keys_buffer.insert(miss_keys_buffer.end(), key.begin(), key.end());
        miss_keys.emplace_back(miss_keys_buffer.data() + current_pos, 5 + key.size());
    }

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
            StaxRecord* rec = tree.get_tree()->get(ctx, key); do_not_optimize(rec); assert(rec != nullptr);
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Hit) Latency" << ": " << avg_ns << " ns/op" << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : miss_keys) {
            StaxRecord* rec = tree.get_tree()->get(ctx, key); do_not_optimize(rec); assert(rec == nullptr);
        }
        end_time = std::chrono::high_resolution_clock::now();
        total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
        avg_ns = total_duration.count() / total_items;
        std::cout << std::left << std::setw(30) << "Avg. Get (Miss) Latency" << ": " << avg_ns << " ns/op" << std::endl;
    }
    {
        std::cout << "\n[std::map]" << std::endl;
        std::map<std::string, std::string, std::less<>> stl_map;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) stl_map.emplace(key, "v");
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
        std::unordered_map<std::string, std::string, StringViewHash, StringViewEqual> stl_umap;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : shuffled_keys) stl_umap.emplace(key, "v");
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
        run_all_lex_benchmarks();
        run_range_scan_correctness_tests();
        run_range_scan_benchmarks();
        run_in_depth_latency_benchmarks();
    } catch (const std::exception& e) {
        std::cerr << "\n\n*** An error occurred: " << e.what() << " ***" << std::endl;
        exit(1);
    }
}

int main() {
    run_all_benchmarks();
    std::cout << "\n\n--- Lexicographical vs. Unordered_Map Comparison ---" << std::endl;
    std::cout << std::string(110, '-') << std::endl;
    std::cout << std::left << std::setw(45) << "Benchmark"
              << std::setw(20) << "StaxTree (ns/op)"
              << std::setw(25) << "Unordered_Map (ns/op)"
              << std::setw(20) << "Faster By" << std::endl;
    std::cout << std::string(110, '-') << std::endl;

    for (auto const& [key, stax_time] : benchmark_results) {
        if (key.find("Lexicographical") != std::string::npos) {
            std::string umap_key = key;
            umap_key.replace(0, 15, "Unordered_Map");

            if (benchmark_results.count(umap_key)) {
                long long umap_time = benchmark_results[umap_key];
                std::cout << std::left << std::setw(45) << key;
                std::cout << std::setw(20) << stax_time;
                std::cout << std::setw(25) << umap_time;

                if (stax_time < umap_time) {
                    double factor = (double)umap_time / stax_time;
                    std::cout << "StaxTree by " << std::fixed << std::setprecision(2) << factor << "x" << std::endl;
                } else {
                    double factor = (double)stax_time / umap_time;
                    std::cout << "Unordered_Map by " << std::fixed << std::setprecision(2) << factor << "x" << std::endl;
                }
            }
        }
    }
    std::cout << std::string(110, '-') << std::endl;

    std::cout << "\nBenchmarks completed." << std::endl;
    return 0;
}


void run_in_depth_latency_benchmarks() {
    std::mt19937_64 rng(1337);
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "--- In-Depth Point Operation Latency Benchmarks (vs STL) ---" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    auto run_test = [&](const std::string& name, size_t key_size, auto key_generator) {
        size_t total_buffer_size = total_items * key_size;
        std::vector<char> key_buffer(total_buffer_size);
        std::vector<std::string_view> keys;
        keys.reserve(total_items);

        for (int i = 0; i < total_items; ++i) {
            char* key_ptr = key_buffer.data() + i * key_size;
            key_generator(i, key_ptr, key_size);
            keys.emplace_back(key_ptr, key_size);
        }

        std::vector<std::string_view> shuffled_keys = keys;
        std::shuffle(shuffled_keys.begin(), shuffled_keys.end(), rng);

        // Sort the original string_view vector for verification
        std::sort(keys.begin(), keys.end());

        run_latency_test_for_keyset(name, keys, shuffled_keys);
    };

    // --- Short Sequential Keys ---
    run_test("Short Sequential Keys (8 bytes)", 8,
        [](uint64_t i, char* buf, size_t size) {
            memcpy(buf, &i, sizeof(i));
        });

    // --- Long Sequential Keys ---
    run_test("Long Sequential Keys (128 bytes)", 128,
        [](int i, char* buf, size_t size) {
            int len = snprintf(buf, size, "lonsequential_key_%0100d", i);
            // This is unsafe if len >= size, but we control the buffer size.
        });

    // --- Hash-like Keys ---
    std::mt19937_64 key_rng(1337);
    run_test("Hash-like Keys (64 bytes)", 64,
        [&](int i, char* buf, size_t size) {
            for(size_t j=0; j < size / sizeof(uint64_t); ++j) {
                reinterpret_cast<uint64_t*>(buf)[j] = key_rng();
            }
        });
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

    // --- Key Generation ---
    const size_t key_size = 16;
    std::vector<char> key_buffer(total_items * key_size);
    std::vector<std::string_view> keys_sorted;
    keys_sorted.reserve(total_items);
    for (int i = 0; i < total_items; ++i) {
        char* key_ptr = key_buffer.data() + i * key_size;
        snprintf(key_ptr, key_size, "user%07d", i);
        keys_sorted.emplace_back(key_ptr, strlen(key_ptr));
    }

    std::vector<std::string_view> keys_shuffled = keys_sorted;
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
        std::map<std::string, std::string, std::less<>> stl_map;
        std::cout << "[SETUP] Inserting " << total_items << " random-order keys into std::map..." << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (const auto& key : keys_shuffled) stl_map.emplace(key, "v_rand");
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
                const auto& start_key = keys_sorted[start_idx];
                const auto& end_key = keys_sorted[start_idx + profile.range_size - 1];
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
