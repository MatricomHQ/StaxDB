// =================================================================================================
// 1. Standard Library and System Headers
// =================================================================================================
#include <iostream>
#include <filesystem>
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>
#include <map>
#include <numeric>
#include <thread>
#include <atomic>
#include <stdexcept>
#include <fstream>
#include <system_error>
#include <cmath>
#include <limits>
#include <functional>
#include <utility>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <unordered_map>
#include <unordered_set>


#include "stax_db/db.h"
#include "stax_db/query.h"
#include "stax_core/stax_new_tree.hpp"
#include "stax_core/dimensional.hpp"

// =================================================================================================
// 2. Global State & Configuration
// =================================================================================================

// --- Temporary Directory Management ---
std::filesystem::path g_temp_dir;

void recursive_delete(const std::filesystem::path& path) {
    std::error_code ec;
    if (std::filesystem::exists(path, ec) && !ec) {
        std::filesystem::remove_all(path, ec);
        if (ec) {
            std::cerr << "Warning: Failed to delete directory " << path << ": " << ec.message() << std::endl;
        }
    }
}


// Global error counter for capping error messages
std::atomic<int> g_error_count = 0;
const int MAX_ERRORS = 5;

void report_error(const std::string& msg) {
    int previous_errors = g_error_count.fetch_add(1, std::memory_order_relaxed);
    if (previous_errors < MAX_ERRORS) {
        std::cerr << msg << std::endl;
    } else if (previous_errors == MAX_ERRORS) {
        // On the 6th error (previous_errors == 5), print final message and exit
        std::cerr << "Maximum number of errors (" << MAX_ERRORS << ") exceeded. Terminating." << std::endl;
        exit(1);
    }
}


// Custom allocator for memory tracking
static std::atomic<size_t> g_tracked_memory_usage = 0;

template<typename T>
class TrackingAllocator {
public:
    using value_type = T;

    TrackingAllocator() = default;
    template<class U>
    constexpr TrackingAllocator(const TrackingAllocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
        const size_t bytes_to_alloc = n * sizeof(T);
        g_tracked_memory_usage.fetch_add(bytes_to_alloc, std::memory_order_relaxed);
        void* p = std::malloc(bytes_to_alloc);
        if (!p) { throw std::bad_alloc(); }
        return static_cast<T*>(p);
    }

    void deallocate(T* p, std::size_t n) noexcept {
        const size_t bytes_to_free = n * sizeof(T);
        g_tracked_memory_usage.fetch_sub(bytes_to_free, std::memory_order_relaxed);
        std::free(p);
    }
};

template<class T, class U>
bool operator==(const TrackingAllocator<T>&, const TrackingAllocator<U>&) { return true; }
template<class T, class U>
bool operator!=(const TrackingAllocator<T>&, const TrackingAllocator<U>&) { return false; }

struct LatencyStats {
    long long avg = 0;
    long long p50 = 0;
    long long p95 = 0;
    long long p99 = 0;
    double stddev = 0.0;

    static LatencyStats calculate(std::vector<long long>& timings) {
        if (timings.empty()) {
            return {};
        }
        std::sort(timings.begin(), timings.end());
        LatencyStats s;
        long long sum = 0;
        for(const auto& t : timings) sum += t;
        s.avg = sum / timings.size();
        s.p50 = timings[timings.size() * 0.50];
        s.p95 = timings[timings.size() * 0.95];
        s.p99 = timings[timings.size() * 0.99];

        double variance_sum = 0.0;
        for(long long timing : timings) {
            variance_sum += (static_cast<double>(timing) - s.avg) * (static_cast<double>(timing) - s.avg);
        }
        s.stddev = std::sqrt(variance_sum / timings.size());
        return s;
    }
};


// =================================================================================================
// 6. Benchmarks
// =================================================================================================

// Forward declarations for benchmark functions
void run_unordered_map_benchmark(size_t key_bytes, bool is_random);
void run_apk_generation_benchmark();

std::map<std::string, long long> benchmark_results;

#if defined(__GNUC__) || defined(__clang__)
#define BSWAP64(x) __builtin_bswap64(x)
#else
#define BSWAP64(x) _byteswap_uint64(x)
#endif
template <size_t N> void make_lex_key(uint64_t val, uint64_t* key_buf) {
    uint64_t n = BSWAP64(val);
    memcpy(key_buf, &n, sizeof(n));
    if (N > sizeof(n)) {
        memset(reinterpret_cast<char*>(key_buf) + sizeof(n), 0, N - sizeof(n));
    }
}


#if defined(__linux__)
#include <fstream>

void print_memory_usage(const std::string& stage_name) {
    std::ifstream status_file("/proc/self/status");
    std::string line;
    long vm_rss = 0;
    while (std::getline(status_file, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            try {
                std::string value_str = line.substr(line.find(':') + 1);
                value_str = value_str.substr(0, value_str.find("kB"));
                vm_rss = std::stol(value_str);
            } catch (...) {
                // Ignore parsing errors
            }
            break;
        }
    }
    if (vm_rss > 0) {
        double vm_rss_mb = vm_rss / 1024.0;
        std::cout << std::left << std::setw(45) << "Memory Usage (" + stage_name + ")" << ": "
                  << std::right << std::setw(9) << std::fixed << std::setprecision(2) << vm_rss_mb << " MB" << std::endl;
    }
}
#else
void print_memory_usage(const std::string&) { /* Not implemented */ }
#endif

void run_unordered_map_benchmark(size_t key_bytes, bool is_random) {
#ifdef __EMSCRIPTEN__
    constexpr size_t NUM_OPS = 1000000;
#else
    constexpr size_t NUM_OPS = 1000000;
#endif
    std::string name = "Unordered_Map " + std::to_string(key_bytes) + "-byte";
    std::unordered_map<std::string, std::string> umap;
    std::vector<uint64_t> keys(NUM_OPS);
    for(size_t i=0; i<NUM_OPS; ++i) keys[i] = i;
    if(is_random) {
        std::mt19937 g(123);
        std::shuffle(keys.begin(), keys.end(), g);
    }
    std::vector<uint64_t> key_buf_u64(key_bytes / sizeof(uint64_t) + (key_bytes % sizeof(uint64_t) != 0));
    auto start = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key<8>(keys[i], key_buf_u64.data());
        std::string key_str(reinterpret_cast<const char*>(key_buf_u64.data()), key_bytes);
        umap[key_str] = "v";
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::string bench_name_insert = name + " Insert ("+(is_random?"Random":"Sequential")+")";
    long long ns_per_op_insert = dur.count()/NUM_OPS;
    std::cout << std::left << std::setw(45) << bench_name_insert << ": " << std::right << std::setw(10) << ns_per_op_insert << " ns/op" << std::endl;
    benchmark_results[bench_name_insert] = ns_per_op_insert;

    start = std::chrono::high_resolution_clock::now();
    for(size_t i=0; i<NUM_OPS; ++i) {
        make_lex_key<8>(keys[i], key_buf_u64.data());
        std::string key_str(reinterpret_cast<const char*>(key_buf_u64.data()), key_bytes);
        if(umap.find(key_str) == umap.end()) report_error("Val fail");
    }
    end = std::chrono::high_resolution_clock::now();
    dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::string bench_name_get = name + " Get ("+(is_random?"Random":"Sequential")+")";
    long long ns_per_op_get = dur.count()/NUM_OPS;
    std::cout << std::left << std::setw(45) << bench_name_get << ": " << std::right << std::setw(10) << ns_per_op_get << " ns/op" << std::endl;
    benchmark_results[bench_name_get] = ns_per_op_get;
}


// Helper to generate a random string of a given length
std::string generate_random_string(size_t length) {
    // A simple character set for value generation.
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
    std::string str(length, 0);
    std::generate_n(str.begin(), length, [&]() { return charset[dist(gen)]; });
    return str;
}

// --- Key Generation ---

// Generates 1M 64-byte random keys
std::vector<std::string> generate_random_keys(size_t num_keys) {
    std::vector<std::string> keys;
    keys.reserve(num_keys);
    std::mt19937_64 rng(123); // a 64-bit random number generator
    for (size_t i = 0; i < num_keys; ++i) {
        char key[64];
        for (int j = 0; j < 8; ++j) {
            reinterpret_cast<uint64_t*>(key)[j] = rng();
        }
        keys.emplace_back(key, 64);
    }
    return keys;
}

// Generates 1M 64-byte pathological keys
// Alternating between low and high values to stress node splitting.
std::vector<std::string> generate_pathological_keys(size_t num_keys) {
    std::vector<std::string> keys;
    keys.reserve(num_keys);
    char key[64];
    for (size_t i = 0; i < num_keys / 2; ++i) {
        // Low key
        memset(key, 0, 64);
        uint64_t val = BSWAP64(i);
        memcpy(key, &val, sizeof(val));
        keys.emplace_back(key, 64);

        // High key
        memset(key, 0xFF, 64);
        val = BSWAP64(~i);
        memcpy(key, &val, sizeof(val));
        keys.emplace_back(key, 64);
    }
    return keys;
}

// Generates 1M 64-byte clustered keys
std::vector<std::string> generate_clustered_keys(size_t num_keys) {
    std::vector<std::string> keys;
    keys.reserve(num_keys);
    std::mt19937_64 rng(456);
    const size_t num_clusters = 1000;
    const size_t cluster_size = num_keys / num_clusters;

    for (size_t i = 0; i < num_clusters; ++i) {
        char prefix[8];
        reinterpret_cast<uint64_t*>(prefix)[0] = rng();

        for (size_t j = 0; j < cluster_size; ++j) {
            char key[64];
            memcpy(key, prefix, 8);
            // Fill the rest with random data
            for(int k = 1; k < 8; ++k) {
                reinterpret_cast<uint64_t*>(key)[k] = rng();
            }
            keys.emplace_back(key, 64);
        }
    }
    return keys;
}

void run_3d_gaming_benchmark(Database* db);

void run_apk_generation_benchmark() {
    std::cout << "\n--- Running APK Generation Benchmark ---" << std::endl;
#ifdef __EMSCRIPTEN__
    const size_t num_ops = 100000;
#else
    const size_t num_ops = 1000000;
#endif
    std::vector<uint64_t> coords;
    std::vector<char> key_buffer(1024);

    auto benchmark_dim = [&](uint32_t D, const std::string& name) {
        coords.resize(D);
        for(uint32_t i = 0; i < D; ++i) coords[i] = i * 1000;

        // Benchmark new default (APK, 2-byte)
        auto start_lr = std::chrono::high_resolution_clock::now();
        for(size_t i = 0; i < num_ops; ++i) {
            coords[0] = i;
            SpatialKeywords::generate_apk(coords.data(), D);
        }
        auto end_lr = std::chrono::high_resolution_clock::now();
        auto dur_lr = std::chrono::duration_cast<std::chrono::nanoseconds>(end_lr - start_lr);
        long long ns_per_op_lr = dur_lr.count() / num_ops;
        std::cout << std::left << std::setw(45) << "APK (2-byte) Generation " + name << ": " << std::right << std::setw(10) << ns_per_op_lr << " ns/op" << std::endl;
    };

    benchmark_dim(2, "2D");
    benchmark_dim(3, "3D");
    benchmark_dim(4, "4D");
    benchmark_dim(8, "8D");
    benchmark_dim(16, "16D");
    benchmark_dim(32, "32D");
}

void run_3d_gaming_benchmark(Database* db) {
    std::cout << "\n--- REAL-WORLD 3D GAMING WORKLOAD ---" << std::endl;

    const uint32_t D = 3;
#ifdef __EMSCRIPTEN__
    const size_t num_points = 100000;
    const int num_queries = 1000;
#else
    const size_t num_points = 1000000;
    const int num_queries = 1000;
#endif

    uint32_t collection_id = db->get_collection("gaming_bench");
    Collection& collection = db->get_collection_by_idx(collection_id);

    std::cout << " Setting up 1M random 3D points for gaming world..." << std::endl;
    auto start_insert = std::chrono::high_resolution_clock::now();

    std::mt19937 g(1337); // Seed for reproducibility
    std::uniform_int_distribution<uint64_t> distrib;
    std::vector<std::vector<uint64_t>> point_sample;
    point_sample.reserve(num_queries);

    for (size_t i = 0; i < num_points; ++i) {
        std::vector<uint64_t> p(D);
        for(uint32_t d=0; d<D; ++d) p[d] = distrib(g);
        std::string key = SpatialKeywords::generate_apk(p.data(), D);
        collection.insert_sync_direct(key, "v", 0);

        if (i < num_queries) {
            point_sample.push_back(p);
        }
    }

    auto end_insert = std::chrono::high_resolution_clock::now();
    auto insert_dur = std::chrono::duration_cast<std::chrono::milliseconds>(end_insert - start_insert);
    std::cout << " Insertion of " << num_points << " points complete in " << insert_dur.count() << " ms." << std::endl;

    // --- Benchmark Scenarios ---
    std::uniform_int_distribution<size_t> sample_dist(0, point_sample.size() - 1);
    std::cout << "\n" << std::string(120, '-') << std::endl;
    std::cout << std::left
              << std::setw(30) << "Gaming Scenario"
              << std::setw(12) << "Avg (ns)"
              << std::setw(12) << "p50 (ns)"
              << std::setw(12) << "p95 (ns)"
              << std::setw(12) << "p99 (ns)"
              << std::setw(12) << "StdDev"
              << std::setw(18) << "Avg Items Found"
              << "Query Type" << std::endl;
    std::cout << std::string(120, '-') << std::endl;

    auto run_query = [&](const std::string& name, const std::string& type, auto query_lambda) {
        long long total_items_found = 0;
        std::vector<long long> timings;
        timings.reserve(num_queries);

        for (int i = 0; i < num_queries; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            long long items_found_this_run = 0;
            query_lambda(items_found_this_run);
            auto end = std::chrono::high_resolution_clock::now();
            timings.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
            total_items_found += items_found_this_run;
        }

        LatencyStats stats = LatencyStats::calculate(timings);
        long long avg_items = total_items_found / num_queries;

        std::cout << std::left
                  << std::setw(30) << name
                  << std::setw(12) << stats.avg
                  << std::setw(12) << stats.p50
                  << std::setw(12) << stats.p95
                  << std::setw(12) << stats.p99
                  << std::setw(12) << std::fixed << std::setprecision(2) << stats.stddev
                  << std::setw(18) << avg_items
                  << type << std::endl;
    };

    // --- 1. Player View Frustum (Box Query) ---
    run_query("1. Player View Frustum", "Box", [&](long long& items){
        const auto& center = point_sample[sample_dist(g)];
        uint64_t min_b_coords[] = {center[0] > 50 ? center[0] - 50 : 0, center[1] > 50 ? center[1] - 50 : 0, center[2] > 1 ? center[2] - 1 : 0};
        uint64_t max_b_coords[] = {center[0] + 50, center[1] + 50, center[2] + 100};

        std::string min_key = SpatialKeywords::generate_apk(min_b_coords, D);
        std::string max_key = SpatialKeywords::generate_apk(max_b_coords, D);

        size_t prefix_len = 0;
        while (prefix_len < min_key.length() && prefix_len < max_key.length() && min_key[prefix_len] == max_key[prefix_len]) {
            prefix_len++;
        }
        std::string_view prefix(min_key.data(), prefix_len);

        QueryBuilder query(db, collection.get_id(), "");
        query.where_string("primary_key", QueryOp::PREFIX, prefix);
        auto results = query.execute();

        for (const auto& doc : results) {
            std::string_view key_sv = doc.get_primary_key();
            if (!key_sv.empty()) {
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(key_sv, coords.data(), D);
                bool in_box = true;
                for (uint32_t i = 0; i < D; ++i) {
                    if (coords[i] < min_b_coords[i] || coords[i] > max_b_coords[i]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    items++;
                }
            }
        }
    });

    // --- 2. Grenade Explosion (Sphere Query) ---
    run_query("2. Grenade Explosion", "Sphere", [&](long long& items){
        // Sphere queries are not yet supported by the QueryBuilder.
    });

    // --- 3. Sniper Scope (Box + KNN) ---
    run_query("3. Sniper Scope", "Box+KNN", [&](long long& items){
        // KNN queries are not yet supported by the QueryBuilder.
    });

    // --- 4. Radar Sweep (Sphere Query) ---
    run_query("4. Radar Sweep", "Sphere", [&](long long& items){
        // Sphere queries are not yet supported by the QueryBuilder.
    });

    // --- 5. Melee Attack (Box Query) ---
    run_query("5. Melee Attack", "Box", [&](long long& items){
        const auto& center = point_sample[sample_dist(g)];
        uint64_t min_b_coords[] = {center[0] > 2000 ? center[0] - 2000 : 0, center[1] > 2000 ? center[1] - 2000 : 0, center[2]};
        uint64_t max_b_coords[] = {center[0] + 2000, center[1] + 2000, center[2] + 5000};

        std::string min_key = SpatialKeywords::generate_apk(min_b_coords, D);
        std::string max_key = SpatialKeywords::generate_apk(max_b_coords, D);

        QueryBuilder query(db, collection.get_id(), "");
        query.where_string("primary_key", QueryOp::BETWEEN, min_key, max_key);
        auto results = query.execute();

        for (const auto& doc : results) {
            std::string_view key_sv = doc.get_primary_key();
            if (!key_sv.empty()) {
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(key_sv, coords.data(), D);
                bool in_box = true;
                for (uint32_t i = 0; i < D; ++i) {
                    if (coords[i] < min_b_coords[i] || coords[i] > max_b_coords[i]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    items++;
                }
            }
        }
    });

    // --- 6. Find Nearest Health Pack (KNN Query) ---
    run_query("6. Find Nearest Health Pack", "KNN (k=1)", [&](long long& items){
        // KNN queries are not yet supported by the QueryBuilder.
    });

    // --- 7. AI Guard Patrol (Box Query) ---
    run_query("7. AI Guard Patrol", "Box", [&](long long& items){
        const auto& center = point_sample[sample_dist(g)];
        uint64_t min_b_coords[] = {center[0] > 1000 ? center[0] - 1000 : 0, center[1] > 500 ? center[1] - 500 : 0, center[2]};
        uint64_t max_b_coords[] = {center[0] + 1000, center[1] + 500, center[2] + 4000};

        std::string min_key = SpatialKeywords::generate_apk(min_b_coords, D);
        std::string max_key = SpatialKeywords::generate_apk(max_b_coords, D);

        QueryBuilder query(db, collection.get_id(), "");
        query.where_string("primary_key", QueryOp::BETWEEN, min_key, max_key);
        auto results = query.execute();

        for (const auto& doc : results) {
            std::string_view key_sv = doc.get_primary_key();
            if (!key_sv.empty()) {
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(key_sv, coords.data(), D);
                bool in_box = true;
                for (uint32_t i = 0; i < D; ++i) {
                    if (coords[i] < min_b_coords[i] || coords[i] > max_b_coords[i]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    items++;
                }
            }
        }
    });

    // --- 8. Artillery Strike (Sphere Query) ---
    run_query("8. Artillery Strike", "Sphere", [&](long long& items){
        // Sphere queries are not yet supported by the QueryBuilder.
    });

    // --- 9. Collision Detection (Box Query) ---
    run_query("9. Collision Detection", "Box", [&](long long& items){
        const auto& center = point_sample[sample_dist(g)];
        uint64_t min_b_coords[] = {center[0] > 100 ? center[0] - 100 : 0, center[1] > 100 ? center[1] - 100 : 0, center[2] > 100 ? center[2] - 100 : 0};
        uint64_t max_b_coords[] = {center[0] + 100, center[1] + 100, center[2] + 100};

        std::string min_key = SpatialKeywords::generate_apk(min_b_coords, D);
        std::string max_key = SpatialKeywords::generate_apk(max_b_coords, D);

        QueryBuilder query(db, collection.get_id(), "");
        query.where_string("primary_key", QueryOp::BETWEEN, min_key, max_key);
        auto results = query.execute();

        for (const auto& doc : results) {
            std::string_view key_sv = doc.get_primary_key();
            if (!key_sv.empty()) {
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(key_sv, coords.data(), D);
                bool in_box = true;
                for (uint32_t i = 0; i < D; ++i) {
                    if (coords[i] < min_b_coords[i] || coords[i] > max_b_coords[i]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    items++;
                }
            }
        }
    });

    // --- 10. Loot Drop Distribution (KNN Query) ---
    run_query("10. Loot Drop Distribution", "KNN (k=5)", [&](long long& items){
        // KNN queries are not yet supported by the QueryBuilder.
    });
}

void run_object_store_benchmarks() {
    // This benchmark is obsolete due to the new Micro-Tree architecture.
    // The relevant benchmarks are now in micro_tree_bench.cpp.
    std::cout << "\n--- Dynamic Object Store Benchmarks (Obsolete) ---" << std::endl;
}

int main() {
    g_temp_dir = std::filesystem::temp_directory_path() / "stax_bench";
    recursive_delete(g_temp_dir);
    std::filesystem::create_directory(g_temp_dir);
    std::cout << "Using temporary directory: " << g_temp_dir.string() << std::endl;

    try {
        srand(12345);

        const size_t num_threads = 4;
        auto db = Database::create_new(g_temp_dir, num_threads);

        run_3d_gaming_benchmark(db.get());

        run_apk_generation_benchmark();

        std::cout << "\n--- Unordered Map Benchmarks (ns/op) ---" << std::endl;
        run_unordered_map_benchmark(8, false); run_unordered_map_benchmark(8, true);
        run_unordered_map_benchmark(16, false); run_unordered_map_benchmark(16, true);
        run_unordered_map_benchmark(32, false); run_unordered_map_benchmark(32, true);

    } catch (const std::exception& e) {
        std::cerr << "An exception occurred: " << e.what() << std::endl;
        recursive_delete(g_temp_dir);
        return 1;
    }

    recursive_delete(g_temp_dir);
    return 0;
}
