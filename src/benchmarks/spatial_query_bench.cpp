// =================================================================================================
// 1. Standard Library and System Headers
// =================================================================================================
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>
#include <memory>
#include <atomic>
#include <thread>
#include <filesystem>
#include <functional>
#include <map>
#include <cmath>
#include <unordered_set>

#include "stax_db/db.h"
#include "stax_core/stax_new_tree.hpp"
#include "stax_core/stax_structs.hpp"
#include "stax_core/dimensional.hpp"


// =================================================================================================
// 2. Global State & Configuration
// =================================================================================================

// Global error counter for capping error messages
std::atomic<int> g_error_count = 0;
const int MAX_ERRORS = 5;

void report_error(const std::string& msg) {
    int previous_errors = g_error_count.fetch_add(1, std::memory_order_relaxed);
    if (previous_errors < MAX_ERRORS) {
        std::cerr << msg << std::endl;
    } else if (previous_errors == MAX_ERRORS) {
        std::cerr << "Maximum number of errors (" << MAX_ERRORS << ") exceeded. Terminating." << std::endl;
        exit(1);
    }
}


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

void print_spatial_query_stats(const std::string& query_type, uint32_t D, double selectivity, long long duration_ns, const QueryStats& stats) {
    double efficiency = (stats.records_scanned > 0) ? static_cast<double>(stats.records_accepted) / stats.records_scanned : 0.0;
    long long ns_per_item = (stats.records_accepted > 0) ? (duration_ns / stats.records_accepted) : 0;

    std::cout << "\n--- " << query_type << " Query Benchmark (" << D << "D, " << selectivity * 100 << "% selectivity) ---" << std::endl;
    std::cout << " Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency" << std::endl;
    std::cout << "--------------------------------------------------------------------------------------------------------------------------------" << std::endl;
    std::cout << " " << std::left << std::setw(12) << duration_ns
              << " | " << std::setw(8) << ns_per_item
              << " | " << std::setw(13) << stats.nodes_visited
              << " | " << std::setw(14) << stats.leaves_visited
              << " | " << std::setw(11) << stats.records_loaded
              << " | " << std::setw(12) << stats.records_scanned
              << " | " << std::setw(13) << stats.records_accepted
              << " | " << std::fixed << std::setprecision(3) << efficiency << std::endl;
}

void run_spatial_workload(
    const std::string& workload_name,
    uint32_t D,
    size_t num_points,
    int num_queries,
    size_t target_records_in_query
) {
    std::cout << "\n--- Running Spatial Workload: " << workload_name << " ---" << std::endl;

    // 1. Setup Database and Collection
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "stax_spatial_bench";
    std::filesystem::create_directories(temp_dir);

    auto db = Database::create_new(temp_dir, 1);
    uint32_t collection_id = db->get_collection("spatial_collection");
    Collection& collection = db->get_collection_by_idx(collection_id);
    StaxTree16& tree = collection.get_tree();
    const StaxAllocator& allocator = tree.get_allocator();

    // 2. Insert Data
    std::mt19937 g(456);
    std::vector<std::vector<uint64_t>> inserted_points;
    inserted_points.reserve(num_points);
    std::uniform_int_distribution<uint64_t> distrib;
    for (size_t i = 0; i < num_points; ++i) {
        std::vector<uint64_t> p(D);
        for(uint32_t d=0; d<D; ++d) p[d] = distrib(g);
        inserted_points.push_back(p);
    }

    std::vector<char> key_buffer(SpatialKeywords::get_max_apk_size(D));
    for (const auto& p : inserted_points) {
        size_t key_size = SpatialKeywords::generate_apk(p.data(), D, reinterpret_cast<uint8_t*>(key_buffer.data()), key_buffer.size());
        collection.insert_sync_direct(std::string_view(key_buffer.data(), key_size), "v", 0);
    }

    // 3. Generate Queries
    double selectivity = static_cast<double>(target_records_in_query) / num_points;
    std::mt19937 query_rng(789);
    std::uniform_int_distribution<size_t> point_dist(0, num_points - 1);
    std::vector<std::vector<uint64_t>> query_centers(num_queries, std::vector<uint64_t>(D));
    std::vector<uint64_t> query_radii(num_queries);

    for (int i = 0; i < num_queries; ++i) {
        query_centers[i] = inserted_points[point_dist(query_rng)];
        QueryStats dummy_stats;
        std::vector<void*> knn_results = tree.query_knn(query_centers[i].data(), target_records_in_query, dummy_stats);

        if (knn_results.size() == target_records_in_query) {
             const StaxRecord* farthest_rec = allocator.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(knn_results.back())));
             std::vector<uint64_t> coords(D);
             SpatialKeywords::get_coords_from_apk(farthest_rec->get_key(), coords.data(), D);
             unsigned __int128 dist_sq = SpatialKeywords::PointDistSq(query_centers[i].data(), coords.data(), D);
             uint64_t r = static_cast<uint64_t>(sqrtl(dist_sq));
             if ((unsigned __int128)r * r < dist_sq) r++;
             query_radii[i] = r;
        } else {
            query_radii[i] = std::numeric_limits<uint64_t>::max();
        }
    }

    // 4. Run Box Query Benchmark + Correctness Test
    {
        QueryStats total_stats;
        long long total_duration_ns = 0;
        size_t box_mismatches = 0;

        for (int i = 0; i < num_queries; ++i) {
            const auto& center_point = query_centers[i];
            uint64_t radius = query_radii[i];
            AABB query_box(D);
             for(uint32_t d=0; d<D; ++d) {
                query_box.min_bounds[d] = (center_point[d] > radius) ? center_point[d] - radius : 0;
                query_box.max_bounds[d] = (center_point[d] < std::numeric_limits<uint64_t>::max() - radius) ? center_point[d] + radius : std::numeric_limits<uint64_t>::max();
            }

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_box(query_box, query_stats);
            auto end = std::chrono::high_resolution_clock::now();
            total_duration_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            total_stats.nodes_visited += query_stats.nodes_visited;
            total_stats.leaves_visited += query_stats.leaves_visited;
            total_stats.records_loaded += query_stats.records_loaded;
            total_stats.records_scanned += query_stats.records_scanned;
            total_stats.records_accepted += query_stats.records_accepted;

            // Correctness Check
            std::unordered_set<std::string> returned_keys;
            for (void* handle : results) {
                 const StaxRecord* rec = allocator.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                 returned_keys.insert(std::string(rec->get_key()));
            }

            std::unordered_set<std::string> ground_truth_keys;
            std::vector<char> key_buffer_gt(SpatialKeywords::get_max_apk_size(D));
            for (const auto& p : inserted_points) {
                bool in_box = true;
                for (uint32_t d = 0; d < D; ++d) {
                    if (p[d] < query_box.min_bounds[d] || p[d] > query_box.max_bounds[d]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    size_t key_size = SpatialKeywords::generate_apk(p.data(), D, reinterpret_cast<uint8_t*>(key_buffer_gt.data()), key_buffer_gt.size());
                    ground_truth_keys.insert(std::string(key_buffer_gt.data(), key_size));
                }
            }

            if (returned_keys != ground_truth_keys) {
                box_mismatches++;
            }
        }
        std::cout << " Box Query Correctness (" << D << "D): " << (num_queries - box_mismatches) << "/" << num_queries << " matched." << std::endl;
        if (box_mismatches > 0) report_error("Box query correctness failed.");


        if (num_queries > 0) {
            total_stats.nodes_visited /= num_queries;
            total_stats.leaves_visited /= num_queries;
            total_stats.records_loaded /= num_queries;
            total_stats.records_scanned /= num_queries;
            total_stats.records_accepted /= num_queries;
        }
        print_spatial_query_stats("Box", D, selectivity, total_duration_ns / num_queries, total_stats);
    }

    // 5. Run Sphere Query Benchmark + Correctness Test
    {
        QueryStats total_stats;
        long long total_duration_ns = 0;
        size_t sphere_mismatches = 0;

        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];
            uint64_t radius = query_radii[i];

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_sphere(center.data(), radius, query_stats);
            auto end = std::chrono::high_resolution_clock::now();

            total_duration_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            total_stats.nodes_visited += query_stats.nodes_visited;
            total_stats.leaves_visited += query_stats.leaves_visited;
            total_stats.records_loaded += query_stats.records_loaded;
            total_stats.records_scanned += query_stats.records_scanned;
            total_stats.records_accepted += query_stats.records_accepted;

            // Correctness Check
            std::unordered_set<std::string> returned_keys;
            for (void* handle : results) {
                 const StaxRecord* rec = allocator.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                 returned_keys.insert(std::string(rec->get_key()));
            }

            std::unordered_set<std::string> ground_truth_keys;
            std::vector<char> key_buffer_gt(SpatialKeywords::get_max_apk_size(D));
            long double radius_sq = static_cast<long double>(radius) * radius;
            for (const auto& p : inserted_points) {
                if (SpatialKeywords::PointDistSq(p.data(), center.data(), D) <= radius_sq) {
                    size_t key_size = SpatialKeywords::generate_apk(p.data(), D, reinterpret_cast<uint8_t*>(key_buffer_gt.data()), key_buffer_gt.size());
                    ground_truth_keys.insert(std::string(key_buffer_gt.data(), key_size));
                }
            }

            if (returned_keys != ground_truth_keys) {
                sphere_mismatches++;
            }
        }
        std::cout << " Sphere Query Correctness (" << D << "D): " << (num_queries - sphere_mismatches) << "/" << num_queries << " matched." << std::endl;
        if (sphere_mismatches > 0) report_error("Sphere query correctness failed.");


        if (num_queries > 0) {
            total_stats.nodes_visited /= num_queries;
            total_stats.leaves_visited /= num_queries;
            total_stats.records_loaded /= num_queries;
            total_stats.records_scanned /= num_queries;
            total_stats.records_accepted /= num_queries;
        }
        print_spatial_query_stats("Sphere", D, selectivity, total_duration_ns / num_queries, total_stats);
    }

    // Cleanup
    db.reset();
    std::filesystem::remove_all(temp_dir);
}


void run_new_spatial_query_benchmarks() {
    std::cout << "\n--- New Expanded Spatial Query Benchmarks ---" << std::endl;
    for (uint32_t D : {2, 3, 4, 8}) {
        size_t num_points = 10000;
        int num_queries = 100;
        size_t target_records = num_points / 100; // ~1% selectivity
        std::string workload_name = std::to_string(D) + "D Uniform";
        run_spatial_workload(workload_name, D, num_points, num_queries, target_records);
    }
}

int main() {
    try {
        run_new_spatial_query_benchmarks();
    } catch (const std::exception& e) {
        std::cerr << "An exception occurred: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
