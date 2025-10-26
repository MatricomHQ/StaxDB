#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include <limits>
#include <memory>
#include <atomic>
#include <cmath>
#include <cassert>
#include <algorithm>
#include <set>

#include "stax_core/stax_new_tree.hpp"
#include "stax_core/dimensional.hpp"
#include "stax_db/arena_structs.h" // For FileHeader

// Helper to print a coordinate vector for debugging
void print_point(const std::vector<uint64_t>& p) {
    std::cout << "(";
    for (size_t i = 0; i < p.size(); ++i) {
        std::cout << p[i] << (i == p.size() - 1 ? "" : ", ");
    }
    std::cout << ")";
}


void print_benchmark_header(const std::string& query_type, uint32_t D, double selectivity) {
    std::cout << "\n--- " << query_type << " Query Benchmark (" << D << "D, " << std::fixed << std::setprecision(3) << selectivity * 100 << "% selectivity) ---" << std::endl;
    std::cout << " Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Expected | Correct | Efficiency" << std::endl;
    std::cout << "--------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
}

void print_query_stats_row(
    long long duration_ns,
    const QueryStats& stats,
    size_t expected_records,
    bool correct,
    const std::set<std::vector<uint64_t>>& tree_results,
    const std::set<std::vector<uint64_t>>& brute_force_results
) {
    double efficiency = (stats.records_scanned > 0) ? static_cast<double>(stats.records_accepted) / stats.records_scanned : 1.0;
    long long ns_per_item = (stats.records_accepted > 0) ? (duration_ns / stats.records_accepted) : 0;
    std::cout << " " << std::left << std::setw(12) << duration_ns
              << " | " << std::setw(8) << ns_per_item
              << " | " << std::setw(13) << stats.nodes_visited
              << " | " << std::setw(14) << stats.leaves_visited
              << " | " << std::setw(11) << stats.records_loaded
              << " | " << std::setw(12) << stats.records_scanned
              << " | " << std::setw(13) << stats.records_accepted
              << " | " << std::setw(8) << expected_records
              << " | " << std::setw(7) << (correct ? "PASS" : "FAIL")
              << " | " << std::fixed << std::setprecision(3) << efficiency << std::endl;

    if (!correct) {
        std::vector<std::vector<uint64_t>> in_tree_not_brute, in_brute_not_tree;
        std::set_difference(tree_results.begin(), tree_results.end(),
                            brute_force_results.begin(), brute_force_results.end(),
                            std::back_inserter(in_tree_not_brute));
        std::set_difference(brute_force_results.begin(), brute_force_results.end(),
                            tree_results.begin(), tree_results.end(),
                            std::back_inserter(in_brute_not_tree));

        if (!in_tree_not_brute.empty()) {
            std::cout << "     [DEBUG] Points in TREE result but NOT in Brute-Force result:" << std::endl;
            for(const auto& p : in_tree_not_brute) {
                std::cout << "      - ";
                print_point(p);
                std::cout << std::endl;
            }
        }
        if (!in_brute_not_tree.empty()) {
            std::cout << "     [DEBUG] Points in Brute-Force result but NOT in TREE result:" << std::endl;
            for(const auto& p : in_brute_not_tree) {
                std::cout << "      - ";
                print_point(p);
                std::cout << std::endl;
            }
        }
    }
}

// Main function to run a full spatial workload benchmark
void run_spatial_workload(
    const std::string& workload_name,
    uint32_t D,
    size_t num_points,
    int num_queries,
    double target_selectivity
) {
    std::cout << "\n--- Running Spatial Workload: " << workload_name << " ---" << std::endl;

    // 1. --- SETUP: Create Tree and Generate Data ---
    std::vector<uint8_t> memory_buffer(1024 * 1024 * 1024); // 1GB
    FileHeader* file_header = reinterpret_cast<FileHeader*>(memory_buffer.data());
    file_header->global_alloc_offset = sizeof(FileHeader);

    StaxAllocator stax_alloc(file_header, memory_buffer.data());
    ThreadLocalAllocator local_alloc(stax_alloc);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(stax_alloc, root_ptr, D);
    TxnContext ctx{1, 1}; // Dummy context for benchmark

    std::cout << " Generating " << num_points << " random " << D << "D points..." << std::endl;
    std::mt19937 g(123); // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> distrib;
    std::vector<std::vector<uint64_t>> inserted_points;
    inserted_points.reserve(num_points);
    std::vector<std::vector<uint64_t>> ground_truth_points;
    ground_truth_points.reserve(num_points);

    for (size_t i = 0; i < num_points; ++i) {
        std::vector<uint64_t> p(D);
        for (uint32_t d = 0; d < D; ++d) p[d] = distrib(g);
        inserted_points.push_back(p);
        std::string key = SpatialKeywords::generate_apk(p.data(), D);
        tree.insert(local_alloc, ctx, key, "");

        std::vector<uint64_t> decoded_p(D);
        SpatialKeywords::get_coords_from_apk(key, decoded_p.data(), D);
        ground_truth_points.push_back(decoded_p);
    }
    std::cout << " Data generation complete." << std::endl;

    // 2. --- SETUP: Generate Query Parameters ---
    size_t target_records_in_query = static_cast<size_t>(num_points * target_selectivity);
    std::cout << " Generating " << num_queries << " queries with target selectivity of " << target_records_in_query << " items..." << std::endl;
    std::vector<std::vector<uint64_t>> query_centers(num_queries);
    std::vector<double> query_radii(num_queries);
    std::uniform_int_distribution<size_t> point_dist(0, num_points - 1);

    for (int i = 0; i < num_queries; ++i) {
        // CRITICAL FIX: Use the ground_truth_points for query centers to ensure precision consistency.
        query_centers[i] = ground_truth_points[point_dist(g)];
        std::vector<void*> knn_results = tree.query_knn(query_centers[i].data(), target_records_in_query);

        if (knn_results.size() >= target_records_in_query) {
            void* handle = knn_results[target_records_in_query - 1];
            StaxRecord* record = stax_alloc.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
            std::vector<uint64_t> coords(D);
            SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D);
            query_radii[i] = sqrt(SpatialKeywords::PointDistSq(coords.data(), query_centers[i].data(), D));
        } else {
            query_radii[i] = 0;
        }
    }
    std::cout << " Query generation complete." << std::endl;

    // 3. --- BENCHMARK: Box Query ---
    {
        print_benchmark_header("Box", D, target_selectivity);
        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];
            uint64_t radius = static_cast<uint64_t>(query_radii[i]);
            AABB query_aabb(D);
            for(uint32_t d=0; d<D; ++d) {
                query_aabb.min_bounds[d] = (center[d] > radius) ? center[d] - radius : 0;
                query_aabb.max_bounds[d] = (center[d] < std::numeric_limits<uint64_t>::max() - radius) ? center[d] + radius : std::numeric_limits<uint64_t>::max();
            }

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_box(query_aabb, &query_stats);
            auto end = std::chrono::high_resolution_clock::now();
            long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

            // Correctness Test
            std::set<std::vector<uint64_t>> brute_force_results;
            for (const auto& p : ground_truth_points) {
                bool in_box = true;
                for (uint32_t d = 0; d < D; ++d) {
                    if (p[d] < query_aabb.min_bounds[d] || p[d] > query_aabb.max_bounds[d]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) {
                    brute_force_results.insert(p);
                }
            }
            std::set<std::vector<uint64_t>> tree_results;
            for (void* handle : results) {
                StaxRecord* record = stax_alloc.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D);
                tree_results.insert(coords);
            }
            bool correct = (tree_results == brute_force_results);
            print_query_stats_row(duration_ns, query_stats, brute_force_results.size(), correct, tree_results, brute_force_results);
        }
    }

    // 4. --- BENCHMARK: Sphere Query ---
    {
        print_benchmark_header("Sphere", D, target_selectivity);
        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];
            double radius = query_radii[i];

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_sphere(center.data(), radius, &query_stats);
            auto end = std::chrono::high_resolution_clock::now();
            long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

            // Correctness Test
            std::set<std::vector<uint64_t>> brute_force_results;
            double radius_sq = radius * radius;
            for (const auto& p : ground_truth_points) {
                if (SpatialKeywords::PointDistSq(p.data(), center.data(), D) <= radius_sq) {
                    brute_force_results.insert(p);
                }
            }
            std::set<std::vector<uint64_t>> tree_results;
            for (void* handle : results) {
                StaxRecord* record = stax_alloc.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D);
                tree_results.insert(coords);
            }
            bool correct = (tree_results == brute_force_results);
            print_query_stats_row(duration_ns, query_stats, brute_force_results.size(), correct, tree_results, brute_force_results);
        }
    }

    // 5. --- BENCHMARK: KNN Query ---
    {
        print_benchmark_header("KNN", D, target_selectivity);
        size_t k = target_records_in_query;
        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_knn(center.data(), k, &query_stats);
            auto end = std::chrono::high_resolution_clock::now();
            long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

            // Correctness Test
            std::vector<std::pair<double, std::vector<uint64_t>>> sorted_points;
            for (const auto& p : ground_truth_points) {
                sorted_points.push_back({SpatialKeywords::PointDistSq(p.data(), center.data(), D), p});
            }
            std::sort(sorted_points.begin(), sorted_points.end(), [](const auto& a, const auto& b) {
                return a.first < b.first;
            });
            std::set<std::vector<uint64_t>> brute_force_results;
            for (size_t j = 0; j < k && j < sorted_points.size(); ++j) {
                brute_force_results.insert(sorted_points[j].second);
            }
            std::set<std::vector<uint64_t>> tree_results;
            for (void* handle : results) {
                StaxRecord* record = stax_alloc.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                std::vector<uint64_t> coords(D);
                SpatialKeywords::get_coords_from_apk(record->get_key(), coords.data(), D);
                tree_results.insert(coords);
            }
            bool correct = (tree_results == brute_force_results);
            print_query_stats_row(duration_ns, query_stats, brute_force_results.size(), correct, tree_results, brute_force_results);
        }
    }
}

int main() {
    std::cout << "--- New Expanded Spatial Query Benchmarks ---" << std::endl;
    run_spatial_workload("2D Uniform", 2, 20000, 10, 0.01); // Reduced queries for faster debug cycle
    run_spatial_workload("3D Uniform", 3, 20000, 10, 0.01);
    run_spatial_workload("4D Uniform", 4, 20000, 10, 0.01);
    run_spatial_workload("8D Uniform", 8, 20000, 10, 0.01);
    run_spatial_workload("1024D Uniform", 1024, 1000, 10, 0.01);
    return 0;
}
