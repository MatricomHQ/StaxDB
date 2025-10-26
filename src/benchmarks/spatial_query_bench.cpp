#include "stax_core/stax_new_tree.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>
#include <memory>

// A minimal test context.
const TxnContext BENCH_CTX = {1, 1, 0};

// =================================================================================================
// --- Benchmark Setup ---
// =================================================================================================
class MockStaxAllocator {
private:
    std::vector<uint8_t> memory_;
    size_t next_offset_ = 1;
public:
    MockStaxAllocator(size_t mb) { memory_.resize(mb * 1024 * 1024); }
    uint64_t allocate(size_t size, size_t alignment = 8) {
        size_t align_mask = alignment - 1;
        size_t aligned_offset = (next_offset_ + align_mask) & ~align_mask;
        if (aligned_offset + size > memory_.size()) throw std::runtime_error("Mock allocator out of memory");
        next_offset_ = aligned_offset + size;
        return aligned_offset;
    }
    template<typename T>
    T* get_ptr(uint64_t offset) {
        if (offset == 0 || offset >= memory_.size()) return nullptr;
        return reinterpret_cast<T*>(memory_.data() + offset);
    }
     template<typename T>
    const T* get_ptr(uint64_t offset) const {
        if (offset == 0 || offset >= memory_.size()) return nullptr;
        return reinterpret_cast<const T*>(memory_.data() + offset);
    }
    uint8_t* get_base_ptr() { return memory_.data(); }
};

// Helper function to print statistics in the desired format
void print_spatial_query_stats(const std::string& query_type, uint32_t D, double selectivity, long long duration_ns, const QueryStats& stats) {
    double efficiency = (stats.records_scanned > 0) ? static_cast<double>(stats.records_accepted) / stats.records_scanned : 0.0;
    long long ns_per_item = (stats.records_accepted > 0) ? (duration_ns / stats.records_accepted) : 0;

    std::cout << "\n--- " << query_type << " Query Benchmark (" << D << "D, " << std::fixed << std::setprecision(3) << selectivity * 100 << "% selectivity) ---" << std::endl;
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

// Main function to run a full spatial workload benchmark
void run_spatial_workload(
    const std::string& workload_name,
    uint32_t D,
    size_t num_points,
    int num_queries,
    double target_selectivity
) {
    std::cout << "\n--- Running Spatial Workload: " << workload_name << " ---" << std::endl;

    // 1. --- SETUP: Create Database and Generate Data ---
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc(500); // Increased memory for larger datasets
    StaxAllocator allocator(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator, root_ptr, D);

    std::cout << " Generating " << num_points << " random " << D << "D points..." << std::endl;
    std::mt19937_64 g(123); // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> distrib;
    std::vector<std::vector<uint64_t>> inserted_points;
    inserted_points.reserve(num_points);

    for (size_t i = 0; i < num_points; ++i) {
        std::vector<uint64_t> p(D);
        for (uint32_t d = 0; d < D; ++d) p[d] = distrib(g);
        inserted_points.push_back(p);
        tree.insert(local_alloc, BENCH_CTX, SpatialKeywords::generate_apk(p.data(), D), "v");
    }
    std::cout << " Data generation complete." << std::endl;

    // 2. --- SETUP: Generate Query Parameters ---
    size_t target_records_in_query = static_cast<size_t>(num_points * target_selectivity);
    std::cout << " Generating " << num_queries << " queries with target selectivity of " << target_records_in_query << " items..." << std::endl;

    std::vector<std::vector<uint64_t>> query_centers(num_queries, std::vector<uint64_t>(D));
    std::vector<long double> query_radii(num_queries);
    std::uniform_int_distribution<size_t> point_dist(0, num_points - 1);

    for (int i = 0; i < num_queries; ++i) {
        query_centers[i] = inserted_points[point_dist(g)];
        QueryStats dummy_stats;
        std::vector<void*> knn_results = tree.query_knn(query_centers[i].data(), target_records_in_query, dummy_stats);

        if (!knn_results.empty()) {
            const StaxRecord* farthest_rec = allocator.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(knn_results.back())));
            std::vector<uint64_t> coords(D);
            SpatialKeywords::get_coords_from_apk(farthest_rec->get_key(), coords.data(), D);
            query_radii[i] = std::sqrt(SpatialKeywords::PointDistSq(query_centers[i].data(), coords.data(), D));
        } else {
            query_radii[i] = 0;
        }
    }
    std::cout << " Query generation complete." << std::endl;

    // 3. --- BENCHMARK: Box Query ---
    {
        QueryStats total_stats;
        long long total_duration_ns = 0;

        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];
            auto radius = static_cast<uint64_t>(query_radii[i]);
            AABB query_box(D);
            for(uint32_t d=0; d<D; ++d) {
                query_box.min_bounds[d] = (center[d] > radius) ? center[d] - radius : 0;
                query_box.max_bounds[d] = (center[d] < std::numeric_limits<uint64_t>::max() - radius) ? center[d] + radius : std::numeric_limits<uint64_t>::max();
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
        }

        if (num_queries > 0) {
            total_stats.nodes_visited /= num_queries;
            total_stats.leaves_visited /= num_queries;
            total_stats.records_loaded /= num_queries;
            total_stats.records_scanned /= num_queries;
            total_stats.records_accepted /= num_queries;
            print_spatial_query_stats("Box", D, target_selectivity, total_duration_ns / num_queries, total_stats);
        }
    }

    // 4. --- BENCHMARK: Sphere Query ---
    {
        QueryStats total_stats;
        long long total_duration_ns = 0;

        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];
            long double radius = query_radii[i];

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
        }

        if (num_queries > 0) {
            total_stats.nodes_visited /= num_queries;
            total_stats.leaves_visited /= num_queries;
            total_stats.records_loaded /= num_queries;
            total_stats.records_scanned /= num_queries;
            total_stats.records_accepted /= num_queries;
            print_spatial_query_stats("Sphere", D, target_selectivity, total_duration_ns / num_queries, total_stats);
        }
    }

    // 5. --- BENCHMARK: KNN Query ---
    {
        QueryStats total_stats;
        long long total_duration_ns = 0;
        int k = target_records_in_query;

        for (int i = 0; i < num_queries; ++i) {
            const auto& center = query_centers[i];

            QueryStats query_stats;
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<void*> results = tree.query_knn(center.data(), k, query_stats);
            auto end = std::chrono::high_resolution_clock::now();

            total_duration_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            total_stats.nodes_visited += query_stats.nodes_visited;
            total_stats.leaves_visited += query_stats.leaves_visited;
            total_stats.records_loaded += query_stats.records_loaded;
            total_stats.records_scanned += query_stats.records_scanned;
            total_stats.records_accepted += query_stats.records_accepted;
        }

        if (num_queries > 0) {
            total_stats.nodes_visited /= num_queries;
            total_stats.leaves_visited /= num_queries;
            total_stats.records_loaded /= num_queries;
            total_stats.records_scanned /= num_queries;
            total_stats.records_accepted /= num_queries;
            print_spatial_query_stats("KNN", D, target_selectivity, total_duration_ns / num_queries, total_stats);
        }
    }
}

int main() {
    std::cout << "--- New Expanded Spatial Query Benchmarks ---" << std::endl;
    run_spatial_workload("2D Uniform", 2, 20000, 100, 0.01);
    run_spatial_workload("3D Uniform", 3, 20000, 100, 0.01);
    run_spatial_workload("4D Uniform", 4, 20000, 100, 0.01);
    run_spatial_workload("8D Uniform", 8, 20000, 100, 0.01);
    run_spatial_workload("1024D Uniform", 1024, 1000, 20, 0.01);
    return 0;
}
