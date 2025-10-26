#include "stax_core/stax_new_tree.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>

// A minimal test context.
const TxnContext BENCH_CTX = {1, 1, 0};
const int NUM_ITEMS = 100000;
const int NUM_SEEKS = 10000;
const int NUM_QUERIES = 1000;

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
    uint8_t* get_base_ptr() { return memory_.data(); }
};

void generate_dimensional_test_data(std::vector<std::vector<uint64_t>>& points, int count, int dims) {
    std::mt19937_64 rng(1337);
    std::uniform_int_distribution<uint64_t> distrib;
    points.resize(count, std::vector<uint64_t>(dims));
    for(int i=0; i<count; ++i) {
        for(int d=0; d<dims; ++d) {
            points[i][d] = distrib(rng);
        }
    }
}

// =================================================================================================
// --- Benchmarks ---
// =================================================================================================
void run_query_benchmarks() {
    std::cout << "\n--- Benchmark: Dimensional Queries ---" << std::endl;
    const int D = 3;
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc(200);
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    std::vector<std::vector<uint64_t>> points;
    generate_dimensional_test_data(points, NUM_ITEMS, D);

    std::cout << "[Setup] Inserting " << NUM_ITEMS << " keys..." << std::endl;
    for(const auto& p : points) {
        tree.insert(local_alloc, BENCH_CTX, SpatialKeywords::generate_apk(p.data(), D), "");
    }

    // --- Query Box Benchmark ---
    auto start_box = std::chrono::high_resolution_clock::now();
    for(int i=0; i < NUM_QUERIES; ++i) {
        AABB query_box(D);
        for(int d=0; d<D; ++d) {
            query_box.min_bounds[d] = points[i][d] - 1000;
            query_box.max_bounds[d] = points[i][d] + 1000;
        }
        QueryStats stats;
        std::vector<void*> results = tree.query_box(query_box, stats);
    }
    auto end_box = std::chrono::high_resolution_clock::now();
    auto total_box = std::chrono::duration_cast<std::chrono::nanoseconds>(end_box - start_box);
    std::cout << "Average query_box latency: " << total_box.count() / NUM_QUERIES << " ns/op" << std::endl;

    // --- Query Sphere Benchmark ---
    auto start_sphere = std::chrono::high_resolution_clock::now();
    for(int i=0; i < NUM_QUERIES; ++i) {
        QueryStats stats;
        std::vector<void*> results = tree.query_sphere(points[i].data(), 1500.0L, stats);
    }
    auto end_sphere = std::chrono::high_resolution_clock::now();
    auto total_sphere = std::chrono::duration_cast<std::chrono::nanoseconds>(end_sphere - start_sphere);
    std::cout << "Average query_sphere latency: " << total_sphere.count() / NUM_QUERIES << " ns/op" << std::endl;

    // --- Query KNN Benchmark ---
    auto start_knn = std::chrono::high_resolution_clock::now();
    for(int i=0; i < NUM_QUERIES; ++i) {
        QueryStats stats;
        std::vector<void*> results = tree.query_knn(points[i].data(), 10, stats);
    }
    auto end_knn = std::chrono::high_resolution_clock::now();
    auto total_knn = std::chrono::duration_cast<std::chrono::nanoseconds>(end_knn - start_knn);
    std::cout << "Average query_knn latency: " << total_knn.count() / NUM_QUERIES << " ns/op" << std::endl;
}

int main() {
    try {
        run_query_benchmarks();
    } catch (const std::exception& e) {
        std::cerr << "A benchmark failed with exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
