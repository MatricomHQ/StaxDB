#include "stax_core/stax_new_tree.hpp"
#include "stax_db/arena_structs.h"
#include <iostream>
#include <vector>
#include <cstdint>
#include <cassert>
#include <iomanip>
#include <map>
#include <algorithm>
#include <set>
#include <cmath>
#include <random>
#include <string>
#include <functional>

// A minimal test context.
const TxnContext TEST_CTX = {1, 1, 0};

// =================================================================================================
// --- Test Helper: Mock Allocator ---
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
    const uint8_t* get_base_ptr() const { return memory_.data(); }
};


// =================================================================================================
// --- Unified Randomized Spatial Query Test Framework ---
// =================================================================================================

// Helper to convert result handles to a sorted list of string values for comparison
std::vector<std::string> handles_to_sorted_values(const std::vector<void*>& handles, StaxAllocator& allocator) {
    std::vector<std::string> values;
    values.reserve(handles.size());
    for (void* handle : handles) {
        StaxRecord* rec = allocator.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
        values.push_back(std::string(rec->get_value_data()));
    }
    std::sort(values.begin(), values.end());
    return values;
}

// The core test function
void run_randomized_spatial_test(
    const std::string& test_name,
    uint32_t D,
    size_t num_points,
    std::function<void(StaxTree16&, const std::vector<std::vector<uint64_t>>&, StaxAllocator&)> test_logic)
{
    std::cout << "------------------------------------------------------" << std::endl;
    std::cout << "Running test: " << test_name << " (" << D << "D, " << num_points << " points)..." << std::endl;

    // 1. Setup Tree and Data
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc(200); // 200MB should be plenty
    StaxAllocator allocator_wrapper(&mock_header, const_cast<uint8_t*>(mock_alloc.get_base_ptr()));
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    std::vector<std::vector<uint64_t>> points(num_points, std::vector<uint64_t>(D));
    std::mt19937_64 g(D); // Seed with dimension for variety but keep it deterministic
    std::uniform_int_distribution<uint64_t> distrib(0, 100000);

    for (size_t i = 0; i < num_points; ++i) {
        for (uint32_t d = 0; d < D; ++d) {
            points[i][d] = distrib(g);
        }
        tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(points[i].data(), D), std::to_string(i));
    }

    // 2. Run the specific query logic
    test_logic(tree, points, allocator_wrapper);
}

// --- Test Logic Implementations ---

void test_logic_box(StaxTree16& tree, const std::vector<std::vector<uint64_t>>& points, StaxAllocator& allocator) {
    const uint32_t D = tree.get_dimensionality();
    AABB query_box(D);
    query_box.min_bounds[0] = 20000; query_box.max_bounds[0] = 80000;
    if (D > 1) { query_box.min_bounds[1] = 10000; query_box.max_bounds[1] = 90000; }
    if (D > 2) { for(uint32_t i=2; i<D; ++i) { query_box.min_bounds[i] = 0; query_box.max_bounds[i] = 100000;} }


    // 1. Get StaxDB results
    QueryStats stats;
    auto stax_handles = tree.query_box(query_box, stats);
    auto stax_results = handles_to_sorted_values(stax_handles, allocator);

    // 2. Get ground truth results
    std::vector<std::string> ground_truth_results;
    for (size_t i = 0; i < points.size(); ++i) {
        bool in_box = true;
        for (uint32_t d = 0; d < D; ++d) {
            if (points[i][d] < query_box.min_bounds[d] || points[i][d] > query_box.max_bounds[d]) {
                in_box = false;
                break;
            }
        }
        if (in_box) {
            ground_truth_results.push_back(std::to_string(i));
        }
    }
    std::sort(ground_truth_results.begin(), ground_truth_results.end());

    // 3. Compare and verify
    std::cout << "  StaxDB found " << stax_results.size() << " records." << std::endl;
    std::cout << "  Ground Truth expects " << ground_truth_results.size() << " records." << std::endl;

    assert(stax_results.size() == ground_truth_results.size() && "Result counts do not match!");
    assert(stax_results == ground_truth_results && "Result sets do not match!");
}

void test_logic_sphere(StaxTree16& tree, const std::vector<std::vector<uint64_t>>& points, StaxAllocator& allocator) {
    const uint32_t D = tree.get_dimensionality();
    std::vector<uint64_t> center(D, 50000);
    long double radius = 15000.0L;
    long double radius_sq = radius * radius;

    // 1. Get StaxDB results
    QueryStats stats;
    auto stax_handles = tree.query_sphere(center.data(), radius, stats);
    auto stax_results = handles_to_sorted_values(stax_handles, allocator);

    // 2. Get ground truth results
    std::vector<std::string> ground_truth_results;
    for (size_t i = 0; i < points.size(); ++i) {
        if (SpatialKeywords::PointDistSq(points[i].data(), center.data(), D) <= radius_sq) {
            ground_truth_results.push_back(std::to_string(i));
        }
    }
    std::sort(ground_truth_results.begin(), ground_truth_results.end());

    // 3. Compare and verify
    std::cout << "  StaxDB found " << stax_results.size() << " records." << std::endl;
    std::cout << "  Ground Truth expects " << ground_truth_results.size() << " records." << std::endl;
    assert(stax_results.size() == ground_truth_results.size() && "Result counts do not match!");
    assert(stax_results == ground_truth_results && "Result sets do not match!");
}

void test_logic_knn(StaxTree16& tree, const std::vector<std::vector<uint64_t>>& points, StaxAllocator& allocator) {
    const uint32_t D = tree.get_dimensionality();
    std::vector<uint64_t> query_point(D, 48000);
    int k = 15;
    if (points.size() < (size_t)k) k = points.size();

    // 1. Get StaxDB results
    QueryStats stats;
    auto stax_handles = tree.query_knn(query_point.data(), k, stats);
    auto stax_results = handles_to_sorted_values(stax_handles, allocator);

    // 2. Get ground truth results
    std::vector<std::pair<long double, std::string>> all_distances;
    for (size_t i = 0; i < points.size(); ++i) {
        long double dist_sq = SpatialKeywords::PointDistSq(points[i].data(), query_point.data(), D);
        all_distances.push_back({dist_sq, std::to_string(i)});
    }
    std::sort(all_distances.begin(), all_distances.end());
    std::vector<std::string> ground_truth_results;
    for (int i = 0; i < k; ++i) {
        ground_truth_results.push_back(all_distances[i].second);
    }
    std::sort(ground_truth_results.begin(), ground_truth_results.end());

    // 3. Compare and verify
    std::cout << "  StaxDB found " << stax_results.size() << " records for k=" << k << "." << std::endl;
    std::cout << "  Ground Truth expects " << ground_truth_results.size() << " records for k=" << k << "." << std::endl;
    assert(stax_results.size() == ground_truth_results.size() && "Result counts do not match!");
    assert(stax_results == ground_truth_results && "Result sets do not match!");
}


int main() {
    try {
        run_randomized_spatial_test("Box Query Correctness", 2, 2000, test_logic_box);
        run_randomized_spatial_test("Box Query Correctness", 3, 2000, test_logic_box);
        run_randomized_spatial_test("Box Query Correctness", 8, 1000, test_logic_box);

        run_randomized_spatial_test("Sphere Query Correctness", 2, 2000, test_logic_sphere);
        run_randomized_spatial_test("Sphere Query Correctness", 3, 2000, test_logic_sphere);
        run_randomized_spatial_test("Sphere Query Correctness", 8, 1000, test_logic_sphere);

        run_randomized_spatial_test("KNN Query Correctness", 2, 2000, test_logic_knn);
        run_randomized_spatial_test("KNN Query Correctness", 3, 2000, test_logic_knn);
        run_randomized_spatial_test("KNN Query Correctness", 8, 1000, test_logic_knn);

        std::cout << "\nAll dimensional query correctness tests passed." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "A test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
