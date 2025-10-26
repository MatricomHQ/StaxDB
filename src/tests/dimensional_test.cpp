#include "stax_core/stax_new_tree.hpp"
#include "stax_db/arena_structs.h"
#include <iostream>
#include <vector>
#include <cstdint>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <set>
#include <random>
#include <cmath>

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
    // Increased size to handle larger randomized tests
    MockStaxAllocator() { memory_.resize(100 * 1024 * 1024); }
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

// =================================================================================================
// --- Randomized Correctness Test for All Spatial Queries ---
// =================================================================================================

void run_randomized_correctness_test(const std::string& query_type, uint32_t D) {
    std::cout << "\n--- Running Randomized Correctness Test: " << query_type << " (" << D << "D) ---" << std::endl;

    const size_t num_points = 2000;
    const int num_queries = 10;

    // 1. Setup Tree
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    // 2. Generate and insert random data
    std::mt19937_64 g(D); // Seed with dimension for variety
    std::uniform_int_distribution<uint64_t> distrib(0, 100000);
    std::vector<std::vector<uint64_t>> all_points;
    all_points.reserve(num_points);
    for (size_t i = 0; i < num_points; ++i) {
        std::vector<uint64_t> p(D);
        for (uint32_t d = 0; d < D; ++d) p[d] = distrib(g);
        all_points.push_back(p);
        tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p.data(), D), std::to_string(i));
    }

    // 3. Run multiple randomized queries
    for (int i = 0; i < num_queries; ++i) {
        std::cout << "  Running Query " << i + 1 << "/" << num_queries << "..." << std::endl;
        std::set<size_t> stax_results;
        std::set<size_t> ground_truth_results;
        QueryStats stats;

        if (query_type == "Box") {
            AABB query_box(D);
            for(uint32_t d=0; d<D; ++d) {
                uint64_t p1 = distrib(g);
                uint64_t p2 = distrib(g);
                query_box.min_bounds[d] = std::min(p1, p2);
                query_box.max_bounds[d] = std::max(p1, p2);
            }

            auto results = tree.query_box(query_box, stats);
            for (void* handle : results) {
                StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                stax_results.insert(std::stoul(std::string(rec->get_value_data())));
            }

            for(size_t j=0; j<all_points.size(); ++j) {
                bool in_box = true;
                for(uint32_t d=0; d<D; ++d) {
                    if (all_points[j][d] < query_box.min_bounds[d] || all_points[j][d] > query_box.max_bounds[d]) {
                        in_box = false;
                        break;
                    }
                }
                if (in_box) ground_truth_results.insert(j);
            }

        } else if (query_type == "Sphere") {
            std::vector<uint64_t> center(D);
            for(uint32_t d=0; d<D; ++d) center[d] = distrib(g);
            long double radius = distrib(g) / 10.0; // smaller radius
            long double radius_sq = radius * radius;

            auto results = tree.query_sphere(center.data(), radius, stats);
            for (void* handle : results) {
                StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                stax_results.insert(std::stoul(std::string(rec->get_value_data())));
            }

            for(size_t j=0; j<all_points.size(); ++j) {
                if (SpatialKeywords::PointDistSq(all_points[j].data(), center.data(), D) <= radius_sq) {
                    ground_truth_results.insert(j);
                }
            }

        } else if (query_type == "KNN") {
            std::vector<uint64_t> center(D);
            for(uint32_t d=0; d<D; ++d) center[d] = distrib(g);
            size_t k = 15;

            auto results = tree.query_knn(center.data(), k, stats);
            for (void* handle : results) {
                StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
                stax_results.insert(std::stoul(std::string(rec->get_value_data())));
            }

            std::vector<std::pair<long double, size_t>> distances;
            for (size_t j = 0; j < all_points.size(); ++j) {
                distances.push_back({SpatialKeywords::PointDistSq(all_points[j].data(), center.data(), D), j});
            }
            std::sort(distances.begin(), distances.end());
            for (size_t j = 0; j < k; ++j) {
                ground_truth_results.insert(distances[j].second);
            }
        }

        // 4. Compare results and report
        std::cout << "    - StaxDB Found:   " << stax_results.size() << " records." << std::endl;
        std::cout << "    - Expected:       " << ground_truth_results.size() << " records." << std::endl;
        if (stax_results != ground_truth_results) {
            throw std::runtime_error("Result mismatch between StaxDB and ground truth!");
        }
        std::cout << "    - CORRECTNESS CONFIRMED: Results are identical." << std::endl;
    }
    std::cout << "--- SUCCESS: All randomized queries for " << query_type << " (" << D << "D) passed. ---" << std::endl;
}

int main() {
    try {
        run_randomized_correctness_test("Box", 2);
        run_randomized_correctness_test("Sphere", 2);
        run_randomized_correctness_test("KNN", 2);

        run_randomized_correctness_test("Box", 4);
        run_randomized_correctness_test("Sphere", 4);
        run_randomized_correctness_test("KNN", 4);

        run_randomized_correctness_test("Box", 8);
        run_randomized_correctness_test("Sphere", 8);
        run_randomized_correctness_test("KNN", 8);

        std::cout << "\n[OK] All dimensional query correctness tests passed." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "A test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
