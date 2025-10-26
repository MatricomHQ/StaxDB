#include "stax_core/stax_new_tree.hpp"
#include "stax_db/arena_structs.h"
#include <iostream>
#include <vector>
#include <cstdint>
#include <iomanip>
#include <map>
#include <algorithm>
#include <set>
#include <cmath>
#include <stdexcept>
#include <string>

// A simple test assertion handler.
void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error("Assertion failed: " + message);
    }
}

// A minimal test context.
const TxnContext TEST_CTX = {1, 1, 0};

// =================================================================================================
// --- Test Helper: Mock Allocator and Tree Setup ---
// =================================================================================================
class MockStaxAllocator {
private:
    std::vector<uint8_t> memory_;
    size_t next_offset_ = 1;
public:
    MockStaxAllocator() { memory_.resize(1024 * 1024 * 10); } // 10MB
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
// --- Test Cases ---
// =================================================================================================

void test_apk_generation() {
    const uint32_t D = 2;
    const uint64_t coords[D] = {0xA1B2C3D4E5F60708, 0x1A2B3C4D5E6F7080};
    std::vector<uint8_t> generated_prefix(D * 2);
    uint8_t* write_ptr = generated_prefix.data();
    SpatialKeywords::generate_apk_interleaved_scalar(coords, D, write_ptr);
    const std::vector<uint8_t> expected_prefix = {0xA1, 0x1A, 0xB2, 0x2B};
    check(generated_prefix == expected_prefix, "test_apk_generation failed");
}

void test_cursor_seek_first() {
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator_wrapper, root_ptr, 0);
    std::map<std::string, std::string> test_data = {
        {"apple", "red"}, {"banana", "yellow"}, {"cherry", "red"},
        {"date", "brown"}, {"grape", "purple"}
    };
    for(const auto& pair : test_data) tree.insert(local_alloc, TEST_CTX, pair.first, pair.second);
    auto cursor_c = tree.create_cursor("blueberry", "grape", false);
    cursor_c->seek_first();
    check(cursor_c->is_valid(), "cursor should be valid");
    StaxRecord* rec_c = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(cursor_c->get_record_handle())));
    check(rec_c->get_key() == "cherry", "seek_first should find 'cherry'");
}

void test_cursor_move_next() {
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    StaxTree16 tree(allocator_wrapper, root_ptr, 0);
    std::vector<std::string> keys = {"d", "a", "c", "e", "bb", "ba", "b", "bc"};
    for(const auto& k : keys) tree.insert(local_alloc, TEST_CTX, k, "v");
    std::sort(keys.begin(), keys.end());
    auto cursor = tree.create_cursor("", "z", false);
    cursor->seek_first();
    size_t count = 0;
    while(cursor->is_valid()) {
        check(count < keys.size(), "move_next iterated too many times");
        StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(cursor->get_record_handle())));
        check(rec->get_key() == keys[count], "cursor not visiting keys in lexicographical order");
        count++;
        cursor->move_next();
    }
    check(count == keys.size(), "move_next did not visit all keys");
}

void test_aabb_tracking() {
    // 1. Setup
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    const uint64_t coords_a[] = {0xA1B2C3D4E5F60708, 0x1A2B3C4D5E6F7080};
    std::string key_a = SpatialKeywords::generate_apk(coords_a, D);
    tree.insert(local_alloc, TEST_CTX, key_a, "value_a");

    const uint64_t coords_b[] = {0xC1B2C3D4E5F60708, 0x3A2B3C4D5E6F7080};
    std::string key_b = SpatialKeywords::generate_apk(coords_b, D);
    tree.insert(local_alloc, TEST_CTX, key_b, "value_b");

    auto cursor = tree.create_cursor("", "z", true);
    cursor->seek_first();
    check(cursor->is_valid(), "AABB test cursor should be valid after seek");

    const AABB* aabb = cursor->get_current_aabb();
    check(aabb != nullptr, "AABB should not be null");
    check(aabb->min_bounds[0] >= 0xA000000000000000, "AABB min bound 0 incorrect for key_a");
    check(aabb->max_bounds[0] < 0xB000000000000000, "AABB max bound 0 incorrect for key_a");
    check(aabb->min_bounds[1] >= 0x1000000000000000, "AABB min bound 1 incorrect for key_a");
    check(aabb->max_bounds[1] < 0x2000000000000000, "AABB max bound 1 incorrect for key_a");

    cursor->move_next();
    check(cursor->is_valid(), "AABB test cursor should be valid after move_next");

    const AABB* aabb2 = cursor->get_current_aabb();
    check(aabb2 != nullptr, "AABB should not be null on second item");
    check(aabb2->min_bounds[0] >= 0xC000000000000000, "AABB min bound 0 incorrect for key_b");
    check(aabb2->max_bounds[0] < 0xD000000000000000, "AABB max bound 0 incorrect for key_b");
    check(aabb2->min_bounds[1] >= 0x3000000000000000, "AABB min bound 1 incorrect for key_b");
    check(aabb2->max_bounds[1] < 0x4000000000000000, "AABB max bound 1 incorrect for key_b");
}

// Helper to get value from handle
std::string_view get_value(MockStaxAllocator& alloc, void* handle) {
    StaxRecord* rec = alloc.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
    return rec->get_value_data();
}

// Brute-force checker for spatial queries
template<typename PointContainer>
void verify_results(MockStaxAllocator& alloc, const std::vector<void*>& results, const PointContainer& expected_points) {
    check(results.size() == expected_points.size(), "Result count mismatch. Expected " + std::to_string(expected_points.size()) + ", got " + std::to_string(results.size()));

    std::set<std::string_view> result_values;
    for (void* handle : results) {
        result_values.insert(get_value(alloc, handle));
    }

    for (const auto& expected_val : expected_points) {
        check(result_values.count(expected_val) == 1, "Expected result '" + std::string(expected_val) + "' not found in query results.");
    }
}


void test_query_box() {
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    // Points inside the box [100, 200] x [100, 200]
    const uint64_t p1[] = {150, 150}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p1, D), "p1");
    const uint64_t p2[] = {120, 180}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p2, D), "p2");
    // Points outside the box
    const uint64_t p3[] = {50, 50};   tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p3, D), "p3");
    const uint64_t p4[] = {250, 250}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p4, D), "p4");
    const uint64_t p5[] = {150, 250}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p5, D), "p5");

    AABB query_aabb(D);
    query_aabb.min_bounds[0] = 100;
    query_aabb.min_bounds[1] = 100;
    query_aabb.max_bounds[0] = 200;
    query_aabb.max_bounds[1] = 200;

    QueryStats stats;
    auto results = tree.query_box(query_aabb, stats);

    verify_results(mock_alloc, results, std::set<std::string_view>{"p1", "p2"});
}

void test_query_sphere() {
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    // Center: 140, 160
    // Points:
    // p1 = {150, 150} -> dist_sq = (10^2) + (-10^2) = 100 + 100 = 200
    // p2 = {120, 180} -> dist_sq = (-20^2) + (20^2) = 400 + 400 = 800
    // p3 = {50, 50}, p4 = {250, 250}, p5 = {150, 250} are all far away
    const uint64_t p1[] = {150, 150}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p1, D), "p1");
    const uint64_t p2[] = {120, 180}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p2, D), "p2");
    const uint64_t p3[] = {50, 50};   tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p3, D), "p3");
    const uint64_t p4[] = {250, 250}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p4, D), "p4");
    const uint64_t p5[] = {150, 250}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p5, D), "p5");

    const uint64_t center[] = {140, 160};
    const long double radius = 15.0L; // radius_sq = 225

    // p1 should be inside, p2 should be outside
    QueryStats stats;
    auto results = tree.query_sphere(center, radius, stats);

    verify_results(mock_alloc, results, std::set<std::string_view>{"p1"});
}

void test_query_knn() {
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    const uint64_t p1[] = {10, 10};   tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p1, D), "p1");
    const uint64_t p2[] = {12, 12};   tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p2, D), "p2"); // close
    const uint64_t p3[] = {100, 100}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p3, D), "p3");
    const uint64_t p4[] = {105, 105}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p4, D), "p4");
    const uint64_t p5[] = {5, 5};     tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p5, D), "p5");   // closest
    const uint64_t p6[] = {200, 200}; tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p6, D), "p6");


    const uint64_t query_point[] = {4, 4};
    const size_t k = 3;

    // Expected order of closeness to {4, 4}:
    // 1. p5 {5, 5}   -> dist_sq = 1^2 + 1^2 = 2
    // 2. p1 {10, 10} -> dist_sq = 6^2 + 6^2 = 72
    // 3. p2 {12, 12} -> dist_sq = 8^2 + 8^2 = 128
    // The rest are much further.

    QueryStats stats;
    auto results = tree.query_knn(query_point, k, stats);

    verify_results(mock_alloc, results, std::set<std::string_view>{"p5", "p1", "p2"});
}

void run_test(void (*test_func)(), const std::string& test_name) {
    std::cout << "Running test: " << test_name << "..." << std::endl;
    try {
        test_func();
        std::cout << "SUCCESS" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "FAILED: " << e.what() << std::endl;
        throw;
    }
}

int main() {
    int failed_tests = 0;
    auto run = [&](void (*test_func)(), const std::string& test_name) {
        std::cout << "Running test: " << test_name << "..." << std::endl;
        try {
            test_func();
            std::cout << "SUCCESS" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "FAILED: " << e.what() << std::endl;
            failed_tests++;
        }
    };

    run(test_apk_generation, "test_apk_generation");
    run(test_cursor_seek_first, "test_cursor_seek_first");
    run(test_cursor_move_next, "test_cursor_move_next");
    run(test_aabb_tracking, "test_aabb_tracking");
    run(test_query_box, "test_query_box");
    run(test_query_sphere, "test_query_sphere");
    run(test_query_knn, "test_query_knn");

    if (failed_tests > 0) {
        std::cerr << "\n" << failed_tests << " test(s) failed." << std::endl;
        return 1;
    }

    std::cout << "\nAll tests passed." << std::endl;
    return 0;
}
