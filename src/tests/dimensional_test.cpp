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
    MockStaxAllocator() { memory_.resize(1024 * 1024); }
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
    std::cout << "Running test: test_apk_generation..." << std::endl;
    const uint32_t D = 2;
    const uint64_t coords[D] = {0xA1B2C3D4E5F60708, 0x1A2B3C4D5E6F7080};
    std::vector<uint8_t> generated_prefix(D * 2);
    uint8_t* write_ptr = generated_prefix.data();
    SpatialKeywords::generate_apk_interleaved_scalar(coords, D, write_ptr);
    const std::vector<uint8_t> expected_prefix = {0xA1, 0x1A, 0xB2, 0x2B};
    assert(generated_prefix == expected_prefix && "test_apk_generation failed");
    std::cout << "SUCCESS" << std::endl;
}

void test_cursor_seek_first() {
    std::cout << "Running test: test_cursor_seek_first..." << std::endl;
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
    assert(cursor_c->is_valid());
    StaxRecord* rec_c = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(cursor_c->get_record_handle())));
    assert(rec_c->get_key() == "cherry");
    std::cout << "SUCCESS" << std::endl;
}

void test_cursor_move_next() {
    std::cout << "Running test: test_cursor_move_next..." << std::endl;
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
        assert(count < keys.size());
        StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(cursor->get_record_handle())));
        assert(rec->get_key() == keys[count]);
        count++;
        cursor->move_next();
    }
    assert(count == keys.size());
    std::cout << "SUCCESS" << std::endl;
}

void test_aabb_tracking() {
    std::cout << "Running test: test_aabb_tracking..." << std::endl;

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
    assert(cursor->is_valid());

    const AABB* aabb = cursor->get_current_aabb();
    assert(aabb != nullptr);
    assert(aabb->min_bounds[0] >= 0xA000000000000000);
    assert(aabb->max_bounds[0] < 0xB000000000000000);
    assert(aabb->min_bounds[1] >= 0x1000000000000000);
    assert(aabb->max_bounds[1] < 0x2000000000000000);

    cursor->move_next();
    assert(cursor->is_valid());

    const AABB* aabb2 = cursor->get_current_aabb();
    assert(aabb2 != nullptr);
    assert(aabb2->min_bounds[0] >= 0xC000000000000000);
    assert(aabb2->max_bounds[0] < 0xD000000000000000);
    assert(aabb2->min_bounds[1] >= 0x3000000000000000);
    assert(aabb2->max_bounds[1] < 0x4000000000000000);

    std::cout << "SUCCESS" << std::endl;
}

// Helper function to create a test tree with some points for box/sphere queries
void setup_dimensional_tree(StaxTree16& tree, ThreadLocalAllocator& local_alloc, const uint32_t D) {
    // Points inside the box [100, 200] x [100, 200]
    const uint64_t p1[] = {150, 150};
    tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p1, D), "p1");

    const uint64_t p2[] = {120, 180};
    tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p2, D), "p2");

    // Points outside the box
    const uint64_t p3[] = {50, 50};
    tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p3, D), "p3");

    const uint64_t p4[] = {250, 250};
    tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p4, D), "p4");

    const uint64_t p5[] = {150, 250}; // outside
    tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(p5, D), "p5");
}

void test_query_box() {
    std::cout << "Running test: test_query_box..." << std::endl;
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);

    setup_dimensional_tree(tree, local_alloc, D);

    AABB query_aabb(D);
    query_aabb.min_bounds[0] = 100;
    query_aabb.min_bounds[1] = 100;
    query_aabb.max_bounds[0] = 200;
    query_aabb.max_bounds[1] = 200;

    QueryStats stats;
    auto results = tree.query_box(query_aabb, stats);

    assert(results.size() == 2);

    std::set<std::string_view> result_values;
    for (void* handle : results) {
        StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
        result_values.insert(rec->get_value_data());
    }

    assert(result_values.count("p1") == 1);
    assert(result_values.count("p2") == 1);
    assert(result_values.count("p3") == 0);
    assert(result_values.count("p4") == 0);
    assert(result_values.count("p5") == 0);

    std::cout << "SUCCESS" << std::endl;
}

void test_query_sphere() {
    std::cout << "Running test: test_query_sphere..." << std::endl;
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
    setup_dimensional_tree(tree, local_alloc, D);

    const uint64_t center[] = {140, 160};
    const long double radius = 15.0L; // radius_sq = 225

    // p1 should be inside, p2 should be outside
    QueryStats stats;
    auto results = tree.query_sphere(center, radius, stats);

    assert(results.size() == 1);

    StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(results[0])));
    assert(rec->get_value_data() == "p1");

    std::cout << "SUCCESS" << std::endl;
}

// Helper function for KNN test
void setup_knn_tree(StaxTree16& tree, ThreadLocalAllocator& local_alloc, const uint32_t D, std::map<std::string, std::array<uint64_t, 2>>& points) {
    points["p1"] = {10, 10};
    points["p2"] = {12, 12}; // close
    points["p3"] = {100, 100};
    points["p4"] = {105, 105};
    points["p5"] = {5, 5}; // closest
    points["p6"] = {200, 200};

    for(const auto& pair : points) {
        tree.insert(local_alloc, TEST_CTX, SpatialKeywords::generate_apk(pair.second.data(), D), pair.first);
    }
}

void test_query_knn() {
    std::cout << "Running test: test_query_knn..." << std::endl;
    FileHeader mock_header;
    mock_header.global_alloc_offset.store(1, std::memory_order_relaxed);
    MockStaxAllocator mock_alloc;
    StaxAllocator allocator_wrapper(&mock_header, mock_alloc.get_base_ptr());
    ThreadLocalAllocator local_alloc(allocator_wrapper);
    std::atomic<uint64_t> root_ptr = 0;
    const uint32_t D = 2;
    StaxTree16 tree(allocator_wrapper, root_ptr, D);
    std::map<std::string, std::array<uint64_t, 2>> points;
    setup_knn_tree(tree, local_alloc, D, points);

    const uint64_t query_point[] = {4, 4};
    const size_t k = 3;

    // Expected order of closeness to {4, 4}:
    // 1. p5 {5, 5}   -> dist_sq = 1^2 + 1^2 = 2
    // 2. p1 {10, 10} -> dist_sq = 6^2 + 6^2 = 72
    // 3. p2 {12, 12} -> dist_sq = 8^2 + 8^2 = 128

    QueryStats stats;
    auto results = tree.query_knn(query_point, k, stats);
    assert(results.size() == k);

    std::set<std::string_view> result_values;
    for (void* handle : results) {
        StaxRecord* rec = allocator_wrapper.get_ptr<StaxRecord>(StaxTree16::get_offset(reinterpret_cast<uint64_t>(handle)));
        result_values.insert(rec->get_value_data());
    }

    assert(result_values.count("p5") == 1);
    assert(result_values.count("p1") == 1);
    assert(result_values.count("p2") == 1);
    assert(result_values.count("p3") == 0);
    assert(result_values.count("p4") == 0);
    assert(result_values.count("p6") == 0);

    std::cout << "SUCCESS" << std::endl;
}

int main() {
    try {
        test_apk_generation();
        test_cursor_seek_first();
        test_cursor_move_next();
        test_aabb_tracking();
        test_query_box();
        test_query_sphere();
        test_query_knn();
        std::cout << "All tests passed." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "A test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
