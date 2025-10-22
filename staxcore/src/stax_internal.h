#include <iostream>
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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// --- SIMD Intrinsics Headers ---
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386)
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif


// =================================================================================================
// --- Mocked/Stubbed Dependencies ---
// =================================================================================================
#if defined(WASM_BUILD)
constexpr uint64_t DB_MAX_VIRTUAL_SIZE = 256ULL * 1024 * 1024; // 256 MB for WASM
#else
constexpr uint64_t DB_MAX_VIRTUAL_SIZE = 10ULL * 1024 * 1024 * 1024; // 10 GB
#endif
struct FileHeader {
    std::atomic<uint64_t> global_alloc_offset;
    std::atomic<uint64_t> root_ptr;
};

using TxnID = uint64_t;

struct TxnContext {
    uint64_t thread_id;
    uint64_t read_snapshot_id;
    TxnID txn_id;
};

#if defined(_MSC_VER)
#define STAX_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define STAX_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define STAX_ALWAYS_INLINE inline
#endif

STAX_ALWAYS_INLINE void simd_memcpy(void* dest, const void* src, size_t n) {
    char* d = static_cast<char*>(dest);
    const char* s = static_cast<const char*>(src);
    size_t i = 0;

#if defined(__AVX2__)
    while (n - i >= 32) {
        __m256i v = _mm256_loadu_si256((const __m256i*)(s + i));
        _mm256_storeu_si256((__m256i*)(d + i), v);
        i += 32;
    }
#endif

    memcpy(d + i, s + i, n - i);
}

// =================================================================================================
// --- StaxAllocator (Unified mmap Allocator) ---
// =================================================================================================
class StaxAllocator {
private:
    FileHeader* file_header_ = nullptr;
    uint8_t* mmap_base_addr_ = nullptr;

public:
    StaxAllocator(FileHeader* file_header, uint8_t* mmap_base_addr)
        : file_header_(file_header), mmap_base_addr_(mmap_base_addr) {}

    uint64_t allocate(size_t size, size_t alignment = 8) {
        if (!file_header_) {
            throw std::runtime_error("Cannot allocate chunk: file header is null.");
        }
        if ((alignment & (alignment - 1)) != 0) {
            throw std::invalid_argument("Alignment must be a power of two.");
        }
        const uint64_t alignment_mask = alignment - 1;
        uint64_t current_offset = file_header_->global_alloc_offset.load(std::memory_order_relaxed);
        while (true) {
            uint64_t aligned_offset = (current_offset + alignment_mask) & ~alignment_mask;
            uint64_t next_offset = aligned_offset + size;
            if (next_offset > DB_MAX_VIRTUAL_SIZE) {
                throw std::runtime_error("Database out of space.");
            }
            if (file_header_->global_alloc_offset.compare_exchange_weak(current_offset, next_offset, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return aligned_offset;
            }
        }
    }

    void deallocate(uint64_t /*offset*/, size_t /*size*/) {
        // No-op
    }

    template<typename T>
    T* get_ptr(uint64_t offset) const {
        if (offset == 0) return nullptr;
        return reinterpret_cast<T*>(mmap_base_addr_ + offset);
    }
};

// --- ThreadLocalAllocator ---
class ThreadLocalAllocator {
private:
    StaxAllocator& global_allocator_;
    uint64_t arena_offset_ = 0;
    uint64_t current_alloc_ptr_ = 0;
    uint64_t arena_end_ptr_ = 0;

    static constexpr size_t ARENA_SIZE = 64 * 1024;

    void request_new_arena() {
        arena_offset_ = global_allocator_.allocate(ARENA_SIZE, ARENA_SIZE); // Align arenas
        current_alloc_ptr_ = arena_offset_;
        arena_end_ptr_ = arena_offset_ + ARENA_SIZE;
    }

public:
    ThreadLocalAllocator(StaxAllocator& global_allocator) : global_allocator_(global_allocator) {
        request_new_arena();
    }

    uint64_t allocate(size_t size, size_t alignment = 8) {
        const uint64_t alignment_mask = alignment - 1;
        uint64_t aligned_ptr = (current_alloc_ptr_ + alignment_mask) & ~alignment_mask;

        if (aligned_ptr + size > arena_end_ptr_) {
            if (size > ARENA_SIZE) {
                return global_allocator_.allocate(size, alignment);
            }
            request_new_arena();
            aligned_ptr = (current_alloc_ptr_ + alignment_mask) & ~alignment_mask;
            if (aligned_ptr + size > arena_end_ptr_) {
                return global_allocator_.allocate(size, alignment);
            }
        }

        current_alloc_ptr_ = aligned_ptr + size;
        return aligned_ptr;
    }
};

// =================================================================================================
// --- Node and Record Structures ---
// =================================================================================================
struct StaxRecord {
    uint32_t key_len;
    uint32_t value_len;
    TxnID txn_id;
    uint64_t prev_version_offset;
    bool is_deleted;

    char* get_key_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord); }
    const char* get_key_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord); }
    char* get_value_data() { return reinterpret_cast<char*>(this) + sizeof(StaxRecord) + key_len; }
    const char* get_value_data() const { return reinterpret_cast<const char*>(this) + sizeof(StaxRecord) + key_len; }
    std::string_view get_key() const { return std::string_view(get_key_data(), key_len); }
};

struct InternalNode {
    uint64_t representative_leaf_offset;
    std::atomic<uint64_t> children[16];

    InternalNode() : representative_leaf_offset(0) {
        for(int i=0; i<16; ++i) children[i].store(0, std::memory_order_release);
    }
};

// =================================================================================================
// --- StaxTree16 (Niblet Tree Implementation) ---
// =================================================================================================
class StaxTree16 {
private:
    StaxAllocator &allocator_;
    std::atomic<uint64_t> &root_ptr_;

public:
    // Pointer Encoding Details
    static constexpr uint64_t LEAF_TAG = 1ULL;
    static constexpr uint64_t OFFSET_MASK = (1ULL << 48) - 1;
    static constexpr int IDX_SHIFT = 48;

    static STAX_ALWAYS_INLINE bool is_leaf(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t get_leaf_offset(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t get_internal_offset(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t make_leaf_ptr(uint64_t offset);
    static STAX_ALWAYS_INLINE uint32_t get_test_idx(uint64_t ptr);
    static STAX_ALWAYS_INLINE uint64_t make_internal_ptr(uint64_t offset, uint32_t idx);
    static STAX_ALWAYS_INLINE int get_nibble_at(std::string_view key, uint32_t nibble_idx);

public:
    StaxTree16(StaxAllocator &allocator, std::atomic<uint64_t> &root_ref);
    void insert(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete = false);
    StaxRecord* get(const TxnContext &ctx, std::string_view key, size_t key_len) const;
    void remove(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key);
    void range_scan(const TxnContext &ctx, std::string_view start_key, std::string_view end_key, std::vector<StaxRecord*>& results) const;

private:
    // --- Other Private Helpers ---
    STAX_ALWAYS_INLINE int get_child_idx(int nibble, uint32_t test_idx) const;
    STAX_ALWAYS_INLINE int get_nibble_from_child_idx(int child_idx, uint32_t test_idx) const;
    uint64_t allocate_new_record(ThreadLocalAllocator& local_alloc, const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete, uint64_t prev_version_offset);
    static int find_first_differing_nibble(std::string_view k1, std::string_view k2);
    StaxRecord* get_visible_record(StaxRecord* record_head, const TxnContext& ctx) const;
};
