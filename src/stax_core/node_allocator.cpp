#include "stax_core/node_allocator.hpp"
#include "stax_db/db.h"

thread_local std::vector<uint64_t> NodeAllocator::thread_local_free_list;

NodeAllocator::NodeAllocator(FileHeader* file_header, uint8_t* mmap_base_addr)
    : file_header_(file_header), mmap_base_addr_(mmap_base_addr)
{
    for (size_t i = 0; i < MAX_CONCURRENT_THREADS; ++i)
    {
        thread_chunks_[i].chunk_base_offset_from_mmap = 0;
        thread_chunks_[i].current_offset_nodes.store(0, std::memory_order_relaxed);
    }
}

uint64_t NodeAllocator::allocate_data_chunk(size_t size_bytes, size_t alignment)
{
    if (!file_header_) {
        throw std::runtime_error("Cannot allocate chunk: file header is null.");
    }

    if ((alignment & (alignment - 1)) != 0)
    {
        throw std::invalid_argument("Alignment must be a power of two.");
    }

    const uint64_t alignment_mask = alignment - 1;
    uint64_t current_offset = file_header_->global_alloc_offset.load(std::memory_order_acquire);

    while (true)
    {
        uint64_t aligned_offset = (current_offset + alignment_mask) & ~alignment_mask;
        uint64_t next_offset = aligned_offset + size_bytes;

        // Note: We don't have access to mmap_size here. A check against DB_MAX_VIRTUAL_SIZE is a safeguard.
        if (next_offset > DB_MAX_VIRTUAL_SIZE)
        {
            throw std::runtime_error("Database out of space during aligned chunk allocation.");
        }

        if (file_header_->global_alloc_offset.compare_exchange_weak(current_offset, next_offset, std::memory_order_release, std::memory_order_acquire))
        {
            return aligned_offset;
        }
    }
}


void NodeAllocator::request_new_chunk(size_t thread_id)
{
    if (thread_id >= MAX_CONCURRENT_THREADS)
    {
        throw std::out_of_range("Thread ID exceeds max threads in NodeAllocator.");
    }

    uint64_t chunk_start_offset = allocate_data_chunk(CHUNK_ALIGNMENT, CHUNK_ALIGNMENT);

    ThreadLocalChunk &chunk = thread_chunks_[thread_id];
    chunk.chunk_base_offset_from_mmap = chunk_start_offset;
    chunk.current_offset_nodes.store(0, std::memory_order_relaxed);
}

uint64_t NodeAllocator::allocate(size_t thread_id)
{
    if (!thread_local_free_list.empty())
    {
        uint64_t recycled_handle = thread_local_free_list.back();
        thread_local_free_list.pop_back();
        return recycled_handle;
    }

    if (thread_id >= MAX_CONCURRENT_THREADS)
    {
        throw std::out_of_range("Thread ID exceeds configured number of threads for NodeAllocator.");
    }

    for (int i = 0; i < 2; ++i)
    {
        ThreadLocalChunk &chunk = thread_chunks_[thread_id];
        if (chunk.chunk_base_offset_from_mmap != 0 && (chunk.current_offset_nodes.load(std::memory_order_relaxed) < NODES_PER_CHUNK))
        {
            uint32_t allocated_node_index = chunk.current_offset_nodes.fetch_add(1, std::memory_order_relaxed);
            if (allocated_node_index < NODES_PER_CHUNK)
            {
                uint64_t handle = chunk.chunk_base_offset_from_mmap + LEFT_CHILD_PTR_ARRAY_OFFSET + (allocated_node_index * sizeof(uint64_t));
                return handle;
            }
        }
        request_new_chunk(thread_id);
    }
    throw std::runtime_error("NodeAllocator: Persistent out of space after attempting to get a new chunk.");
}

void NodeAllocator::deallocate(uint64_t node_handle)
{
    if (node_handle == NIL_INDEX)
        return;
    thread_local_free_list.push_back(node_handle);
}

size_t NodeAllocator::get_total_occupied_size() const
{
    size_t total = 0;
    for (size_t i = 0; i < MAX_CONCURRENT_THREADS; ++i)
    {
        total += thread_chunks_[i].current_offset_nodes.load(std::memory_order_relaxed) * 18;
    }
    return total;
}