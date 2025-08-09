#include "stax_core/node_allocator.hpp"
#include "stax_db/db.h"

thread_local std::vector<uint64_t> NodeAllocator::thread_local_free_list;

NodeAllocator::NodeAllocator(Database *parent_db, uint8_t *mmap_base_addr)
    : parent_db_(parent_db), mmap_base_addr_(mmap_base_addr)
{
    for (size_t i = 0; i < MAX_CONCURRENT_THREADS; ++i)
    {
        thread_chunks_[i].chunk_base_offset_from_mmap = 0;
        thread_chunks_[i].current_offset_nodes.store(0, std::memory_order_relaxed);
    }
}

void NodeAllocator::request_new_chunk(size_t thread_id)
{
    if (thread_id >= MAX_CONCURRENT_THREADS)
    {
        throw std::out_of_range("Thread ID exceeds max threads in NodeAllocator.");
    }

    uint64_t chunk_start_offset = parent_db_->allocate_data_chunk(CHUNK_ALIGNMENT, CHUNK_ALIGNMENT);

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