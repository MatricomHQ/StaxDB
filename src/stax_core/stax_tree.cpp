#include "stax_core/stax_tree.hpp"

StaxTree::StaxTree(NodeAllocator &internal_alloc,
                   CollectionRecordAllocator &record_alloc,
                   std::atomic<uint64_t> &root_ref)
    : internal_node_allocator_(internal_alloc),
      record_allocator_(record_alloc),
      root_ptr_(root_ref) {}

uint32_t StaxTree::find_critical_bit(const char *s1_data, size_t len1, const char *s2_data, size_t len2) const
{
    const size_t min_len = (std::min)(len1, len2);
    size_t diff_byte_idx = 0;

#if defined(__AVX2__) && defined(__BMI__)
    for (; diff_byte_idx + 32 <= min_len; diff_byte_idx += 32)
    {
        __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s1_data + diff_byte_idx));
        __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2_data + diff_byte_idx));
        int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2));
        if (mask != (int)0xFFFFFFFF)
        {
            diff_byte_idx += _tzcnt_u32(~mask);
            goto found_diff_byte;
        }
    }
#endif
#if defined(__SSE2__) || defined(_M_X64) || _M_IX86_FP >= 2
    for (; diff_byte_idx + 16 <= min_len; diff_byte_idx += 16)
    {
        __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(s1 + diff_byte_idx));
        __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(s2 + diff_byte_idx));
        int mask = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, v2));
        if (mask != 0xFFFF)
        {
#if defined(_MSC_VER)
            unsigned long index;
            _BitScanForward(&index, ~mask);
            diff_byte_idx += index;
#else
            diff_byte_idx += __builtin_ctz(~mask);
#endif
            goto found_diff_byte;
        }
    }
#elif defined(__aarch64__)
    for (; diff_byte_idx + 16 <= min_len; diff_byte_idx += 16)
    {
        uint8x16_t v1 = vld1q_u8(reinterpret_cast<const uint8_t *>(s1_data + diff_byte_idx));
        uint8x16_t v2 = vld1q_u8(reinterpret_cast<const uint8_t *>(s2_data + diff_byte_idx));
        uint8x16_t v_xor = veorq_u8(v1, v2);

        uint64x2_t v_xor_64 = vreinterpretq_u64_u8(v_xor);
        if (vgetq_lane_u64(v_xor_64, 0) != 0)
        {
            diff_byte_idx += __builtin_ctzll(vgetq_lane_u64(v_xor_64, 0)) / 8;
            goto found_diff_byte;
        }
        if (vgetq_lane_u64(v_xor_64, 1) != 0)
        {
            diff_byte_idx += 8 + (__builtin_ctzll(vgetq_lane_u64(v_xor_64, 1)) / 8);
            goto found_diff_byte;
        }
    }
#endif

    while (diff_byte_idx < min_len && s1_data[diff_byte_idx] == s2_data[diff_byte_idx])
    {
        diff_byte_idx++;
    }

found_diff_byte:
    if (diff_byte_idx == min_len && len1 == len2)
        return (std::numeric_limits<uint32_t>::max)();

    const uint8_t char1 = (diff_byte_idx < len1) ? static_cast<uint8_t>(s1_data[diff_byte_idx]) : 0;
    const uint8_t char2 = (diff_byte_idx < len2) ? static_cast<uint8_t>(s2_data[diff_byte_idx]) : 0;
    const uint8_t diff = char1 ^ char2;

    return static_cast<uint32_t>((diff_byte_idx * 8) + (count_leading_zeros(static_cast<uint32_t>(diff)) - ((std::numeric_limits<unsigned int>::digits) - 8)));
}

std::atomic<uint64_t> *StaxTree::get_link_from_step(const TraversalStep &step)
{
    if (step.parent_node_idx == PARENT_IS_ROOT)
    {
        return &root_ptr_;
    }

    std::atomic<uint64_t> &left_child_ptr_ref = internal_node_allocator_.get_left_child_ptr(step.parent_node_idx);
    if (left_child_ptr_ref.load(std::memory_order_relaxed) == step.child_ptr)
    {
        return &left_child_ptr_ref;
    }
    else
    {
        return &internal_node_allocator_.get_right_child_ptr(step.parent_node_idx);
    }
}

void StaxTree::insert(const TxnContext &ctx, std::string_view key, std::string_view value, bool is_delete)
{
    thread_local PathBuffer path;
    const char *key_data = key.data();
    const size_t key_len = key.length();

retry_operation:
    path.reset();
    uint64_t current_parent_idx = PARENT_IS_ROOT;
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);

    while (current_ptr != NIL_POINTER)
    {
        if (!path.push({current_parent_idx, current_ptr}))
        {
            path.grow();
            goto retry_operation;
        }

        if (current_ptr & POINTER_TAG_BIT)
        {
            break;
        }

        current_parent_idx = current_ptr;
        bool bit = get_bit(key_data, key_len, internal_node_allocator_.get_bit_index(current_ptr));
        current_ptr = bit ? internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_acquire) : internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_acquire);
    }

    if (path.size() == 0)
    {
        uint32_t new_record_rel_offset;
        void *record_block_ptr = record_allocator_.reserve_record_space(ctx.thread_id, key_len, value.length(), new_record_rel_offset);
        record_allocator_.finalize_record_header_and_data(record_block_ptr, key_len, value.length(), is_delete, ctx.txn_id, CollectionRecordAllocator::NIL_RECORD_OFFSET, key_data, value.data());
        uint64_t new_tagged_ptr = static_cast<uint64_t>(new_record_rel_offset) | POINTER_TAG_BIT;

        uint64_t expected_root = NIL_POINTER;
        if (root_ptr_.compare_exchange_strong(expected_root, new_tagged_ptr, std::memory_order_release, std::memory_order_relaxed))
            return;
        goto retry_operation;
    }

    TraversalStep &leaf_step = path.back();
    uint32_t leaf_record_offset = leaf_step.child_ptr & POINTER_INDEX_MASK;

    const char *existing_key_data;
    uint32_t existing_key_len;
    uint32_t existing_value_len;
    record_allocator_.get_record_key_and_lengths(leaf_record_offset, &existing_key_data, existing_key_len, existing_value_len);

    if (existing_key_data && existing_key_len == key_len && simd_memcmp(existing_key_data, key_data, key_len) == 0)
    {
        uint32_t new_record_rel_offset;
        void *record_block_ptr = record_allocator_.reserve_record_space(ctx.thread_id, key_len, value.length(), new_record_rel_offset);
        record_allocator_.finalize_record_header_and_data(record_block_ptr, key_len, value.length(), is_delete, ctx.txn_id, leaf_record_offset, key_data, value.data());
        uint64_t new_tagged_ptr = static_cast<uint64_t>(new_record_rel_offset) | POINTER_TAG_BIT;

        std::atomic<uint64_t> *link_to_modify = get_link_from_step(leaf_step);
        uint64_t expected_leaf_ptr = leaf_step.child_ptr;
        if (link_to_modify->compare_exchange_strong(expected_leaf_ptr, new_tagged_ptr, std::memory_order_release, std::memory_order_relaxed))
            return;
        goto retry_operation;
    }
    else
    {
        uint32_t critical_bit = find_critical_bit(key_data, key_len, existing_key_data, existing_key_len);

        auto it = std::lower_bound(path.begin(), path.end() - 1, critical_bit,
                                   [&](const TraversalStep &step, uint32_t bit)
                                   {
                                       if (step.child_ptr & POINTER_TAG_BIT)
                                       {
                                           return true;
                                       }
                                       return internal_node_allocator_.get_bit_index(step.child_ptr) < bit;
                                   });

        size_t split_step_index = std::distance(path.begin(), it);

        TraversalStep &split_step = path[split_step_index];

        uint32_t new_record_rel_offset;
        void *record_block_ptr = record_allocator_.reserve_record_space(ctx.thread_id, key_len, value.length(), new_record_rel_offset);
        record_allocator_.finalize_record_header_and_data(record_block_ptr, key_len, value.length(), is_delete, ctx.txn_id, CollectionRecordAllocator::NIL_RECORD_OFFSET, key_data, value.data());
        uint64_t new_tagged_ptr = static_cast<uint64_t>(new_record_rel_offset) | POINTER_TAG_BIT;

        bool existing_key_bit = get_bit(existing_key_data, existing_key_len, critical_bit);

        uint64_t left_child, right_child;
        if (existing_key_bit)
        {
            left_child = new_tagged_ptr;
            right_child = split_step.child_ptr;
        }
        else
        {
            left_child = split_step.child_ptr;
            right_child = new_tagged_ptr;
        }

        uint64_t new_internal_node_idx = internal_node_allocator_.allocate(ctx.thread_id);
        internal_node_allocator_.set_bit_index(new_internal_node_idx, critical_bit);
        internal_node_allocator_.get_left_child_ptr(new_internal_node_idx).store(left_child, std::memory_order_relaxed);
        internal_node_allocator_.get_right_child_ptr(new_internal_node_idx).store(right_child, std::memory_order_relaxed);

        std::atomic<uint64_t> *link_to_modify = get_link_from_step(split_step);
        uint64_t expected_link_value = split_step.child_ptr;
        if (link_to_modify->compare_exchange_strong(expected_link_value, new_internal_node_idx, std::memory_order_release, std::memory_order_relaxed))
            return;

        internal_node_allocator_.deallocate(new_internal_node_idx);
        goto retry_operation;
    }
}

void StaxTree::insert_batch(const TxnContext &ctx, const CoreKVPair *kv_pairs, size_t num_kvs, TransactionBatch &batch)
{
    if (num_kvs == 0)
        return;

    for (size_t i = 0; i < num_kvs; ++i)
    {
        insert(ctx, kv_pairs[i].key, kv_pairs[i].value, false);
        batch.logical_item_count_delta++;
        batch.live_record_bytes_delta += (kv_pairs[i].key.length() + kv_pairs[i].value.length() + CollectionRecordAllocator::HEADER_SIZE);
    }
}

std::optional<RecordData> StaxTree::get(const TxnContext &ctx, std::string_view key) const
{
    uint64_t current_ptr = root_ptr_.load(std::memory_order_relaxed);
    const char *key_data = key.data();
    const size_t key_len = key.length();

    for (int i = 0; i < 256; ++i)
    {
        if ((current_ptr & POINTER_TAG_BIT) || current_ptr == NIL_POINTER)
        {
            break;
        }

        uint32_t bit_index0 = internal_node_allocator_.get_bit_index(current_ptr);
        bool bit0 = get_bit(key_data, key_len, bit_index0);
        uint64_t next_ptr0 = bit0 ? internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_relaxed)
                                  : internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_relaxed);

        if ((next_ptr0 & POINTER_TAG_BIT) || next_ptr0 == NIL_POINTER)
        {
            current_ptr = next_ptr0;
            break;
        }

#if defined(__x86_64__) || defined(__i386__)
        _mm_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr0), _MM_HINT_T0);
#elif defined(__aarch64__)
        __builtin_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr0), 0, 0);
#endif

        uint32_t bit_index1 = internal_node_allocator_.get_bit_index(next_ptr0);
        bool bit1 = get_bit(key_data, key_len, bit_index1);
        uint64_t next_ptr1 = bit1 ? internal_node_allocator_.get_right_child_ptr(next_ptr0).load(std::memory_order_relaxed)
                                  : internal_node_allocator_.get_left_child_ptr(next_ptr0).load(std::memory_order_relaxed);

        if ((next_ptr1 & POINTER_TAG_BIT) || next_ptr1 == NIL_POINTER)
        {
            current_ptr = next_ptr1;
            break;
        }

#if defined(__x86_64__) || defined(__i386__)
        _mm_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr1), _MM_HINT_T0);
#elif defined(__aarch64__)
        __builtin_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr1), 0, 0);
#endif

        uint32_t bit_index2 = internal_node_allocator_.get_bit_index(next_ptr1);
        bool bit2 = get_bit(key_data, key_len, bit_index2);
        uint64_t next_ptr2 = bit2 ? internal_node_allocator_.get_right_child_ptr(next_ptr1).load(std::memory_order_relaxed)
                                  : internal_node_allocator_.get_left_child_ptr(next_ptr1).load(std::memory_order_relaxed);

        if ((next_ptr2 & POINTER_TAG_BIT) || next_ptr2 == NIL_POINTER)
        {
            current_ptr = next_ptr2;
            break;
        }

#if defined(__x86_64__) || defined(__i386__)
        _mm_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr2), _MM_HINT_T0);
#elif defined(__aarch64__)
        __builtin_prefetch(internal_node_allocator_.get_bit_index_ptr(next_ptr2), 0, 0);
#endif

        uint32_t bit_index3 = internal_node_allocator_.get_bit_index(next_ptr2);
        bool bit3 = get_bit(key_data, key_len, bit_index3);
        current_ptr = bit3 ? internal_node_allocator_.get_right_child_ptr(next_ptr2).load(std::memory_order_relaxed)
                           : internal_node_allocator_.get_left_child_ptr(next_ptr2).load(std::memory_order_relaxed);
    }

    if (current_ptr == NIL_POINTER)
        return std::nullopt;

    uint32_t record_rel_offset = current_ptr & POINTER_INDEX_MASK;

    const char *head_key_ptr;
    uint32_t head_key_len;
    uint32_t head_value_len;
    record_allocator_.get_record_key_and_lengths(record_rel_offset, &head_key_ptr, head_key_len, head_value_len);

    if (head_key_ptr == nullptr || head_key_len != key_len || simd_memcmp(head_key_ptr, key_data, key_len) != 0)
    {
        return std::nullopt;
    }

    uint32_t current_version_offset = record_rel_offset;

    if (current_version_offset != CollectionRecordAllocator::NIL_RECORD_OFFSET)
    {
        void *record_address = record_allocator_.get_record_address(current_version_offset);
#if defined(__x86_64__) || defined(__i386__)
        _mm_prefetch(static_cast<const char *>(record_address), _MM_HINT_T0);
#elif defined(__aarch64__)
        __builtin_prefetch(record_address, 0, 0);
#endif
    }

    while (current_version_offset != CollectionRecordAllocator::NIL_RECORD_OFFSET)
    {
        RecordData record = record_allocator_.get_record_data(current_version_offset);

        uint32_t next_version_offset = record.prev_version_rel_offset;
        if (next_version_offset != CollectionRecordAllocator::NIL_RECORD_OFFSET)
        {
            void *next_record_address = record_allocator_.get_record_address(next_version_offset);
#if defined(__x86_64__) || defined(__i386__)
            _mm_prefetch(static_cast<const char *>(next_record_address), _MM_HINT_T0);
#elif defined(__aarch64__)
            __builtin_prefetch(next_record_address, 0, 0);
#endif
        }

        if (record.txn_id <= ctx.read_snapshot_id)
        {
            if (record.is_deleted)
                return std::nullopt;
            return record;
        }
        current_version_offset = next_version_offset;
    }

    return std::nullopt;
}

void StaxTree::multi_get_simd(const TxnContext &ctx, const std::vector<std::string_view> &keys, std::vector<std::optional<RecordData>> &results) const
{
    size_t n = keys.size();
    results.resize(n);
    size_t i = 0;

#if defined(__AVX2__)
    const size_t simd_width = 8;
    if (n >= simd_width)
    {
    }
#elif defined(__aarch64__)
    const size_t simd_width = 4;
    if (n >= simd_width)
    {
    }
#endif

    for (; i < n; ++i)
    {
        results[i] = get(ctx, keys[i]);
    }
}

void StaxTree::remove(const TxnContext &ctx, std::string_view key)
{
    insert(ctx, key, "", true);
}

void StaxTree::seek(std::string_view start_key, std::stack<uint64_t, std::vector<uint64_t>> &path_stack) const
{
    uint64_t current_ptr = root_ptr_.load(std::memory_order_relaxed);
    if (current_ptr == NIL_POINTER)
        return;
    const char *key_data = start_key.data();
    const size_t key_len = start_key.length();

    while (current_ptr != NIL_POINTER)
    {
        path_stack.push(current_ptr);
        if (current_ptr & POINTER_TAG_BIT)
            break;

        uint32_t bit_index = internal_node_allocator_.get_bit_index(current_ptr);
        bool bit = get_bit(key_data, key_len, bit_index);
        uint64_t next_ptr = bit ? internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_relaxed) : internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_relaxed);

        current_ptr = next_ptr;
    }
}

void StaxTree::find_leaf_nodes_recursive(uint64_t current_ptr, std::string_view prefix, std::vector<uint64_t> &leaf_nodes) const
{
    if (current_ptr == NIL_POINTER)
    {
        return;
    }

    if (current_ptr & POINTER_TAG_BIT)
    {
        uint32_t record_rel_offset = current_ptr & POINTER_INDEX_MASK;
        const char *key_ptr;
        uint32_t key_len, value_len;
        record_allocator_.get_record_key_and_lengths(record_rel_offset, &key_ptr, key_len, value_len);
        if (key_ptr && std::string_view(key_ptr, key_len).starts_with(prefix))
        {
            leaf_nodes.push_back(current_ptr);
        }
        return;
    }

    uint32_t bit_index = internal_node_allocator_.get_bit_index(current_ptr);
    size_t byte_idx = bit_index / 8;

    if (byte_idx >= prefix.length())
    {

        find_leaf_nodes_recursive(internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_acquire), prefix, leaf_nodes);
        find_leaf_nodes_recursive(internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_acquire), prefix, leaf_nodes);
    }
    else
    {

        
        find_leaf_nodes_recursive(internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_acquire), prefix, leaf_nodes);
        find_leaf_nodes_recursive(internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_acquire), prefix, leaf_nodes);
    }
}

void StaxTree::find_leaf_nodes_in_range(std::string_view prefix, std::vector<uint64_t> &leaf_nodes) const
{
    uint64_t current_ptr = root_ptr_.load(std::memory_order_acquire);
    if (current_ptr == NIL_POINTER)
    {
        return;
    }

    while (current_ptr != NIL_POINTER && !(current_ptr & POINTER_TAG_BIT))
    {
        uint32_t bit_index = internal_node_allocator_.get_bit_index(current_ptr);
        size_t byte_idx = bit_index / 8;

        if (byte_idx >= prefix.length())
        {

            break;
        }

        bool bit = get_bit(prefix.data(), prefix.length(), bit_index);
        current_ptr = bit ? internal_node_allocator_.get_right_child_ptr(current_ptr).load(std::memory_order_acquire) : internal_node_allocator_.get_left_child_ptr(current_ptr).load(std::memory_order_acquire);
    }

    find_leaf_nodes_recursive(current_ptr, prefix, leaf_nodes);
}