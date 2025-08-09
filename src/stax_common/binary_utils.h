#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <cstring> 
#include "stax_common/common_types.hpp"

// --- Start of Final, Robust Bug Fix: Manual, Unconditional Endian Swapping ---

static inline uint64_t stax_bswap_64(uint64_t val) {
    val = ((val << 8) & 0xFF00FF00FF00FF00ULL) | ((val >> 8) & 0x00FF00FF00FF00FFULL);
    val = ((val << 16) & 0xFFFF0000FFFF0000ULL) | ((val >> 16) & 0x0000FFFF0000FFFFULL);
    return (val << 32) | (val >> 32);
}

static inline uint32_t stax_bswap_32(uint32_t val) {
    val = ((val << 8) & 0xFF00FF00) | ((val >> 8) & 0x00FF00FF);
    return (val << 16) | (val >> 16);
}

// A more robust check for endianness. Assume little-endian unless a known big-endian macro is set.
#if defined(__BIG_ENDIAN__) || defined(_BIG_ENDIAN) || defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    #define STAX_BIG_ENDIAN_PLATFORM 1
#else
    #define STAX_LITTLE_ENDIAN_PLATFORM 1
#endif


#if defined(STAX_LITTLE_ENDIAN_PLATFORM)
    #define stax_htobe64(x) stax_bswap_64(x)
    #define stax_be64toh(x) stax_bswap_64(x)
    #define stax_htobe32(x) stax_bswap_32(x)
    #define stax_be32toh(x) stax_bswap_32(x)
#elif defined(STAX_BIG_ENDIAN_PLATFORM)
    #define stax_htobe64(x) (x)
    #define stax_be64toh(x) (x)
    #define stax_htobe32(x) (x)
    #define stax_be32toh(x) (x)
#else
    #error "Could not determine endianness of platform. Please define STAX_LITTLE_ENDIAN_PLATFORM or STAX_BIG_ENDIAN_PLATFORM."
#endif

// --- End of Final Bug Fix ---


inline size_t to_binary_key_buf(uint64_t id, char *buffer, size_t buffer_size)
{
    if (buffer_size < 8)
        return 0;
    uint64_t be_id = stax_htobe64(id);
    memcpy(buffer, &be_id, sizeof(be_id));
    return 8;
}

inline uint64_t from_binary_key_u64(std::string_view s)
{
    if (s.length() != 8)
        return 0;
    uint64_t be_id;
    memcpy(&be_id, s.data(), sizeof(be_id));
    return stax_be64toh(be_id);
}

inline size_t to_binary_key_buf(uint32_t id, char *buffer, size_t buffer_size)
{
    if (buffer_size < 4)
        return 0;
    uint32_t be_id = stax_htobe32(id);
    memcpy(buffer, &be_id, sizeof(be_id));
    return 4;
}

inline uint32_t from_binary_key_u32(std::string_view s)
{
    if (s.length() != 4)
        return 0;
    uint32_t be_id;
    memcpy(&be_id, s.data(), sizeof(be_id));
    return stax_be32toh(be_id);
}


inline std::string to_binary_key(uint32_t id)
{
    std::string s;
    s.resize(4);
    uint32_t be_id = stax_htobe32(id);
    memcpy(&s[0], &be_id, sizeof(be_id));
    return s;
}

inline std::string to_binary_key(uint64_t id)
{
    std::string s;
    s.resize(8);
    uint64_t be_id = stax_htobe64(id);
    memcpy(&s[0], &be_id, sizeof(be_id));
    return s;
}


inline std::string_view to_string_view(StaxSlice s) {
    return std::string_view(s.data, s.len);
}