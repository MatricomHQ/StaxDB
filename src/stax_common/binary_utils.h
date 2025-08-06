#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <cstring> 
#include "stax_common/common_types.hpp"

#if defined(_WIN32)
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#include <stdlib.h>
#define htobe64(x) _byteswap_uint64(x)
#define be64toh(x) _byteswap_uint64(x)
#else
#include <arpa/inet.h>
#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define htobe64(x) OSSwapHostToBigInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#else
#include <endian.h>
#endif
#endif



inline size_t to_binary_key_buf(uint64_t id, char *buffer, size_t buffer_size)
{
    if (buffer_size < 8)
        return 0;
    uint64_t be_id = htobe64(id);
    memcpy(buffer, &be_id, sizeof(be_id));
    return 8;
}

inline uint64_t from_binary_key_u64(std::string_view s)
{
    if (s.length() != 8)
        return 0;
    uint64_t be_id;
    memcpy(&be_id, s.data(), sizeof(be_id));
    return be64toh(be_id);
}

inline size_t to_binary_key_buf(uint32_t id, char *buffer, size_t buffer_size)
{
    if (buffer_size < 4)
        return 0;
    buffer[0] = static_cast<char>((id >> 24) & 0xFF);
    buffer[1] = static_cast<char>((id >> 16) & 0xFF);
    buffer[2] = static_cast<char>((id >> 8) & 0xFF);
    buffer[3] = static_cast<char>(id & 0xFF);
    return 4;
}

inline uint32_t from_binary_key_u32(std::string_view s)
{
    if (s.length() != 4)
        return 0;
    uint32_t id = 0;
    id |= static_cast<uint32_t>(static_cast<unsigned char>(s[0])) << 24;
    id |= static_cast<uint32_t>(static_cast<unsigned char>(s[1])) << 16;
    id |= static_cast<uint32_t>(static_cast<unsigned char>(s[2])) << 8;
    id |= static_cast<uint32_t>(static_cast<unsigned char>(s[3]));
    return id;
}


inline std::string to_binary_key(uint32_t id)
{
    std::string s;
    s.resize(4);
    s[0] = static_cast<char>((id >> 24) & 0xFF);
    s[1] = static_cast<char>((id >> 16) & 0xFF);
    s[2] = static_cast<char>((id >> 8) & 0xFF);
    s[3] = static_cast<char>(id & 0xFF);
    return s;
}

inline std::string to_binary_key(uint64_t id)
{
    std::string s;
    s.resize(8);
    uint64_t be_id = htobe64(id);
    memcpy(&s[0], &be_id, sizeof(be_id));
    return s;
}


inline std::string_view to_string_view(StaxSlice s) {
    return std::string_view(s.data, s.len);
}