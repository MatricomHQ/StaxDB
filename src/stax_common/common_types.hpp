#pragma once

#include <cstdint>
#include <atomic>
#include <type_traits>
#include <string_view>
#include <limits>


typedef uint64_t TxnID;

enum class FieldType : uint16_t
{
    UNKNOWN = 0,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    BOOL,
    DOUBLE,
    STRING
};

#pragma pack(push, 1)
struct FieldEntry
{
    uint16_t field_id;
    FieldType type_tag;
    uint32_t offset_in_pool;
    uint32_t length_in_pool;
};
#pragma pack(pop)
static_assert(sizeof(FieldEntry) == 12, "FieldEntry must be 12 bytes");
static_assert(std::is_standard_layout<FieldEntry>::value, "FieldEntry must be standard layout");

#pragma pack(push, 1)
struct FlexDocHeader
{
    uint16_t field_count;
};
#pragma pack(pop)
static_assert(sizeof(FlexDocHeader) == 2, "FlexDocHeader must be 2 bytes");
static_assert(std::is_standard_layout<FlexDocHeader>::value, "FlexDocHeader must be standard layout");

struct DataView
{
    const char *data;
    size_t len;

    DataView() : data(nullptr), len(0) {}

    DataView(const char *ptr, size_t l) : data(ptr), len(l) {}

    operator std::string_view() const
    {
        return std::string_view(data, len);
    }
};

struct CoreKVPair
{
    std::string_view key;
    std::string_view value;
};


struct StaxSlice {
    const char* data;
    size_t len;
};

enum StaxPropertyType {
    STAX_PROP_STRING,
    STAX_PROP_NUMERIC,
    STAX_PROP_GEO
};

struct StaxGeoPoint {
    double lat;
    double lon;
};

union StaxPropertyValue {
    StaxSlice string_val;
    uint64_t numeric_val;
    StaxGeoPoint geo_val;
};

struct StaxObjectProperty {
    StaxSlice field;
    StaxPropertyType type;
    StaxPropertyValue value;
};