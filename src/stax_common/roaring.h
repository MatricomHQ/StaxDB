#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "stax_common/constants.h"

#ifdef _MSC_VER
#include <intrin.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif





typedef struct roaring_container_s roaring_container_t;

typedef struct {
    roaring_container_t** containers;
    uint16_t* keys; 
    int32_t num_containers;
    int32_t allocation_size;
} roaring_array_t;

typedef struct {
    int32_t cardinality;
    int32_t allocation_size;
    uint16_t* content;
} array_container_t;

typedef struct {
    
    int64_t cardinality;
    uint64_t* bitset;
} bitset_container_t;

struct roaring_container_s {
    bool is_bitset;
    union {
        array_container_t array;
        bitset_container_t bitset;
    } container_data;
};

struct roaring_bitmap_s {
    roaring_array_t high_low_container;
};

typedef struct roaring_bitmap_s roaring_bitmap_t;
typedef struct roaring_uint32_iterator_s roaring_uint32_iterator_t;



roaring_bitmap_t* roaring_bitmap_create(void);
void roaring_bitmap_free(roaring_bitmap_t* r);
roaring_bitmap_t* roaring_bitmap_copy(const roaring_bitmap_t* r_orig);
void roaring_bitmap_add(roaring_bitmap_t* r, uint32_t val);
uint64_t roaring_bitmap_get_cardinality(const roaring_bitmap_t* r);
bool roaring_bitmap_is_empty(const roaring_bitmap_t *r);


void roaring_bitmap_and_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2);
void roaring_bitmap_or_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2);
void roaring_bitmap_andnot_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2);
roaring_bitmap_t* roaring_bitmap_and(const roaring_bitmap_t* r1, const roaring_bitmap_t* r2); 


roaring_uint32_iterator_t* roaring_create_iterator(const roaring_bitmap_t* r);
void roaring_free_iterator(roaring_uint32_iterator_t* it);
uint32_t roaring_read_uint32(roaring_uint32_iterator_t* it, void* val);
void roaring_advance_uint32_iterator(roaring_uint32_iterator_t* it);


bool roaring_bitmap_to_uint32_array(const roaring_bitmap_t *r, uint32_t *out);


size_t roaring_bitmap_size_in_bytes(const roaring_bitmap_t* r);
size_t roaring_bitmap_serialize(const roaring_bitmap_t* r, char* buf);
roaring_bitmap_t* roaring_bitmap_portable_deserialize(const char* buf);

struct roaring_uint32_iterator_s {
    const roaring_bitmap_t* bitmap;
    int32_t container_index;
    int32_t inner_index;
    bool has_next;
};


#ifdef __cplusplus
} 
#endif