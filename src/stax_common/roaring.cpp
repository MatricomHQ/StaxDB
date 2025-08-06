

#include "stax_common/roaring.h" 


#define ROARING_ARRAY_TO_BITSET_CONVERSION_THRESHOLD ROARING_ARRAY_TO_BITSET_CONVERSION_THRESHOLD_INTERNAL
#define ROARING_BITSET_CONTAINER_SIZE_IN_ELEMENTS ROARING_BITSET_CONTAINER_SIZE_IN_ELEMENTS_INTERNAL
#define ROARING_BITSET_CONTAINER_SIZE_IN_U64 ROARING_BITSET_CONTAINER_SIZE_IN_U64_INTERNAL




static uint16_t* binary_search_uint16_array(uint16_t* arr, int32_t size, uint16_t val) {
    int32_t low = 0;
    int32_t high = size - 1;
    while (low <= high) {
        int32_t mid = low + (high - low) / 2;
        if (arr[mid] == val) {
            return arr + mid;
        } else if (arr[mid] < val) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return arr + low;
}

static roaring_container_t* roaring_container_create_array() {
    roaring_container_t* c = (roaring_container_t*)malloc(sizeof(roaring_container_t));
    if (!c) return NULL;
    c->is_bitset = false;
    c->container_data.array.cardinality = 0;
    c->container_data.array.allocation_size = 4;
    c->container_data.array.content = (uint16_t*)malloc(c->container_data.array.allocation_size * sizeof(uint16_t));
    return c;
}

static roaring_container_t* roaring_container_create_bitset() {
    roaring_container_t* c = (roaring_container_t*)malloc(sizeof(roaring_container_t));
    if (!c) return NULL;
    c->is_bitset = true;
    c->container_data.bitset.cardinality = -1;
    c->container_data.bitset.bitset = (uint64_t*)calloc(ROARING_BITSET_CONTAINER_SIZE_IN_U64, sizeof(uint64_t));
    return c;
}

static void roaring_container_free(roaring_container_t* c) {
    if (!c) return;
    if (c->is_bitset) {
        free(c->container_data.bitset.bitset);
    } else {
        free(c->container_data.array.content);
    }
    free(c);
}

static roaring_container_t* roaring_container_copy(const roaring_container_t* c_orig); 
static void roaring_container_to_bitset(roaring_container_t** c_ptr);
static void roaring_container_add(roaring_container_t** c_ptr, uint16_t val);
static roaring_container_t* roaring_container_andnot(const roaring_container_t* c1, const roaring_container_t* c2);
static bool roaring_container_contains(const roaring_container_t* c, uint16_t val);

static void roaring_container_add(roaring_container_t** c_ptr, uint16_t val) {
    roaring_container_t* c = *c_ptr;
    if (c->is_bitset) {
        bitset_container_t* bitset_c = &c->container_data.bitset;
        uint64_t* word_ptr = &bitset_c->bitset[val / 64];
        uint64_t mask = 1ULL << (val % 64);
        if (!(*word_ptr & mask)) {
            *word_ptr |= mask;
            if (bitset_c->cardinality != -1) bitset_c->cardinality++;
        }
    } else {
        array_container_t* array_c = &c->container_data.array;
        uint16_t* location = binary_search_uint16_array(array_c->content, array_c->cardinality, val);
        
        if (location != array_c->content + array_c->cardinality && *location == val) {
            return;
        }

        if (array_c->cardinality >= ROARING_ARRAY_TO_BITSET_CONVERSION_THRESHOLD) {
            roaring_container_to_bitset(c_ptr);
            roaring_container_add(c_ptr, val);
            return;
        }

        if (array_c->cardinality >= array_c->allocation_size) {
            array_c->allocation_size *= 2;
            uint16_t* new_content = (uint16_t*)realloc(array_c->content, array_c->allocation_size * sizeof(uint16_t));
            if (!new_content) {
                roaring_container_to_bitset(c_ptr);
                roaring_container_add(c_ptr, val);
                return;
            }
            array_c->content = new_content;
            location = binary_search_uint16_array(array_c->content, array_c->cardinality, val);
        }

        size_t insert_pos = location - array_c->content;
        memmove(array_c->content + insert_pos + 1, array_c->content + insert_pos, (array_c->cardinality - insert_pos) * sizeof(uint16_t));
        array_c->content[insert_pos] = val;
        array_c->cardinality++;
    }
}

static bool roaring_container_contains(const roaring_container_t* c, uint16_t val) {
    if (c->is_bitset) {
        return (c->container_data.bitset.bitset[val / 64] & (1ULL << (val % 64))) != 0;
    } else {
        const array_container_t* array_c = &c->container_data.array;
        uint16_t* location = binary_search_uint16_array(array_c->content, array_c->cardinality, val);
        return (location != array_c->content + array_c->cardinality && *location == val);
    }
}

static int64_t roaring_container_get_cardinality(const roaring_container_t* c) {
    if (!c) return 0;
    if (c->is_bitset) {
        bitset_container_t* bitset_c = (bitset_container_t*)&c->container_data.bitset;
        if (bitset_c->cardinality != -1) return bitset_c->cardinality;
        int64_t card = 0;
        for (int i = 0; i < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++i) {
            uint64_t word = bitset_c->bitset[i];
            #if defined(__GNUC__) || defined(__clang__)
            card += __builtin_popcountll(word);
            #elif defined(_MSC_VER)
            card += __popcnt64(word);
            #else
            word = word - ((word >> 1) & 0x5555555555555555ULL);
            word = (word & 0x3333333333333333ULL) + ((word >> 2) & 0x3333333333333333ULL);
            card += (((word + (word >> 4)) & 0xF0F0F0F0F0F0F0FULL) * 0x101010101010101ULL) >> 56;
            #endif
        }
        bitset_c->cardinality = card;
        return card;
    } else {
        return c->container_data.array.cardinality;
    }
}

static roaring_container_t* roaring_container_and(const roaring_container_t* c1, const roaring_container_t* c2) {
    if (!c1 || !c2) return NULL;
    roaring_container_t* result;
    if (c1->is_bitset && c2->is_bitset) {
        result = roaring_container_create_bitset();
        if (!result) return NULL;
        for (int i = 0; i < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++i) {
            result->container_data.bitset.bitset[i] = c1->container_data.bitset.bitset[i] & c2->container_data.bitset.bitset[i];
        }
        result->container_data.bitset.cardinality = -1;
        return result;
    }
    if (c1->is_bitset) {
        result = roaring_container_create_array();
        if (!result) return NULL;
        const bitset_container_t* bitset_c = &c1->container_data.bitset;
        const array_container_t* array_c = &c2->container_data.array;
        for (int i = 0; i < array_c->cardinality; ++i) {
            uint16_t val = array_c->content[i];
            if ((bitset_c->bitset[val / 64] & (1ULL << (val % 64))) != 0) {
                roaring_container_add(&result, val);
            }
        }
    } else if (c2->is_bitset) {
        result = roaring_container_create_array();
        if (!result) return NULL;
        const bitset_container_t* bitset_c = &c2->container_data.bitset;
        const array_container_t* array_c = &c1->container_data.array;
        for (int i = 0; i < array_c->cardinality; ++i) {
            uint16_t val = array_c->content[i];
            if ((bitset_c->bitset[val / 64] & (1ULL << (val % 64))) != 0) {
                roaring_container_add(&result, val);
            }
        }
    } else {
        result = roaring_container_create_array();
        if (!result) return NULL;
        const array_container_t* arr1 = &c1->container_data.array;
        const array_container_t* arr2 = &c2->container_data.array;
        int32_t i1 = 0, i2 = 0;
        while(i1 < arr1->cardinality && i2 < arr2->cardinality) {
            if (arr1->content[i1] < arr2->content[i2]) i1++;
            else if (arr1->content[i1] > arr2->content[i2]) i2++;
            else { 
                roaring_container_add(&result, arr1->content[i1]);
                i1++; i2++; 
            }
        }
    }
    return result;
}

static roaring_container_t* roaring_container_or(const roaring_container_t* c1, const roaring_container_t* c2) {
    if (!c1) return roaring_container_copy(c2);
    if (!c2) return roaring_container_copy(c1);
    roaring_container_t* result;
    if (c1->is_bitset || c2->is_bitset) {
        result = roaring_container_create_bitset();
        if (!result) return NULL;
        const roaring_container_t *bitset_c = c1->is_bitset ? c1 : c2;
        const roaring_container_t *other_c = c1->is_bitset ? c2 : c1;
        memcpy(result->container_data.bitset.bitset, bitset_c->container_data.bitset.bitset, ROARING_BITSET_CONTAINER_SIZE_IN_U64 * sizeof(uint64_t));
        if (other_c->is_bitset) {
             for (int i = 0; i < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++i) {
                result->container_data.bitset.bitset[i] |= other_c->container_data.bitset.bitset[i];
             }
        } else {
            for (int i = 0; i < other_c->container_data.array.cardinality; ++i) {
                 uint16_t val = other_c->container_data.array.content[i];
                 result->container_data.bitset.bitset[val / 64] |= (1ULL << (val % 64));
            }
        }
        result->container_data.bitset.cardinality = -1;
    } else {
        result = roaring_container_create_array();
        if (!result) return NULL;
        array_container_t* res_array = &result->container_data.array;
        const array_container_t* arr1 = &c1->container_data.array;
        const array_container_t* arr2 = &c2->container_data.array;
        int32_t total_card_estimate = arr1->cardinality + arr2->cardinality;
        if(total_card_estimate > ROARING_ARRAY_TO_BITSET_CONVERSION_THRESHOLD) {
             roaring_container_free(result);
             roaring_container_t* c1_temp = roaring_container_copy(c1);
             if (!c1_temp) return NULL;
             roaring_container_to_bitset(&c1_temp);
             result = roaring_container_or(c1_temp, c2);
             roaring_container_free(c1_temp);
             return result;
        }
        res_array->allocation_size = total_card_estimate;
        res_array->content = (uint16_t*)realloc(res_array->content, res_array->allocation_size * sizeof(uint16_t));
        if (!res_array->content && res_array->allocation_size > 0) {
            roaring_container_free(result);
            return NULL;
        }

        int32_t i1 = 0, i2 = 0, pos = 0;
        while(i1 < arr1->cardinality && i2 < arr2->cardinality) {
            if (arr1->content[i1] < arr2->content[i2]) res_array->content[pos++] = arr1->content[i1++];
            else if (arr1->content[i1] > arr2->content[i2]) res_array->content[pos++] = arr2->content[i2++];
            else { res_array->content[pos++] = arr1->content[i1++]; i2++; }
        }
        while(i1 < arr1->cardinality) res_array->content[pos++] = arr1->content[i1++];
        while(i2 < arr2->cardinality) res_array->content[pos++] = arr2->content[i2++];
        res_array->cardinality = pos;
    }
    return result;
}

static roaring_container_t* roaring_container_andnot(const roaring_container_t* c1, const roaring_container_t* c2) {
    if (!c1) return NULL;
    if (!c2) return roaring_container_copy(c1);

    roaring_container_t* result;
    if (c1->is_bitset && c2->is_bitset) {
        result = roaring_container_create_bitset();
        if (!result) return NULL;
        for (int i = 0; i < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++i) {
            result->container_data.bitset.bitset[i] = c1->container_data.bitset.bitset[i] & (~c2->container_data.bitset.bitset[i]);
        }
        result->container_data.bitset.cardinality = -1;
        return result;
    }
    if (!c1->is_bitset) {
        result = roaring_container_create_array();
        if (!result) return NULL;
        const array_container_t* arr1 = &c1->container_data.array;
        for (int i = 0; i < arr1->cardinality; ++i) {
            uint16_t val = arr1->content[i];
            if (!roaring_container_contains(c2, val)) {
                roaring_container_add(&result, val);
            }
        }
    } else {
        roaring_container_t* c2_bitset = roaring_container_copy(c2);
        if (!c2_bitset) return NULL;
        roaring_container_to_bitset(&c2_bitset);
        
        result = roaring_container_andnot(c1, c2_bitset);
        roaring_container_free(c2_bitset);
    }
    return result;
}

static void roaring_container_to_bitset(roaring_container_t** c_ptr) {
    roaring_container_t* c = *c_ptr;
    if (c->is_bitset) return;

    roaring_container_t* new_c = roaring_container_create_bitset();
    if (!new_c) return;
    
    array_container_t* array_c = &c->container_data.array;
    bitset_container_t* bitset_c = &new_c->container_data.bitset;

    for (int i = 0; i < array_c->cardinality; ++i) {
        uint16_t val = array_c->content[i];
        bitset_c->bitset[val / 64] |= (1ULL << (val % 64));
    }
    bitset_c->cardinality = array_c->cardinality;
    
    roaring_container_free(c);
    *c_ptr = new_c;
}

static roaring_container_t* roaring_container_copy(const roaring_container_t* c_orig) {
    if (!c_orig) return NULL;
    if (c_orig->is_bitset) {
        roaring_container_t* c_copy = roaring_container_create_bitset();
        if (!c_copy) return NULL;
        memcpy(c_copy->container_data.bitset.bitset, c_orig->container_data.bitset.bitset, ROARING_BITSET_CONTAINER_SIZE_IN_U64 * sizeof(uint64_t));
        c_copy->container_data.bitset.cardinality = c_orig->container_data.bitset.cardinality;
        return c_copy;
    } else {
        roaring_container_t* c_copy = roaring_container_create_array();
        if (!c_copy) return NULL;
        int32_t card = c_orig->container_data.array.cardinality;
        c_copy->container_data.array.allocation_size = card;
        c_copy->container_data.array.cardinality = card;
        c_copy->container_data.array.content = (uint16_t*)realloc(c_copy->container_data.array.content, card * sizeof(uint16_t));
        if (!c_copy->container_data.array.content && card > 0) {
            roaring_container_free(c_copy);
            return NULL;
        }
        memcpy(c_copy->container_data.array.content, c_orig->container_data.array.content, card * sizeof(uint16_t));
        return c_copy;
    }
}



roaring_bitmap_t* roaring_bitmap_create(void) {
    roaring_bitmap_t* r = (roaring_bitmap_t*)malloc(sizeof(roaring_bitmap_t));
    if (!r) return NULL;
    r->high_low_container.containers = NULL;
    r->high_low_container.keys = NULL;
    r->high_low_container.num_containers = 0;
    r->high_low_container.allocation_size = 0;
    return r;
}

void roaring_bitmap_free(roaring_bitmap_t* r) {
    if (!r) return;
    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        roaring_container_free(r->high_low_container.containers[i]);
    }
    free(r->high_low_container.containers);
    free(r->high_low_container.keys);
    free(r);
}

roaring_bitmap_t* roaring_bitmap_copy(const roaring_bitmap_t* r_orig) {
    roaring_bitmap_t* r_copy = roaring_bitmap_create();
    if (!r_copy) return NULL;
    if (r_orig->high_low_container.num_containers == 0) return r_copy;
    int num = r_orig->high_low_container.num_containers;
    r_copy->high_low_container.allocation_size = num;
    r_copy->high_low_container.num_containers = num;
    r_copy->high_low_container.keys = (uint16_t*)malloc(num * sizeof(uint16_t));
    if (!r_copy->high_low_container.keys) { roaring_bitmap_free(r_copy); return NULL; }
    r_copy->high_low_container.containers = (roaring_container_t**)malloc(num * sizeof(roaring_container_t*));
    if (!r_copy->high_low_container.containers) { roaring_bitmap_free(r_copy); return NULL; }

    memcpy(r_copy->high_low_container.keys, r_orig->high_low_container.keys, num * sizeof(uint16_t));
    for(int i = 0; i < num; ++i) {
        r_copy->high_low_container.containers[i] = roaring_container_copy(r_orig->high_low_container.containers[i]);
        if (!r_copy->high_low_container.containers[i]) { roaring_bitmap_free(r_copy); return NULL; }
    }
    return r_copy;
}

void roaring_bitmap_add(roaring_bitmap_t* r, uint32_t val) {
    uint16_t high = val >> 16;
    uint16_t low = val & 0xFFFF;
    roaring_array_t* ra = &r->high_low_container;

    uint16_t* key_location = binary_search_uint16_array(ra->keys, ra->num_containers, high);
    size_t key_pos = key_location - ra->keys;

    if (key_location != ra->keys + ra->num_containers && *key_location == high) {
        roaring_container_add(&ra->containers[key_pos], low);
    } else {
        if (ra->num_containers >= ra->allocation_size) {
            ra->allocation_size = (ra->allocation_size == 0) ? 4 : ra->allocation_size * 2;
            uint16_t* new_keys = (uint16_t*)realloc(ra->keys, ra->allocation_size * sizeof(uint16_t));
            if (!new_keys) return;
            ra->keys = new_keys;

            roaring_container_t** new_containers = (roaring_container_t**)realloc(ra->containers, ra->allocation_size * sizeof(roaring_container_t*));
            if (!new_containers) {
                free(ra->keys);
                ra->keys = NULL;
                return;
            }
            ra->containers = new_containers;
            
            key_location = binary_search_uint16_array(ra->keys, ra->num_containers, high);
            key_pos = key_location - ra->keys;
        }
        memmove(ra->keys + key_pos + 1, ra->keys + key_pos, (ra->num_containers - key_pos) * sizeof(uint16_t));
        memmove(ra->containers + key_pos + 1, ra->containers + key_pos, (ra->num_containers - key_pos) * sizeof(roaring_container_t*));
        ra->keys[key_pos] = high;
        roaring_container_t* new_container = roaring_container_create_array();
        if (!new_container) return; 
        ra->containers[key_pos] = new_container;
        ra->num_containers++;
        roaring_container_add(&ra->containers[key_pos], low);
    }
}

uint64_t roaring_bitmap_get_cardinality(const roaring_bitmap_t* r) {
    uint64_t card = 0;
    if (!r) return 0;
    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        card += roaring_container_get_cardinality(r->high_low_container.containers[i]);
    }
    return card;
}

bool roaring_bitmap_is_empty(const roaring_bitmap_t *r) {
    if (r == NULL || r->high_low_container.num_containers == 0) {
        return true;
    }
    return roaring_bitmap_get_cardinality(r) == 0;
}


void roaring_bitmap_and_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2) {
    roaring_array_t* ra1 = &r1->high_low_container;
    const roaring_array_t* ra2 = &r2->high_low_container;
    int32_t write_pos = 0;
    int32_t pos1 = 0, pos2 = 0;
    while (pos1 < ra1->num_containers && pos2 < ra2->num_containers) {
        uint16_t k1 = ra1->keys[pos1];
        uint16_t k2 = ra2->keys[pos2];
        if (k1 == k2) {
            roaring_container_t* res = roaring_container_and(ra1->containers[pos1], ra2->containers[pos2]);
            if (!res) { return; } 
            if (roaring_container_get_cardinality(res) > 0) {
                roaring_container_free(ra1->containers[pos1]);
                ra1->containers[write_pos] = res;
                ra1->keys[write_pos] = k1;
                write_pos++;
            } else {
                roaring_container_free(res);
                roaring_container_free(ra1->containers[pos1]);
            }
            pos1++;
            pos2++;
        } else if (k1 < k2) {
            roaring_container_free(ra1->containers[pos1]);
            pos1++;
        } else {
            pos2++;
        }
    }
    for (int i = pos1; i < ra1->num_containers; ++i) {
        roaring_container_free(ra1->containers[i]);
    }
    ra1->num_containers = write_pos;
}

void roaring_bitmap_or_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2) {
    roaring_array_t* ra1 = &r1->high_low_container;
    const roaring_array_t* ra2 = &r2->high_low_container;
    if (ra2->num_containers == 0) return;
    if (ra1->num_containers == 0) {
        roaring_bitmap_t* copy = roaring_bitmap_copy(r2);
        if (!copy) return;
        free(ra1->keys); 
        free(ra1->containers);
        *ra1 = copy->high_low_container;
        free(copy);
        return;
    }

    int32_t new_alloc_size = ra1->num_containers + ra2->num_containers;
    uint16_t* new_keys = (uint16_t*)malloc(new_alloc_size * sizeof(uint16_t));
    if (!new_keys) return;
    roaring_container_t** new_containers = (roaring_container_t**)malloc(new_alloc_size * sizeof(roaring_container_t*));
    if (!new_containers) { free(new_keys); return; }

    int32_t write_pos = 0;
    int32_t pos1 = 0, pos2 = 0;
    while (pos1 < ra1->num_containers && pos2 < ra2->num_containers) {
        uint16_t k1 = ra1->keys[pos1];
        uint16_t k2 = ra2->keys[pos2];
        if (k1 < k2) {
            new_keys[write_pos] = k1; new_containers[write_pos] = ra1->containers[pos1]; pos1++;
        } else if (k2 < k1) {
            new_keys[write_pos] = k2; 
            roaring_container_t* copied_container = roaring_container_copy(ra2->containers[pos2]);
            if (!copied_container) { return; }
            new_containers[write_pos] = copied_container; 
            pos2++;
        } else {
            new_keys[write_pos] = k1;
            roaring_container_t* merged_container = roaring_container_or(ra1->containers[pos1], ra2->containers[pos2]);
            if (!merged_container) { return; }
            new_containers[write_pos] = merged_container;
            roaring_container_free(ra1->containers[pos1]);
            pos1++; pos2++;
        }
        write_pos++;
    }
    while (pos1 < ra1->num_containers) {
        new_keys[write_pos] = ra1->keys[pos1]; new_containers[write_pos] = ra1->containers[pos1]; pos1++; write_pos++;
    }
    while (pos2 < ra2->num_containers) {
        new_keys[write_pos] = ra2->keys[pos2]; 
        roaring_container_t* copied_container = roaring_container_copy(ra2->containers[pos2]);
        if (!copied_container) { return; }
        new_containers[write_pos] = copied_container; 
        pos2++; write_pos++;
    }
    free(ra1->keys); 
    free(ra1->containers);
    ra1->keys = new_keys; 
    ra1->containers = new_containers;
    ra1->num_containers = write_pos; 
    ra1->allocation_size = new_alloc_size; 
}

void roaring_bitmap_andnot_inplace(roaring_bitmap_t* r1, const roaring_bitmap_t* r2) {
    roaring_array_t* ra1 = &r1->high_low_container;
    const roaring_array_t* ra2 = &r2->high_low_container;
    int32_t write_pos = 0;
    int32_t pos1 = 0, pos2 = 0;
    while (pos1 < ra1->num_containers && pos2 < ra2->num_containers) {
        uint16_t k1 = ra1->keys[pos1];
        uint16_t k2 = ra2->keys[pos2];
        if (k1 == k2) {
            roaring_container_t* res = roaring_container_andnot(ra1->containers[pos1], ra2->containers[pos2]);
            if (!res) { return; } 
            if (roaring_container_get_cardinality(res) > 0) {
                roaring_container_free(ra1->containers[pos1]);
                ra1->containers[write_pos] = res;
                ra1->keys[write_pos] = k1;
                write_pos++;
            } else {
                roaring_container_free(res);
                roaring_container_free(ra1->containers[pos1]);
            }
            pos1++;
            pos2++;
        } else if (k1 < k2) {
            ra1->containers[write_pos] = ra1->containers[pos1];
            ra1->keys[write_pos] = k1;
            write_pos++;
            pos1++;
        } else {
            pos2++;
        }
    }
    while (pos1 < ra1->num_containers) {
        ra1->containers[write_pos] = ra1->containers[pos1];
        ra1->keys[write_pos] = ra1->keys[pos1];
        write_pos++;
        pos1++;
    }
    for (int i = write_pos; i < ra1->num_containers; ++i) {
        roaring_container_free(ra1->containers[i]);
    }
    ra1->num_containers = write_pos;
}

roaring_bitmap_t* roaring_bitmap_and(const roaring_bitmap_t* r1, const roaring_bitmap_t* r2) {
    if (!r1 || !r2) return NULL;
    roaring_bitmap_t* result = roaring_bitmap_copy(r1);
    if (!result) return NULL;
    roaring_bitmap_and_inplace(result, r2);
    return result;
}


roaring_uint32_iterator_t* roaring_create_iterator(const roaring_bitmap_t* r) {
    roaring_uint32_iterator_t* it = (roaring_uint32_iterator_t*)malloc(sizeof(roaring_uint32_iterator_t));
    if (!it) return NULL;
    it->bitmap = r;
    it->container_index = 0;
    it->inner_index = 0;
    it->has_next = (r->high_low_container.num_containers > 0);
    while(it->has_next && roaring_container_get_cardinality(r->high_low_container.containers[it->container_index]) == 0) {
        it->container_index++;
        if (it->container_index >= r->high_low_container.num_containers) {
            it->has_next = false;
        }
    }
    return it;
}

void roaring_free_iterator(roaring_uint32_iterator_t* it) {
    free(it);
}

uint32_t roaring_read_uint32(roaring_uint32_iterator_t* it, void* val) {
    if (!it || !it->has_next) return 0;
    const roaring_array_t* ra = &it->bitmap->high_low_container;
    uint32_t high_bits = (uint32_t)ra->keys[it->container_index] << 16;
    const roaring_container_t* c = ra->containers[it->container_index];
    uint16_t low_bits;

    if (c->is_bitset) {
        int64_t current_bit_count = 0;
        for (int i = 0; i < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++i) {
            uint64_t word = c->container_data.bitset.bitset[i];
            while(word != 0) {
                int lsb_pos;
                #if defined(__GNUC__) || defined(__clang__)
                lsb_pos = __builtin_ctzll(word);
                #elif defined(_MSC_VER)
                unsigned long msc_lsb_pos;
                _BitScanForward64(&msc_lsb_pos, word);
                lsb_pos = msc_lsb_pos;
                #else
                uint64_t temp = word & (-(int64_t)word);
                lsb_pos = 0;
                if (temp != 0) {
                   while ((temp & 1) == 0) {
                       temp >>= 1;
                       lsb_pos++;
                   }
                }
                #endif
                if (current_bit_count == it->inner_index) {
                    low_bits = i * 64 + lsb_pos;
                    uint32_t result = high_bits | low_bits;
                    if(val) *((uint32_t*)val) = result;
                    return result;
                }
                current_bit_count++;
                word &= word - 1;
            }
        }
        return 0;
    } else {
        low_bits = c->container_data.array.content[it->inner_index];
    }
    uint32_t result = high_bits | low_bits;
    if(val) *((uint32_t*)val) = result;
    return result;
}

void roaring_advance_uint32_iterator(roaring_uint32_iterator_t* it) {
    if (!it || !it->has_next) return;
    const roaring_array_t* ra = &it->bitmap->high_low_container;
    const roaring_container_t* c = ra->containers[it->container_index];
    it->inner_index++;
    if (it->inner_index >= roaring_container_get_cardinality(c)) {
        it->inner_index = 0;
        it->container_index++;
        while(it->container_index < ra->num_containers && 
              roaring_container_get_cardinality(ra->containers[it->container_index]) == 0) {
            it->container_index++;
        }

        if (it->container_index >= ra->num_containers) {
            it->has_next = false;
        }
    }
}

bool roaring_bitmap_to_uint32_array(const roaring_bitmap_t *r, uint32_t *out) {
    if(!r || !out) return false;
    uint32_t pos = 0;
    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        const roaring_container_t* c = r->high_low_container.containers[i];
        if (roaring_container_get_cardinality(c) == 0) continue;
        uint32_t high_bits = (uint32_t)r->high_low_container.keys[i] << 16;
        if (c->is_bitset) {
            for (int j = 0; j < ROARING_BITSET_CONTAINER_SIZE_IN_U64; ++j) {
                uint64_t word = c->container_data.bitset.bitset[j];
                while(word != 0) {
                    int lsb_pos;
                     #if defined(__GNUC__) || defined(__clang__)
                     lsb_pos = __builtin_ctzll(word);
                     #elif defined(_MSC_VER)
                     unsigned long msc_lsb_pos;
                     _BitScanForward64(&msc_lsb_pos, word);
                     lsb_pos = msc_lsb_pos;
                     #else
                     uint64_t temp = word & (-(int64_t)word);
                     lsb_pos = 0;
                     if (temp != 0) {
                        while ((temp & 1) == 0) {
                            temp >>= 1;
                            lsb_pos++;
                        }
                     }
                     #endif
                    out[pos++] = high_bits | (j * 64 + lsb_pos);
                    word &= word - 1; 
                }
            }
        } else {
            for(int j = 0; j < c->container_data.array.cardinality; ++j) {
                out[pos++] = high_bits | c->container_data.array.content[j];
            }
        }
    }
    return true;
}


size_t roaring_bitmap_size_in_bytes(const roaring_bitmap_t* r) {
    if (!r) return sizeof(uint32_t);
    size_t size = sizeof(uint32_t);
    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        if (roaring_container_get_cardinality(r->high_low_container.containers[i]) == 0) continue; 

        size += sizeof(uint16_t) + sizeof(uint8_t) + sizeof(int32_t);
        const roaring_container_t* c = r->high_low_container.containers[i];
        if (c->is_bitset) {
            size += ROARING_BITSET_CONTAINER_SIZE_IN_U64 * sizeof(uint64_t);
        } else {
            size += c->container_data.array.cardinality * sizeof(uint16_t);
        }
    }
    return size;
}

size_t roaring_bitmap_serialize(const roaring_bitmap_t* r, char* buf) {
    if (!r || !buf) return 0;
    char* orig_buf = buf;
    
    uint32_t actual_num_containers = 0;
    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        if (roaring_container_get_cardinality(r->high_low_container.containers[i]) > 0) {
            actual_num_containers++;
        }
    }

    memcpy(buf, &actual_num_containers, sizeof(uint32_t));
    buf += sizeof(uint32_t);

    for (int i = 0; i < r->high_low_container.num_containers; ++i) {
        const roaring_container_t* c = r->high_low_container.containers[i];
        if (roaring_container_get_cardinality(c) == 0) continue;

        uint16_t key = r->high_low_container.keys[i];
        memcpy(buf, &key, sizeof(uint16_t));
        buf += sizeof(uint16_t);

        uint8_t is_bitset = c->is_bitset;
        memcpy(buf, &is_bitset, sizeof(uint8_t));
        buf += sizeof(uint8_t);
        
        
        
        
        
        
        int32_t card = static_cast<int32_t>(roaring_container_get_cardinality(c));
        memcpy(buf, &card, sizeof(int32_t));
        buf += sizeof(int32_t);

        if (c->is_bitset) {
            size_t bytes_to_copy = ROARING_BITSET_CONTAINER_SIZE_IN_U64 * sizeof(uint64_t);
            memcpy(buf, c->container_data.bitset.bitset, bytes_to_copy);
            buf += bytes_to_copy;
        } else {
            size_t bytes_to_copy = c->container_data.array.cardinality * sizeof(uint16_t);
            memcpy(buf, c->container_data.array.content, bytes_to_copy);
            buf += bytes_to_copy;
        }
    }
    return buf - orig_buf;
}

roaring_bitmap_t* roaring_bitmap_portable_deserialize(const char* buf) {
    if (!buf) return NULL;
    roaring_bitmap_t* r = roaring_bitmap_create();
    if (!r) return NULL;
    uint32_t num_containers;
    memcpy(&num_containers, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    
    if(num_containers > 0) {
        r->high_low_container.allocation_size = num_containers;
        r->high_low_container.num_containers = num_containers;
        r->high_low_container.keys = (uint16_t*)malloc(num_containers * sizeof(uint16_t));
        if (!r->high_low_container.keys) { roaring_bitmap_free(r); return NULL; }
        r->high_low_container.containers = (roaring_container_t**)malloc(num_containers * sizeof(roaring_container_t*));
        if (!r->high_low_container.containers) { roaring_bitmap_free(r); return NULL; }
    }

    for (uint32_t i = 0; i < num_containers; ++i) {
        uint16_t key;
        memcpy(&key, buf, sizeof(uint16_t));
        buf += sizeof(uint16_t);
        r->high_low_container.keys[i] = key;

        uint8_t is_bitset;
        memcpy(&is_bitset, buf, sizeof(uint8_t));
        buf += sizeof(uint8_t);

        int32_t card;
        memcpy(&card, buf, sizeof(int32_t));
        buf += sizeof(int32_t);

        roaring_container_t* c;
        if (is_bitset) {
            c = roaring_container_create_bitset();
            if (!c) { roaring_bitmap_free(r); return NULL; }
            c->container_data.bitset.cardinality = card;
            size_t bytes_to_copy = ROARING_BITSET_CONTAINER_SIZE_IN_U64 * sizeof(uint64_t);
            memcpy(c->container_data.bitset.bitset, buf, bytes_to_copy);
            buf += bytes_to_copy;
        } else {
            c = roaring_container_create_array();
            if (!c) { roaring_bitmap_free(r); return NULL; }
            c->container_data.array.cardinality = card;
            c->container_data.array.allocation_size = card;
            size_t bytes_to_copy = card * sizeof(uint16_t);
            c->container_data.array.content = (uint16_t*)realloc(c->container_data.array.content, bytes_to_copy);
            if (!c->container_data.array.content && card > 0) {
                 roaring_container_free(c); roaring_bitmap_free(r); return NULL;
            }
            memcpy(c->container_data.array.content, buf, bytes_to_copy);
            buf += bytes_to_copy;
        }
        r->high_low_container.containers[i] = c;
    }
    return r;
}