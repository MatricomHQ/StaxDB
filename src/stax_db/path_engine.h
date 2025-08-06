#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>
#include <charconv> 
#include <algorithm>
#include <cstdio>
#include <cstring>

#include "stax_common/common_types.hpp"




struct PathQuery {
    std::string key_path;
    std::string value_component;
};

class PathEngine {
public:
    PathEngine() = default;

    
    std::string create_key_string(const PathQuery& query) const {
        return query.key_path + '\0' + query.value_component;
    }
    
    
    std::string create_key_string(const std::string& key_path) const {
        return key_path;
    }
    
    
    std::string create_numeric_sortable_key(const std::string& key_path, uint64_t numeric_val) const {
        std::string result;
        result.resize(key_path.length() + 1 + 20);
        char* buffer = result.data();

        memcpy(buffer, key_path.data(), key_path.length());

        buffer[key_path.length()] = ':';

        int written = snprintf(buffer + key_path.length() + 1, 21, "%020llu", (long long unsigned)numeric_val);

        if (written != 20) {
            throw std::runtime_error("Failed to create numeric sortable key: snprintf encoding error.");
        }

        return result;
    }

    size_t create_numeric_sortable_key(char* buffer, size_t buffer_size, std::string_view key_path, uint64_t numeric_val) const {
        const size_t required_size = key_path.length() + 1 + 20;
        if (buffer_size < required_size) {
            return 0;
        }

        memcpy(buffer, key_path.data(), key_path.length());

        buffer[key_path.length()] = ':';

        int written = snprintf(buffer + key_path.length() + 1, 21, "%020llu", (long long unsigned)numeric_val);
        
        if (written != 20) {
            return 0;
        }

        return required_size;
    }
    
    
    std::string create_prefix_key_string(const std::string& key_path) const {
        return key_path + '\0';
    }

    
    PathQuery deserialize_key_string(std::string_view serialized_key) const {
        PathQuery query;
        size_t separator_pos = serialized_key.find('\0');
        if (separator_pos == std::string_view::npos) {
            query.key_path = std::string(serialized_key);
        } else {
            query.key_path = std::string(serialized_key.substr(0, separator_pos));
            query.value_component = std::string(serialized_key.substr(separator_pos + 1));
        }
        return query;
    }
    
    
    static bool value_to_uint64(std::string_view sv, uint64_t& out_val) {
        if (sv.empty()) return false;
        auto result = std::from_chars(sv.data(), sv.data() + sv.size(), out_val);
        return result.ec == std::errc() && result.ptr == sv.data() + sv.size();
    }
};