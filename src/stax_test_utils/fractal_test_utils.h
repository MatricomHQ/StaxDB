#pragma once

#include <iostream>
#include <string>
#include <chrono>
#include <iomanip>
#include <cstdint>
#include "stax_tx/transaction.h" 
#include "stax_db/path_engine.h" 
#include "stax_common/common_types.hpp" 

namespace FractalTestUtils {

struct TestUser {
    uint64_t user_id;
    uint8_t age;
    uint16_t country_id;
    uint8_t tier;
    std::string username;
    std::string email;
    std::string bio;
    uint64_t registration_timestamp;

    TestUser() : user_id(0), age(0), country_id(0), tier(0), registration_timestamp(0) {}

    TestUser(uint64_t user_id, uint8_t age, uint16_t country_id, uint8_t tier, std::string username, std::string email, std::string bio)
        : user_id(user_id), age(age), country_id(country_id), tier(tier), username(std::move(username)), email(std::move(email)), bio(std::move(bio)), registration_timestamp(0) {}
    
    uint64_t pack_fractal_payload() const {
        uint64_t payload = 0;
        payload |= (static_cast<uint64_t>(country_id) << 48);
        payload |= (static_cast<uint64_t>(age) << 40);
        payload |= (static_cast<uint64_t>(tier) << 38);
        return payload;
    }

    std::string serialize_flex_doc() const {
        return "id:" + std::to_string(user_id) + "|name:" + username + "|email:" + email + "|age:" + std::to_string(age) + "|country:" + std::to_string(country_id) + "|tier:" + std::to_string(tier) + "|bio:" + bio + "|reg_ts:" + std::to_string(registration_timestamp);
    }

    static TestUser deserialize_flex_doc(std::string_view doc_sv);
};

struct WideUser {
    uint64_t user_id;
    uint16_t f1_region, f2_category, f3_status;
    uint16_t f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    std::string f16_notes;

    uint64_t pack_z_order_payload() const;
    std::string serialize_doc() const {
        return "id:" + std::to_string(user_id) + "|f1:" + std::to_string(f1_region);
    }
};

inline uint64_t spread_bits_16(uint16_t value) {
    uint64_t x = value;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8))  & 0x00FF00FF00FF00FF;
    x = (x | (x << 4))  & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2))  & 0x3333333333333333;
    x = (x | (x << 1))  & 0x5555555555555555;
    return x;
}

inline uint64_t z_order_encode_3x16(uint16_t val1, uint16_t val2, uint16_t val3) {
    return (spread_bits_16(val1) << 2) | (spread_bits_16(val2) << 1) | spread_bits_16(val3);
}

inline uint64_t WideUser::pack_z_order_payload() const {
    return z_order_encode_3x16(f1_region, f2_category, f3_status);
}

inline TestUser TestUser::deserialize_flex_doc(std::string_view doc_sv) {
    TestUser u{};
    std::string_view current_doc = doc_sv;

    size_t prev_pos = 0;
    while(prev_pos < current_doc.length()) {
        size_t pipe_pos = current_doc.find('|', prev_pos);
        if (pipe_pos == std::string_view::npos) {
            pipe_pos = current_doc.length();
        }
        std::string_view token = current_doc.substr(prev_pos, pipe_pos - prev_pos);
        
        size_t colon_pos = token.find(':');
        if (colon_pos != std::string_view::npos) {
            std::string_view key = token.substr(0, colon_pos);
            std::string_view val = token.substr(colon_pos + 1);
            
            uint64_t temp_val = 0;
            if (key == "id") PathEngine::value_to_uint64(val, u.user_id);
            else if (key == "name") u.username = std::string(val);
            else if (key == "email") u.email = std::string(val);
            else if (key == "age") { PathEngine::value_to_uint64(val, temp_val); u.age = static_cast<uint8_t>(temp_val); }
            else if (key == "country") { PathEngine::value_to_uint64(val, temp_val); u.country_id = static_cast<uint16_t>(temp_val); }
            else if (key == "tier") { PathEngine::value_to_uint64(val, temp_val); u.tier = static_cast<uint8_t>(temp_val); }
            else if (key == "bio") u.bio = std::string(val);
            else if (key == "reg_ts") PathEngine::value_to_uint64(val, u.registration_timestamp);
        }
        
        if (pipe_pos == current_doc.length()) break; 
        prev_pos = pipe_pos + 1;
    }
    return u;
}

inline void insert_user(Transaction& txn, const TestUser& user, const PathEngine& pe) {
    std::string doc_key = "doc:user:" + std::to_string(user.user_id);
    txn.insert(doc_key, user.serialize_flex_doc());
    
    uint64_t fractal_payload = user.pack_fractal_payload();
    std::string idx_key = pe.create_numeric_sortable_key("idx:user", fractal_payload) + ":" + std::to_string(user.user_id);
    txn.insert(idx_key, "1");

    std::string str_idx_key = "idx_str:user:username:" + user.username + ":" + std::to_string(user.user_id);
    txn.insert(str_idx_key, "1");
}

inline void delete_user_indexes(Transaction& txn, const TestUser& user, const PathEngine& pe) {
    uint64_t fractal_payload = user.pack_fractal_payload();
    std::string idx_key = pe.create_numeric_sortable_key("idx:user", fractal_payload) + ":" + std::to_string(user.user_id);
    txn.remove(idx_key);
    
    std::string str_idx_key = "idx_str:user:username:" + user.username + ":" + std::to_string(user.user_id);
    txn.remove(str_idx_key);
}

inline void insert_wide_user(Transaction& txn, const WideUser& user, const PathEngine& pe) {
    std::string doc_key = "doc:wide_user:" + std::to_string(user.user_id);
    txn.insert(doc_key, user.serialize_doc());
    
    uint64_t z_payload = user.pack_z_order_payload();
    std::string idx_key_prefix = pe.create_numeric_sortable_key("idx:wide_user", z_payload);
    std::string full_idx_key = idx_key_prefix + ":" + std::to_string(user.user_id);
    txn.insert(full_idx_key, "1");
}