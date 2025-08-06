//

//
#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <utility>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif


#include "stax_db/db.h" 
#include "stax_db/path_engine.h" 
#include "stax_tx/transaction.h" 

namespace Tests {


static inline int get_process_id() {
#if defined(_WIN32)
    return _getpid();
#else
    return getpid();
#endif
}



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

    TestUser(uint64_t id, uint8_t a, uint16_t cid, uint8_t t, std::string uname, std::string mail, std::string b)
        : user_id(id), age(a), country_id(cid), tier(t), username(std::move(uname)), email(std::move(mail)), bio(std::move(b)), registration_timestamp(0) {}
    
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


inline void insert_user_local(::Collection& col, const TxnContext& ctx, TransactionBatch& batch, const TestUser& user, const ::PathEngine& pe) {
    std::string doc_key = "doc:user:" + std::to_string(user.user_id);
    col.insert(ctx, batch, doc_key, user.serialize_flex_doc());
    
    uint64_t fractal_payload = user.pack_fractal_payload();
    std::string idx_key = pe.create_numeric_sortable_key("idx:user", fractal_payload) + ":" + std::to_string(user.user_id);
    col.insert(ctx, batch, idx_key, "1");

    std::string str_idx_key = "idx_str:user:username:" + user.username + ":" + std::to_string(user.user_id);
    col.insert(ctx, batch, str_idx_key, "1");
}


inline void delete_user_indexes_local(::Collection& col, const TxnContext& ctx, TransactionBatch& batch, const TestUser& user, const ::PathEngine& pe) {
    uint64_t fractal_payload = user.pack_fractal_payload();
    std::string idx_key = pe.create_numeric_sortable_key("idx:user", fractal_payload) + ":" + std::to_string(user.user_id);
    col.remove(ctx, batch, idx_key);
    
    std::string str_idx_key = "idx_str:user:username:" + user.username + ":" + std::to_string(user.user_id);
    col.remove(ctx, batch, str_idx_key);
}

} 