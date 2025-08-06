#pragma once

#include <napi.h>
#include "stax_db/db.h"
#include "stax_api/staxdb_api.h" 


class DatabaseWrap;

class KVTransactionWrap : public Napi::ObjectWrap<KVTransactionWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, DatabaseWrap* db_wrap, StaxCollection col_idx);
    KVTransactionWrap(const Napi::CallbackInfo& info);
    ~KVTransactionWrap();

private:
    static Napi::FunctionReference constructor;
    Database* db_instance_ = nullptr; 
    StaxCollection col_idx_ = 0;
    TxnContext ctx_;
    TransactionBatch batch_;
    bool is_finished_ = false;

    void Insert(const Napi::CallbackInfo& info);
    void Remove(const Napi::CallbackInfo& info);
    void Commit(const Napi::CallbackInfo& info);
    void Abort(const Napi::CallbackInfo& info);
};