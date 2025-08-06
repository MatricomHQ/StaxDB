#pragma once

#include <napi.h>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <algorithm>
#include <variant>
#include <iostream>

#include "stax_api/staxdb_api.h"
#include "stax_db/db.h"
#include "stax_common/geohash.hpp"
#include "stax_common/binary_utils.h"

#include "kv_transaction_wrap.h"
#include "graph_transaction_wrap.h"


class TransactionWrap;
class ResultSetWrap;
class GraphWrap;


class DatabaseWrap : public Napi::ObjectWrap<DatabaseWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    DatabaseWrap(const Napi::CallbackInfo& info);
    ~DatabaseWrap();

    StaxDB db_handle_;
    Database* db_instance_; 

private:
    friend class TransactionWrap;
    friend class ResultSetWrap;
    friend class GraphWrap;
    friend class KVTransactionWrap;
    friend class GraphTransactionWrap;
    static Napi::FunctionReference constructor;

    void CloseSync(const Napi::CallbackInfo& info);
    Napi::Value GetCollectionSync(const Napi::CallbackInfo& info);
    Napi::Value GetGraph(const Napi::CallbackInfo& info);
    Napi::Value BeginTransaction(const Napi::CallbackInfo& info);
    Napi::Value ExecuteRangeQuery(const Napi::CallbackInfo& info);
    void ExecuteBatch(const Napi::CallbackInfo& info);
    void InsertBatchAsync(const Napi::CallbackInfo& info);
    void MultiGetAsync(const Napi::CallbackInfo& info);
};


class GraphWrap : public Napi::ObjectWrap<GraphWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, Napi::Object db_wrap_obj);
    GraphWrap(const Napi::CallbackInfo& info);
    ~GraphWrap();

private:
    static Napi::FunctionReference constructor;
    Napi::Reference<Napi::Object> db_wrap_ref_;
    StaxGraph graph_handle_ = nullptr;
    
    
    void InsertRelationship(const Napi::CallbackInfo& info);
    void Commit(const Napi::CallbackInfo& info);
    Napi::Value CompileQuery(const Napi::CallbackInfo& info);
    Napi::Value ExecuteQuery(const Napi::CallbackInfo& info);
    void DeleteObject(const Napi::CallbackInfo& info);
    Napi::Value InsertObject(const Napi::CallbackInfo& info);
    void UpdateObject(const Napi::CallbackInfo& info);
    Napi::Value BeginTransaction(const Napi::CallbackInfo& info);
};



class ResultSetWrap : public Napi::ObjectWrap<ResultSetWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, StaxResultSet rs_handle);
    ResultSetWrap(const Napi::CallbackInfo& info);
    ~ResultSetWrap();

private:
    static Napi::FunctionReference constructor;
    StaxResultSet result_set_handle_ = nullptr;
    bool is_closed_ = false;

    
    Napi::Value GetPage(const Napi::CallbackInfo& info);
    Napi::Value GetTotalCount(const Napi::CallbackInfo& info);
    void Close(const Napi::CallbackInfo& info);
};



class TransactionWrap : public Napi::ObjectWrap<TransactionWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, DatabaseWrap* db_wrap, StaxCollection col_idx, bool is_read_only);
    TransactionWrap(const Napi::CallbackInfo& info);
    ~TransactionWrap();

private:
    static Napi::FunctionReference constructor;
    Database* db_instance_;
    StaxCollection col_idx_;
    TxnContext ctx_;
    bool is_finished_ = false;

    Napi::Value Get(const Napi::CallbackInfo& info);
    void Commit(const Napi::CallbackInfo& info);
    void Abort(const Napi::CallbackInfo& info);
};