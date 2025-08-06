#pragma once

#include <napi.h>
#include "stax_graph/graph_engine.h"


class DatabaseWrap;

class GraphTransactionWrap : public Napi::ObjectWrap<GraphTransactionWrap> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, DatabaseWrap* db_wrap);
    GraphTransactionWrap(const Napi::CallbackInfo& info);
    ~GraphTransactionWrap();

private:
    static Napi::FunctionReference constructor;
    Database* db_instance_ = nullptr;
    std::unique_ptr<GraphTransaction> txn_ = nullptr;
    bool is_finished_ = false;

    
    Napi::Value InsertObject(const Napi::CallbackInfo& info);
    void UpdateObject(const Napi::CallbackInfo& info);
    void DeleteObject(const Napi::CallbackInfo& info);
    void InsertRelationship(const Napi::CallbackInfo& info);
    
    void Commit(const Napi::CallbackInfo& info);
    void Abort(const Napi::CallbackInfo& info);
};