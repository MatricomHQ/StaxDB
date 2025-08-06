#include "kv_transaction_wrap.h"
#include "database_wrap.h"

Napi::FunctionReference KVTransactionWrap::constructor;

Napi::Object KVTransactionWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxWriteTransaction", {
        InstanceMethod("insert", &KVTransactionWrap::Insert),
        InstanceMethod("remove", &KVTransactionWrap::Remove),
        InstanceMethod("commit", &KVTransactionWrap::Commit),
        InstanceMethod("abort", &KVTransactionWrap::Abort),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("StaxWriteTransaction", func);
    return exports;
}

Napi::Object KVTransactionWrap::NewInstance(Napi::Env env, DatabaseWrap* db_wrap, StaxCollection col_idx) {
    Napi::Object obj = constructor.New({});
    KVTransactionWrap* txn_wrap = Unwrap(obj);
    txn_wrap->db_instance_ = db_wrap->db_instance_;
    txn_wrap->col_idx_ = col_idx;
    
    Collection& col = txn_wrap->db_instance_->get_collection_by_idx(col_idx);
    txn_wrap->ctx_ = col.begin_transaction_context(0, false); 
    return obj;
}

KVTransactionWrap::KVTransactionWrap(const Napi::CallbackInfo& info) 
    : Napi::ObjectWrap<KVTransactionWrap>(info), col_idx_(0) {}

KVTransactionWrap::~KVTransactionWrap() {
    if (!is_finished_ && db_instance_) {
        
        try {
            Collection& col = db_instance_->get_collection_by_idx(col_idx_);
            col.abort(ctx_);
        } catch (const std::exception& e) {
            
        }
    }
}

void KVTransactionWrap::Insert(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    std::string key = info[0].As<Napi::String>();
    std::string value = info[1].As<Napi::String>();
    
    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx_);
        col.insert(ctx_, batch_, key, value);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void KVTransactionWrap::Remove(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    std::string key = info[0].As<Napi::String>();

    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx_);
        col.remove(ctx_, batch_, key);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void KVTransactionWrap::Commit(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    is_finished_ = true;
    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx_);
        col.commit(ctx_, batch_);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void KVTransactionWrap::Abort(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    is_finished_ = true;
    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx_);
        col.abort(ctx_);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}