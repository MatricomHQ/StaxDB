#include "database_wrap.h"
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


Napi::FunctionReference DatabaseWrap::constructor;

Napi::Value DropSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "Database path (string) is required for dropSync.").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    std::string path_str = info[0].As<Napi::String>();
    staxdb_drop(path_str.c_str());

    const char* err = staxdb_get_last_error();
    if (err && strlen(err) > 0) {
        Napi::Error::New(env, std::string("Failed to drop database: ") + err).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}


Napi::Object DatabaseWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxDB", {
        InstanceMethod("closeSync", &DatabaseWrap::CloseSync),
        InstanceMethod("getCollectionSync", &DatabaseWrap::GetCollectionSync),
        InstanceMethod("getGraph", &DatabaseWrap::GetGraph),
        InstanceMethod("beginTransaction", &DatabaseWrap::BeginTransaction),
        InstanceMethod("executeRangeQuery", &DatabaseWrap::ExecuteRangeQuery),
        InstanceMethod("executeBatch", &DatabaseWrap::ExecuteBatch),
        InstanceMethod("insertBatchAsync", &DatabaseWrap::InsertBatchAsync),
        InstanceMethod("multiGetAsync", &DatabaseWrap::MultiGetAsync),
        StaticMethod("dropSync", DropSync)
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("StaxDB", func);
    return exports;
}

DatabaseWrap::DatabaseWrap(const Napi::CallbackInfo& info) : Napi::ObjectWrap<DatabaseWrap>(info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "Database path (string) is required.").ThrowAsJavaScriptException();
        return;
    }
    std::string path_str = info[0].As<Napi::String>();
    const size_t num_threads = 4;
    db_handle_ = staxdb_init_path(path_str.c_str(), num_threads, StaxDurability_NoSync);
    if (!db_handle_) {
        Napi::Error::New(env, std::string("Failed to open database: ") + staxdb_get_last_error()).ThrowAsJavaScriptException();
        return;
    }
    db_instance_ = staxdb_get_db_instance(db_handle_);
}

DatabaseWrap::~DatabaseWrap() {
    if (db_handle_) {
        staxdb_close(db_handle_);
        db_handle_ = nullptr;
        db_instance_ = nullptr;
    }
}

void DatabaseWrap::CloseSync(const Napi::CallbackInfo& info) {
    if (db_handle_) {
        staxdb_close(db_handle_);
        db_handle_ = nullptr;
        db_instance_ = nullptr;
    }
}

Napi::Value DatabaseWrap::GetCollectionSync(const Napi::CallbackInfo& info) {
    std::string name = info[0].As<Napi::String>();
    StaxSlice name_slice = { name.c_str(), name.length() };
    uint32_t col_idx = staxdb_get_collection(db_handle_, name_slice);
    return Napi::Number::New(info.Env(), col_idx);
}

Napi::Value DatabaseWrap::GetGraph(const Napi::CallbackInfo& info) {
    return GraphWrap::NewInstance(info.Env(), this->Value().As<Napi::Object>());
}

Napi::Value DatabaseWrap::BeginTransaction(const Napi::CallbackInfo& info) {
    StaxCollection col_idx = info[0].As<Napi::Number>().Uint32Value();
    bool is_read_only = info[1].As<Napi::Boolean>();
    
    if (is_read_only) {
        return TransactionWrap::NewInstance(info.Env(), this, col_idx, true);
    } else {
        return KVTransactionWrap::NewInstance(info.Env(), this, col_idx);
    }
}

Napi::Value DatabaseWrap::ExecuteRangeQuery(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    StaxCollection col_idx = info[0].As<Napi::Number>().Uint32Value();
    Napi::Object options_obj = info[1].As<Napi::Object>();

    StaxQueryOptions options = {};
    std::string start_key_str, end_key_str; 

    if (options_obj.Has("start")) {
        start_key_str = options_obj.Get("start").As<Napi::String>();
        options.start_key = {start_key_str.c_str(), start_key_str.length()};
    }
    if (options_obj.Has("end")) {
        end_key_str = options_obj.Get("end").As<Napi::String>();
        options.end_key = {end_key_str.c_str(), end_key_str.length()};
    }
    
    StaxResultSet rs_handle = staxdb_execute_range_query(this->db_instance_, col_idx, &options);
    return ResultSetWrap::NewInstance(env, rs_handle);
}


void DatabaseWrap::ExecuteBatch(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    StaxCollection col_idx = info[0].As<Napi::Number>().Uint32Value();
    Napi::Buffer<char> buffer = info[1].As<Napi::Buffer<char>>();
    char* data = buffer.Data();
    size_t length = buffer.Length();
    size_t offset = 0;

    if (length < 4) {
        Napi::TypeError::New(env, "Batch buffer is too small.").ThrowAsJavaScriptException();
        return;
    }

    uint32_t num_ops = *reinterpret_cast<uint32_t*>(data + offset);
    offset += 4;

    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx);
        TxnContext ctx = col.begin_transaction_context(0, false);
        TransactionBatch batch;

        for (uint32_t i = 0; i < num_ops; ++i) {
            if (offset >= length) break;
            uint8_t op_type = *reinterpret_cast<uint8_t*>(data + offset);
            offset += 1;
            if (offset + 4 > length) break;
            uint32_t key_len = *reinterpret_cast<uint32_t*>(data + offset);
            offset += 4;
            if (offset + key_len > length) break;
            std::string_view key(data + offset, key_len);
            offset += key_len;

            if (op_type == 1) { 
                if (offset + 4 > length) break;
                uint32_t val_len = *reinterpret_cast<uint32_t*>(data + offset);
                offset += 4;
                if (offset + val_len > length) break;
                std::string_view value(data + offset, val_len);
                offset += val_len;
                col.insert(ctx, batch, key, value);
            } else if (op_type == 2) { 
                col.remove(ctx, batch, key);
            }
        }
        col.commit(ctx, batch);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}


Napi::FunctionReference GraphWrap::constructor;

Napi::Object GraphWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxGraph", {
        InstanceMethod("insertRelationship", &GraphWrap::InsertRelationship),
        InstanceMethod("commit", &GraphWrap::Commit),
        InstanceMethod("compileQuery", &GraphWrap::CompileQuery),
        InstanceMethod("executeQuery", &GraphWrap::ExecuteQuery),
        InstanceMethod("deleteObject", &GraphWrap::DeleteObject),
        InstanceMethod("insertObject", &GraphWrap::InsertObject),
        InstanceMethod("updateObject", &GraphWrap::UpdateObject),
        InstanceMethod("beginTransaction", &GraphWrap::BeginTransaction),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("StaxGraph", func);
    return exports;
}

Napi::Object GraphWrap::NewInstance(Napi::Env env, Napi::Object db_wrap_obj) {
    Napi::Object obj = constructor.New({});
    GraphWrap* gw = Unwrap(obj);
    gw->db_wrap_ref_ = Napi::Persistent(db_wrap_obj);
    DatabaseWrap* db_wrap = Napi::ObjectWrap<DatabaseWrap>::Unwrap(db_wrap_obj);
    gw->graph_handle_ = staxdb_get_graph(db_wrap->db_handle_);
    return obj;
}

GraphWrap::GraphWrap(const Napi::CallbackInfo& info) : Napi::ObjectWrap<GraphWrap>(info) {}

GraphWrap::~GraphWrap() {
    db_wrap_ref_.Reset();
}

Napi::Value GraphWrap::BeginTransaction(const Napi::CallbackInfo& info) {
    DatabaseWrap* db_wrap = Napi::ObjectWrap<DatabaseWrap>::Unwrap(db_wrap_ref_.Value());
    return GraphTransactionWrap::NewInstance(info.Env(), db_wrap);
}

void GraphWrap::InsertRelationship(const Napi::CallbackInfo& info) {
    uint32_t source_id = info[0].As<Napi::Number>().Uint32Value();
    std::string rel_type = info[1].As<Napi::String>();
    uint32_t target_id = info[2].As<Napi::Number>().Uint32Value();
    StaxSlice rel_slice = { rel_type.c_str(), rel_type.length() };
    staxdb_graph_insert_relationship(graph_handle_, source_id, rel_slice, target_id);
}

void GraphWrap::Commit(const Napi::CallbackInfo& info) {
    staxdb_graph_commit(graph_handle_);
}

void GraphWrap::DeleteObject(const Napi::CallbackInfo& info) {
    uint32_t obj_id = info[0].As<Napi::Number>().Uint32Value();
    staxdb_graph_delete_object(graph_handle_, obj_id);
}



static void convert_js_obj_to_properties(Napi::Env env, Napi::Object data_obj, std::vector<StaxObjectProperty>& c_properties, std::vector<std::string>& string_storage) {
    Napi::Array property_names = data_obj.GetPropertyNames();
    uint32_t num_properties = property_names.Length();
    c_properties.reserve(num_properties);
    string_storage.reserve(num_properties * 2);

    for (uint32_t i = 0; i < num_properties; ++i) {
        Napi::Value key_val = property_names.Get(i);
        string_storage.push_back(key_val.As<Napi::String>());
        const std::string& field = string_storage.back();

        if (field == "__stax_id") continue;

        Napi::Value value_val = data_obj.Get(key_val);
        StaxObjectProperty prop = {};
        prop.field = { field.c_str(), field.length() };

        if (value_val.IsString()) {
            prop.type = STAX_PROP_STRING;
            string_storage.push_back(value_val.As<Napi::String>());
            prop.value.string_val = { string_storage.back().c_str(), string_storage.back().length() };
        } else if (value_val.IsNumber()) {
            prop.type = STAX_PROP_NUMERIC;
            prop.value.numeric_val = static_cast<uint64_t>(value_val.As<Napi::Number>().Int64Value());
        } else if (value_val.IsBoolean()) {
            prop.type = STAX_PROP_NUMERIC;
            prop.value.numeric_val = value_val.As<Napi::Boolean>().Value() ? 1 : 0;
        } else if (value_val.IsObject() && !value_val.IsArray() && !value_val.IsNull()) {
            Napi::Object nested_obj = value_val.As<Napi::Object>();
            if (nested_obj.Has("lat") && nested_obj.Has("lon")) {
                prop.type = STAX_PROP_GEO;
                prop.value.geo_val.lat = nested_obj.Get("lat").As<Napi::Number>().DoubleValue();
                prop.value.geo_val.lon = nested_obj.Get("lon").As<Napi::Number>().DoubleValue();
            } else {
                continue; 
            }
        } else {
            continue; 
        }
        c_properties.push_back(prop);
    }
}

Napi::Value GraphWrap::InsertObject(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsObject()) {
        Napi::TypeError::New(env, "Expected (data: object)").ThrowAsJavaScriptException();
        return env.Null();
    }
    Napi::Object data_obj = info[0].As<Napi::Object>();

    try {
        std::vector<StaxObjectProperty> c_properties;
        std::vector<std::string> string_storage;
        convert_js_obj_to_properties(env, data_obj, c_properties, string_storage);

        uint32_t new_id = 0;
        if (!c_properties.empty()) {
            new_id = staxdb_graph_insert_object(graph_handle_, c_properties.data(), c_properties.size());
        }
        return Napi::Number::New(env, new_id);
    } catch (const Napi::Error& e) {
        throw e;
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Null();
    }
}

void GraphWrap::UpdateObject(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsNumber() || !info[1].IsObject()) {
        Napi::TypeError::New(env, "Expected (objectId: number, data: object)").ThrowAsJavaScriptException();
        return;
    }
    uint32_t obj_id = info[0].As<Napi::Number>().Uint32Value();
    Napi::Object data_obj = info[1].As<Napi::Object>();

    try {
        std::vector<StaxObjectProperty> c_properties;
        std::vector<std::string> string_storage;
        convert_js_obj_to_properties(env, data_obj, c_properties, string_storage);

        staxdb_graph_update_object(graph_handle_, obj_id, c_properties.data(), c_properties.size());
        
    } catch (const Napi::Error& e) {
        throw e;
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

Napi::Value GraphWrap::CompileQuery(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Array steps_arr = info[0].As<Napi::Array>();
    uint32_t num_steps = steps_arr.Length();
    if (num_steps == 0) return Napi::Number::New(env, -1);

    std::vector<StaxGraphQueryStep> c_steps(num_steps);
    std::vector<std::string> string_storage;
    string_storage.reserve(num_steps * 2);

    for (uint32_t i = 0; i < num_steps; ++i) {
        Napi::Object step_obj = steps_arr.Get(i).As<Napi::Object>();
        c_steps[i] = {};
        std::string op_type_str = step_obj.Get("op_type").As<Napi::String>();
        if (op_type_str == "find") c_steps[i].op_type = STAX_GRAPH_FIND_BY_PROPERTY;
        else if (op_type_str == "traverse") c_steps[i].op_type = STAX_GRAPH_TRAVERSE;
        else if (op_type_str == "intersect") c_steps[i].op_type = STAX_GRAPH_INTERSECT;
        else if (op_type_str == "union") c_steps[i].op_type = STAX_GRAPH_UNION;

        if (c_steps[i].op_type == STAX_GRAPH_TRAVERSE) {
            std::string dir_str = step_obj.Get("direction").As<Napi::String>();
            c_steps[i].direction = (dir_str == "out") ? STAX_GRAPH_OUTGOING : STAX_GRAPH_INCOMING;
            
            if (step_obj.Has("filter")) {
                Napi::Object filter_obj = step_obj.Get("filter").As<Napi::Object>();
                Napi::Array filter_keys = filter_obj.GetPropertyNames();
                c_steps[i].has_filter = true;
                c_steps[i].filter_property_count = filter_keys.Length();
            } else {
                c_steps[i].has_filter = false;
                c_steps[i].filter_property_count = 0;
            }
        }
        string_storage.push_back(step_obj.Get("field").As<Napi::String>());
        c_steps[i].field = { string_storage.back().c_str(), string_storage.back().length() };
        
        bool uses_range = false;
        if(step_obj.Has("value")) {
            Napi::Value val = step_obj.Get("value");
            if(val.IsObject()) {
                uses_range = true;
            }
        }
        c_steps[i].uses_numeric_range = uses_range;
    }
    uint32_t plan_id = staxdb_graph_compile_plan(graph_handle_, c_steps.data(), num_steps);
    return Napi::Number::New(env, plan_id);
}

Napi::Value GraphWrap::ExecuteQuery(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    uint32_t plan_id = info[0].As<Napi::Number>().Uint32Value();
    Napi::Array plan_js = info[1].As<Napi::Array>();
    
    std::vector<StaxSlice> c_params;
    std::vector<std::string> param_storage;
    std::vector<char> binary_param_buffer;
    bool lossless; 

    param_storage.reserve(plan_js.Length() * 2);
    binary_param_buffer.reserve(plan_js.Length() * 16);

    for (uint32_t i = 0; i < plan_js.Length(); ++i) {
        Napi::Object step = plan_js.Get(i).As<Napi::Object>();
        
        if (step.Has("value")) {
            Napi::Value val = step.Get("value");
            if (val.IsString()) {
                param_storage.push_back(val.As<Napi::String>());
                c_params.push_back({param_storage.back().c_str(), param_storage.back().length()});
            } else if (val.IsObject()) {
                 Napi::Object obj = val.As<Napi::Object>();
                 uint64_t gte = 0, lte = 0;
                 if (obj.Has("lat") && obj.Has("lon")) {
                    double lat = obj.Get("lat").As<Napi::Number>().DoubleValue();
                    double lon = obj.Get("lon").As<Napi::Number>().DoubleValue();
                    gte = lte = GeoHash::encode(lat, lon);
                 } else { 
                    Napi::Value gte_val = obj.Get("gte");
                    if (gte_val.IsBigInt()) {
                        gte = gte_val.As<Napi::BigInt>().Uint64Value(&lossless);
                    } else if (gte_val.IsNumber()) {
                        gte = gte_val.As<Napi::Number>().Int64Value();
                    }
                    
                    Napi::Value lte_val = obj.Get("lte");
                    if (lte_val.IsBigInt()) {
                        lte = lte_val.As<Napi::BigInt>().Uint64Value(&lossless);
                    } else if (lte_val.IsNumber()) {
                        lte = lte_val.As<Napi::Number>().Int64Value();
                    } else {
                        lte = std::numeric_limits<uint64_t>::max();
                    }
                 }
                 size_t offset = binary_param_buffer.size();
                 binary_param_buffer.resize(offset + 16);
                 to_binary_key_buf(gte, binary_param_buffer.data() + offset, 8);
                 c_params.push_back({binary_param_buffer.data() + offset, 8});
                 to_binary_key_buf(lte, binary_param_buffer.data() + offset + 8, 8);
                 c_params.push_back({binary_param_buffer.data() + offset + 8, 8});
            } else if (val.IsNumber()) {
                param_storage.push_back(val.ToString());
                c_params.push_back({param_storage.back().c_str(), param_storage.back().length()});
            }
        }
        
        if (step.Get("op_type").As<Napi::String>().Utf8Value() == "traverse" && step.Has("filter")) {
            Napi::Object filter_obj = step.Get("filter").As<Napi::Object>();
            Napi::Array filter_keys = filter_obj.GetPropertyNames();
            for (uint32_t j = 0; j < filter_keys.Length(); ++j) {
                Napi::Value key_val = filter_keys.Get(j);
                std::string key_str = key_val.As<Napi::String>();
                std::string val_str = filter_obj.Get(key_val).ToString(); 

                param_storage.push_back(key_str);
                c_params.push_back({param_storage.back().c_str(), param_storage.back().length()});
                param_storage.push_back(val_str);
                c_params.push_back({param_storage.back().c_str(), param_storage.back().length()});
            }
        }
    }
    
    StaxResultSet rs_handle = staxdb_graph_execute_plan(graph_handle_, plan_id, c_params.data(), c_params.size());
    if (!rs_handle) {
        return env.Null();
    }
    if (rs_handle->result_type == GRAPH_ID_RESULT) {
        rs_handle->kv_data = (void*)graph_handle_;
    }
    return ResultSetWrap::NewInstance(env, rs_handle);
}



Napi::FunctionReference ResultSetWrap::constructor;

Napi::Object ResultSetWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxResultSet", {
        InstanceMethod("getPage", &ResultSetWrap::GetPage),
        InstanceMethod("getTotalCount", &ResultSetWrap::GetTotalCount),
        InstanceMethod("close", &ResultSetWrap::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    return exports;
}

Napi::Object ResultSetWrap::NewInstance(Napi::Env env, StaxResultSet rs_handle) {
    Napi::Object obj = constructor.New({});
    ResultSetWrap* rs_wrap = Unwrap(obj);
    rs_wrap->result_set_handle_ = rs_handle;
    
    if (!rs_wrap->result_set_handle_ && strlen(staxdb_get_last_error()) > 0) {
        Napi::Error::New(env, std::string("Failed to create native result set: ") + staxdb_get_last_error()).ThrowAsJavaScriptException();
        return env.Null().As<Napi::Object>();
    }
    obj.Set("result_type", Napi::Number::New(env, rs_handle ? rs_handle->result_type : 0));
    return obj;
}

ResultSetWrap::ResultSetWrap(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ResultSetWrap>(info) {}

ResultSetWrap::~ResultSetWrap() {
    if (!is_closed_ && result_set_handle_) {
        staxdb_resultset_free(result_set_handle_);
        result_set_handle_ = nullptr;
    }
}

Napi::Value ResultSetWrap::GetPage(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_) {
        Napi::Error::New(env, "Result set is already closed.").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    uint32_t page_number = info[0].As<Napi::Number>().Uint32Value();
    uint32_t page_size = info[1].As<Napi::Number>().Uint32Value();

    StaxPageResult page_data = staxdb_resultset_get_page(result_set_handle_, page_number, page_size);

    Napi::Object page_obj = Napi::Object::New(env);
    page_obj.Set("page_number", Napi::Number::New(env, page_data.page_number));
    page_obj.Set("total_pages", Napi::Number::New(env, page_data.total_pages));
    page_obj.Set("total_results", Napi::Number::New(env, page_data.total_results));
    
    Napi::Array results_arr = Napi::Array::New(env, page_data.results_in_page);
    for (uint32_t i = 0; i < page_data.results_in_page; ++i) {
        const StaxKVPair& pair = page_data.results[i];
        
        if (result_set_handle_->result_type == GRAPH_ID_RESULT) {
            Napi::Object result_obj = Napi::Object::New(env);
            
            uint32_t obj_id = from_binary_key_u32(std::string_view(pair.key.data, pair.key.len));
            result_obj.Set("__stax_id", Napi::Number::New(env, obj_id));

            std::string_view serialized_obj(pair.value.data, pair.value.len);
            size_t start = 0;
            while(start < serialized_obj.length()) {
                size_t pipe_pos = serialized_obj.find('|', start);
                if (pipe_pos == std::string_view::npos) pipe_pos = serialized_obj.length();
                std::string_view token = serialized_obj.substr(start, pipe_pos - start);
                size_t colon_pos = token.find(':');
                if (colon_pos != std::string_view::npos) {
                    std::string key(token.substr(0, colon_pos));
                    std::string value(token.substr(colon_pos + 1));
                    result_obj.Set(key, value);
                }
                start = pipe_pos + 1;
            }
            results_arr[i] = result_obj;
        } else {
             Napi::Object result_obj = Napi::Object::New(env);
            result_obj.Set("key", Napi::Buffer<char>::Copy(env, pair.key.data, pair.key.len));
            result_obj.Set("value", Napi::Buffer<char>::Copy(env, pair.value.data, pair.value.len));
            results_arr[i] = result_obj;
        }
    }
    page_obj.Set("results", results_arr);

    return page_obj;
}

Napi::Value ResultSetWrap::GetTotalCount(const Napi::CallbackInfo& info) {
    if (is_closed_) return Napi::Number::New(info.Env(), 0);
    uint64_t count = staxdb_resultset_get_total_count(result_set_handle_);
    return Napi::Number::New(info.Env(), count);
}

void ResultSetWrap::Close(const Napi::CallbackInfo& info) {
    if (is_closed_) return;
    staxdb_resultset_free(result_set_handle_);
    result_set_handle_ = nullptr;
    is_closed_ = true;
}



Napi::FunctionReference TransactionWrap::constructor;

Napi::Object TransactionWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxReadTransaction", {
        InstanceMethod("get", &TransactionWrap::Get),
        InstanceMethod("commit", &TransactionWrap::Commit),
        InstanceMethod("abort", &TransactionWrap::Abort),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    return exports;
}

Napi::Object TransactionWrap::NewInstance(Napi::Env env, DatabaseWrap* db_wrap, StaxCollection col_idx, bool is_read_only) {
    Napi::Object obj = constructor.New({});
    TransactionWrap* txn_wrap = Unwrap(obj);
    txn_wrap->db_instance_ = db_wrap->db_instance_;
    txn_wrap->col_idx_ = col_idx;
    txn_wrap->ctx_ = db_wrap->db_instance_->begin_transaction_context(0, is_read_only);
    return obj;
}

TransactionWrap::TransactionWrap(const Napi::CallbackInfo& info) : Napi::ObjectWrap<TransactionWrap>(info) {}
TransactionWrap::~TransactionWrap() {
    if (!is_finished_) { db_instance_->abort(ctx_); }
}

Napi::Value TransactionWrap::Get(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) { Napi::Error::New(env, "Transaction is already closed.").ThrowAsJavaScriptException(); return env.Undefined(); }
    std::string key = info[0].As<Napi::String>();
    try {
        Collection& col = db_instance_->get_collection_by_idx(col_idx_);
        auto record = col.get(ctx_, key);
        if (record.has_value()) {
            return Napi::Buffer<char>::Copy(env, record->value_ptr, record->value_len);
        }
        return env.Null();
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }
}

void TransactionWrap::Commit(const Napi::CallbackInfo& info) {
    if (is_finished_) return;
    is_finished_ = true;
}

void TransactionWrap::Abort(const Napi::CallbackInfo& info) {
    if (is_finished_) return;
    db_instance_->abort(ctx_);
    is_finished_ = true;
}



class BatchInsertWorker : public Napi::AsyncWorker {
public:
    BatchInsertWorker(Napi::Function& callback, StaxDB db, StaxCollection col, Napi::Array& js_array)
        : Napi::AsyncWorker(callback), db_handle_(db), col_(col) {
        kv_data_.reserve(js_array.Length());
        for (uint32_t i = 0; i < js_array.Length(); ++i) {
            Napi::Object obj = js_array.Get(i).As<Napi::Object>();
            kv_data_.push_back({obj.Get("key").As<Napi::String>(), obj.Get("value").As<Napi::String>()});
        }
    }
protected:
    void Execute() override {
        Database* cpp_db = staxdb_get_db_instance(db_handle_);
        if (!cpp_db) { SetError("Failed to get DB instance in worker thread."); return; }
        size_t num_pairs = kv_data_.size();
        if (num_pairs == 0) return;
        size_t num_threads = cpp_db->get_num_configured_threads();
        std::vector<std::thread> threads;
        size_t items_per_thread = (num_pairs + num_threads - 1) / num_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                Collection& col = cpp_db->get_collection_by_idx(col_);
                TxnContext ctx = col.begin_transaction_context(t, false);
                TransactionBatch batch;
                size_t start = t * items_per_thread;
                size_t end = std::min(start + items_per_thread, num_pairs);
                for (size_t i = start; i < end; ++i) {
                    col.insert(ctx, batch, kv_data_[i].first, kv_data_[i].second);
                }
                col.commit(ctx, batch);
            });
        }
        for (auto& th : threads) th.join();
        const char* err = staxdb_get_last_error();
        if (err && strlen(err) > 0) SetError(err);
    }
    void OnOK() override {
        Napi::HandleScope scope(Env());
        Callback().Call({});
    }
    void OnError(const Napi::Error& e) override {
        Napi::HandleScope scope(Env());
        Callback().Call({e.Value()});
    }

private:
    StaxDB db_handle_;
    StaxCollection col_;
    std::vector<std::pair<std::string, std::string>> kv_data_;
};

void DatabaseWrap::InsertBatchAsync(const Napi::CallbackInfo& info) {
    StaxCollection col = info[0].As<Napi::Number>().Uint32Value();
    Napi::Array arr = info[1].As<Napi::Array>();
    Napi::Function cb = info[2].As<Napi::Function>();
    (new BatchInsertWorker(cb, this->db_handle_, col, arr))->Queue();
}


class MultiGetWorker : public Napi::AsyncWorker {
public:
    MultiGetWorker(Napi::Function& callback, StaxDB db, StaxCollection col, Napi::Array& js_keys)
        : Napi::AsyncWorker(callback), db_handle_(db), col_(col) {
        keys_to_get_.reserve(js_keys.Length());
        for (uint32_t i = 0; i < js_keys.Length(); ++i) {
            keys_to_get_.push_back(js_keys.Get(i).As<Napi::String>());
        }
    }

protected:
    void Execute() override {
        Database* cpp_db = staxdb_get_db_instance(db_handle_);
        if (!cpp_db) { SetError("Failed to get DB instance in worker thread."); return; }
        
        Collection& col = cpp_db->get_collection_by_idx(col_);
        TxnContext ctx = col.begin_transaction_context(0, true);

        std::vector<std::string_view> key_views;
        key_views.reserve(keys_to_get_.size());
        for(const auto& k : keys_to_get_) {
            key_views.push_back(k);
        }

        col.get_critbit_tree().multi_get_simd(ctx, key_views, results_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::HandleScope scope(env);
        Napi::Array js_results = Napi::Array::New(env, results_.size());
        for (size_t i = 0; i < results_.size(); ++i) {
            if (results_[i].has_value()) {
                js_results[i] = Napi::Buffer<char>::Copy(env, results_[i]->value_ptr, results_[i]->value_len);
            } else {
                js_results[i] = env.Null();
            }
        }
        Callback().Call({env.Null(), js_results});
    }

    void OnError(const Napi::Error& e) override {
        Napi::HandleScope scope(Env());
        Callback().Call({e.Value()});
    }

private:
    StaxDB db_handle_;
    StaxCollection col_;
    std::vector<std::string> keys_to_get_;
    std::vector<std::optional<RecordData>> results_;
};

void DatabaseWrap::MultiGetAsync(const Napi::CallbackInfo& info) {
    StaxCollection col = info[0].As<Napi::Number>().Uint32Value();
    Napi::Array arr = info[1].As<Napi::Array>();
    Napi::Function cb = info[2].As<Napi::Function>();
    (new MultiGetWorker(cb, this->db_handle_, col, arr))->Queue();
}


Napi::Value GetLastError(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    return Napi::String::New(env, staxdb_get_last_error());
}


Napi::Object Initialize(Napi::Env env, Napi::Object exports) {
    DatabaseWrap::Init(env, exports);
    TransactionWrap::Init(env, exports);
    ResultSetWrap::Init(env, exports);
    GraphWrap::Init(env, exports);
    KVTransactionWrap::Init(env, exports);
    GraphTransactionWrap::Init(env, exports);
    exports.Set(Napi::String::New(env, "getLastError"), Napi::Function::New(env, GetLastError, "getLastError"));
    return exports;
}

NODE_API_MODULE(staxdb, Initialize)