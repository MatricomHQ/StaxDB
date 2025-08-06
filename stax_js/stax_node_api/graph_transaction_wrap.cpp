#include "graph_transaction_wrap.h"
#include "database_wrap.h"

Napi::FunctionReference GraphTransactionWrap::constructor;


static void convert_js_obj_to_properties_for_txn(Napi::Env env, Napi::Object data_obj, std::vector<StaxObjectProperty>& c_properties, std::vector<std::string>& string_storage) {
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

Napi::Object GraphTransactionWrap::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "StaxGraphTransaction", {
        InstanceMethod("insertObject", &GraphTransactionWrap::InsertObject),
        InstanceMethod("updateObject", &GraphTransactionWrap::UpdateObject),
        InstanceMethod("deleteObject", &GraphTransactionWrap::DeleteObject),
        InstanceMethod("insertRelationship", &GraphTransactionWrap::InsertRelationship),
        InstanceMethod("commit", &GraphTransactionWrap::Commit),
        InstanceMethod("abort", &GraphTransactionWrap::Abort),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("StaxGraphTransaction", func);
    return exports;
}

Napi::Object GraphTransactionWrap::NewInstance(Napi::Env env, DatabaseWrap* db_wrap) {
    Napi::Object obj = constructor.New({});
    GraphTransactionWrap* gtxn_wrap = Unwrap(obj);
    gtxn_wrap->db_instance_ = db_wrap->db_instance_;
    gtxn_wrap->txn_ = std::make_unique<GraphTransaction>(db_wrap->db_instance_, 0);
    return obj;
}

GraphTransactionWrap::GraphTransactionWrap(const Napi::CallbackInfo& info) : Napi::ObjectWrap<GraphTransactionWrap>(info) {}

GraphTransactionWrap::~GraphTransactionWrap() {
    if (txn_ && !is_finished_) {
        txn_->abort();
    }
}

Napi::Value GraphTransactionWrap::InsertObject(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return env.Null();
    }
    Napi::Object data_obj = info[0].As<Napi::Object>();
    
    try {
        std::vector<StaxObjectProperty> c_properties;
        std::vector<std::string> string_storage;
        convert_js_obj_to_properties_for_txn(env, data_obj, c_properties, string_storage);
        
        
        DbGeneration* active_gen = db_instance_->get_active_generation();
        if (!active_gen) {
            Napi::Error::New(env, "No active database generation found.").ThrowAsJavaScriptException();
            return env.Null();
        }

        const uint32_t graph_collection_idx = db_instance_->get_collection("graph_ofv"); 
        CollectionEntry& graph_collection_entry = active_gen->get_collection_entry_ref(graph_collection_idx);
        uint32_t obj_id = graph_collection_entry.object_id_counter.fetch_add(1, std::memory_order_relaxed);
        
        for (const auto& prop : c_properties) {
            std::string_view field_name_sv = to_string_view(prop.field);
            uint32_t field_id = global_id_map.get_or_create_id(field_name_sv);
            switch (prop.type) {
                case STAX_PROP_STRING:
                    txn_->insert_fact_string(obj_id, field_id, field_name_sv, to_string_view(prop.value.string_val));
                    break;
                case STAX_PROP_NUMERIC:
                    txn_->insert_fact_numeric(obj_id, field_id, field_name_sv, prop.value.numeric_val);
                    break;
                case STAX_PROP_GEO:
                    txn_->insert_fact_geo(obj_id, field_id, field_name_sv, prop.value.geo_val.lat, prop.value.geo_val.lon);
                    break;
            }
        }
        return Napi::Number::New(env, obj_id);

    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Null();
    }
}

void GraphTransactionWrap::UpdateObject(const Napi::CallbackInfo& info) {
     Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    uint32_t obj_id = info[0].As<Napi::Number>().Uint32Value();
    Napi::Object data_obj = info[1].As<Napi::Object>();

    try {
        std::vector<StaxObjectProperty> c_properties;
        std::vector<std::string> string_storage;
        convert_js_obj_to_properties_for_txn(env, data_obj, c_properties, string_storage);
        
        txn_->update_object(obj_id, c_properties);

    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void GraphTransactionWrap::DeleteObject(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    uint32_t obj_id = info[0].As<Napi::Number>().Uint32Value();
    try {
        txn_->clear_object_facts(obj_id);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void GraphTransactionWrap::InsertRelationship(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    uint32_t source_id = info[0].As<Napi::Number>().Uint32Value();
    std::string rel_type = info[1].As<Napi::String>();
    uint32_t target_id = info[2].As<Napi::Number>().Uint32Value();
    try {
        uint32_t rel_id = global_id_map.get_or_create_id(rel_type);
        txn_->insert_fact(source_id, rel_id, target_id);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void GraphTransactionWrap::Commit(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    is_finished_ = true;
    try {
        txn_->commit();
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}

void GraphTransactionWrap::Abort(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_finished_) {
        Napi::Error::New(env, "Transaction has already been committed or aborted.").ThrowAsJavaScriptException();
        return;
    }
    is_finished_ = true;
    try {
        txn_->abort();
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
}