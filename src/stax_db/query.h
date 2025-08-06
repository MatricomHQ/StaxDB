#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <map>
#include <optional>
#include <variant>

#include "stax_db/path_engine.h" 
#include "stax_common/common_types.hpp"
#include "stax_db/db.h"


class FlexDoc {
public:
    FlexDoc(DataView raw_data); 
    std::optional<std::string_view> get_field(std::string_view field_name) const;
    bool is_valid() const { return data_.data != nullptr && data_.len > 0; }

private:
    DataView data_;
};

enum class QueryOp {
    EQ,
    GT,
    LT,
    GTE,
    LTE,
    BETWEEN,
    PREFIX
};

using QueryValue = std::variant<uint64_t, std::string_view>;

struct QueryCondition {
    std::string attribute_name;
    QueryOp op;
    QueryValue value1;
    std::optional<QueryValue> value2; 
};

class QueryBuilder {
public:
    QueryBuilder(Database* db, uint32_t collection_idx, std::string_view ns_param, size_t thread_id = 0);

    QueryBuilder& where(std::string_view attribute, QueryOp op, uint64_t value);
    QueryBuilder& where(std::string_view attribute, QueryOp op, uint64_t val1, uint64_t val2); 
    QueryBuilder& where_string(std::string_view attribute, QueryOp op, std::string_view value);

    QueryBuilder& limit(size_t max_results);
    QueryBuilder& select(std::initializer_list<std::string_view> fields);

    std::vector<FlexDoc> execute();

private:
    Database* db_;
    uint32_t collection_idx_;
    std::string ns_;
    size_t thread_id_;
    std::vector<QueryCondition> conditions_;
    std::vector<std::string> select_fields_;
    size_t limit_ = std::numeric_limits<size_t>::max();
};