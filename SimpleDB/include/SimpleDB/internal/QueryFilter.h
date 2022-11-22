#ifndef _SIMPLEDB_QUERY_FILTER
#define _SIMPLEDB_QUERY_FILTER

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

enum CompareOp {
    EQ,        // =
    NE,        // <>
    LT,        // <
    LE,        // <=
    GT,        // >
    GE,        // >=
    IS_NULL,   // IS NULL
    NOT_NULL,  // NOT NULL
    LIKE,      // LIKE
};

struct CompareCondition {
    const char *columnName;
    CompareOp op;
    const char *value;

    CompareCondition(const char *columnName, CompareOp op, const char *value)
        : columnName(columnName), op(op), value(value) {}
    CompareCondition() = default;

    static CompareCondition eq(const char *columnName, const char *value) {
        return CompareCondition(columnName, EQ, value);
    }
};

using CompareConditions = std::vector<CompareCondition>;

struct VirtualTable {
    VirtualTable() = default;
    VirtualTable(const std::vector<ColumnInfo> &columns) {
        for (int i = 0; i < columns.size(); i++) {
            columnNameMap[columns[i].name] = i;
        }
    }

    std::map<std::string, int> columnNameMap;

    int getColumnIndex(const std::string name) {
        auto it = columnNameMap.find(name);
        return it == columnNameMap.end() ? -1 : it->second;
    }
};

struct BaseFilter {
    virtual ~BaseFilter() {}
    virtual std::pair<bool, bool> apply(Columns &columns) = 0;
};

struct ConditionFilter : public BaseFilter {
    ConditionFilter() = default;
    ~ConditionFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareCondition condition;
    VirtualTable *table;
};

struct SelectFilter : public BaseFilter {
    SelectFilter() = default;
    ~SelectFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    std::set<std::string> columnNames;
    VirtualTable *table;
};

struct LimitFilter : public BaseFilter {
    LimitFilter() : limit(-1) {}
    ~LimitFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    int limit;
    int count = 0;
};

struct AggregatedFilter : public BaseFilter {
    AggregatedFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    std::vector<BaseFilter *> filters;
};

}  // namespace Internal
}  // namespace SimpleDB
#endif