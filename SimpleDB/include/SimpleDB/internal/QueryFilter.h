#ifndef _SIMPLEDB_QUERY_FILTER
#define _SIMPLEDB_QUERY_FILTER

#include <cstring>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

enum CompareOp {
    EQ,  // =
    NE,  // <>
    LT,  // <
    LE,  // <=
    GT,  // >
    GE,  // >=
};

struct CompareValueCondition {
    std::string columnName;
    CompareOp op;
    ColumnValue value;

    CompareValueCondition(const std::string &columnName, CompareOp op,
                          const ColumnValue &value)
        : columnName(columnName), op(op), value(value) {}

    CompareValueCondition(const std::string &columnName, CompareOp op,
                          const char *string)
        : columnName(columnName), op(op) {
        std::strcpy(value.stringValue, string);
    }

    CompareValueCondition() = default;

    static CompareValueCondition eq(const char *columnName,
                                    const ColumnValue &value) {
        return CompareValueCondition(columnName, EQ, value);
    }

    static CompareValueCondition eq(const char *columnName,
                                    const char *string) {
        return CompareValueCondition(columnName, EQ, string);
    }
};

struct CompareNullCondition {
    std::string columnName;
    bool isNull;
    CompareNullCondition(const std::string &columnName, bool isNull)
        : columnName(columnName), isNull(isNull) {}
    CompareNullCondition() = default;
};

struct QuerySelector {
    enum Type { COLUMN, COUNT_STAR, COUNT_COL, AVG, MAX, MIN, SUM };
    Type type;
    std::string columnName;

    std::string getColumnName() const;
};

struct VirtualTable {
    VirtualTable() = default;
    VirtualTable(const std::vector<ColumnInfo> &columns) {
        this->columns = columns;
        for (int i = 0; i < columns.size(); i++) {
            columnNameMap[columns[i].name] = i;
        }
    }

    std::map<std::string, int> columnNameMap;
    std::vector<ColumnInfo> columns;

    int getColumnIndex(const std::string name) {
        auto it = columnNameMap.find(name);
        return it == columnNameMap.end() ? -1 : it->second;
    }
};

struct BaseFilter {
    virtual ~BaseFilter() {}
    virtual std::pair<bool, bool> apply(Columns &columns) = 0;
    virtual bool finalize(Columns &columns) { return false; }
};

struct ValueConditionFilter : public BaseFilter {
    ValueConditionFilter() = default;
    ~ValueConditionFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareValueCondition condition;
    VirtualTable *table;
};

struct NullConditionFilter : public BaseFilter {
    NullConditionFilter() = default;
    ~NullConditionFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareNullCondition condition;
    VirtualTable *table;
};

struct SelectFilter : public BaseFilter {
    struct Context {
        union {
            int intValue;
            float floatValue;
        } value;
        bool isNull = true;
        void initializeInt(int value) {
            if (isNull) {
                isNull = false;
                this->value.intValue = value;
            }
        }
        void initializeFloat(float value) {
            if (isNull) {
                isNull = false;
                this->value.floatValue = value;
            }
        }
    };
    SelectFilter() = default;
    ~SelectFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    void build();
    virtual bool finalize(Columns &columns) override;
    std::vector<QuerySelector> selectors;
    std::vector<int> selectIndexes;
    std::vector<Context> selectContexts;
    bool isAggregated;
    int count = 0;
    VirtualTable *table;
};

struct LimitFilter : public BaseFilter {
    LimitFilter() : limit(-1) {}
    ~LimitFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    int limit;
    int count = 0;
};

struct OffsetFilter : public BaseFilter {
    OffsetFilter() : offset(0) {}
    ~OffsetFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    int offset;
    int count = 0;
};

struct AggregatedFilter : public BaseFilter {
    AggregatedFilter() = default;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    virtual bool finalize(Columns &columns) override;
    std::vector<BaseFilter *> filters;
};

}  // namespace Internal
}  // namespace SimpleDB
#endif