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

struct ColumnId {
    std::string tableName;
    std::string columnName;

    std::string getDesc() const {
        return tableName.empty() ? columnName : tableName + "." + columnName;
    }
};

struct CompareValueCondition {
    ColumnId columnId;
    CompareOp op;
    ColumnValue value;

    CompareValueCondition(const ColumnId &id, CompareOp op,
                          const ColumnValue &value)
        : columnId(id), op(op), value(value) {}

    CompareValueCondition(const ColumnId &id, CompareOp op, const char *string)
        : columnId(id), op(op) {
        std::strcpy(value.stringValue, string);
    }

    CompareValueCondition() = default;

    // static CompareValueCondition eq(const char *columnName,
    //                                 const ColumnValue &value) {
    //     return CompareValueCondition(columnName, EQ, value);
    // }

    // static CompareValueCondition eq(const char *columnName,
    //                                 const char *string) {
    //     return CompareValueCondition(columnName, EQ, string);
    // }
};

struct CompareNullCondition {
    ColumnId columnId;
    bool isNull;
    CompareNullCondition(const ColumnId &id, bool isNull)
        : columnId(id), isNull(isNull) {}
    CompareNullCondition() = default;
};

struct CompareColumnCondition {
    ColumnId lhs;
    CompareOp op;
    ColumnId rhs;
    CompareColumnCondition(const ColumnId &lhs, CompareOp op,
                           const ColumnId &rhs)
        : lhs(lhs), op(op), rhs(rhs) {}
    CompareColumnCondition() = default;
};

struct QuerySelector {
    enum Type { COLUMN, COUNT_STAR, COUNT_COL, AVG, MAX, MIN, SUM };
    Type type;
    ColumnId column;

    std::string getColumnName() const;
};

struct VirtualTable {
    VirtualTable() = default;
    VirtualTable(const std::vector<ColumnInfo> &columns) {
        this->columns = columns;
    }

    std::vector<ColumnInfo> columns;

    int getColumnIndex(const ColumnId id) {
        int index = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns[i].columnName == id.columnName) {
                if (id.tableName.empty() ||
                    columns[i].tableName == id.tableName) {
                    if (index != -1) {
                        throw AmbiguousColumnError(id.getDesc());
                    }
                    index = i;
                }
            }
        }
        return index;
    }
};

struct BaseFilter {
    virtual ~BaseFilter() {}
    virtual std::pair<bool, bool> apply(Columns &columns) = 0;
    virtual void build(){};
    virtual bool finalize(Columns &columns) { return false; }
};

struct ValueConditionFilter : public BaseFilter {
    ValueConditionFilter() = default;
    ~ValueConditionFilter() = default;
    virtual void build() override;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareValueCondition condition;
    VirtualTable *table;
    int columnIndex;
};

struct NullConditionFilter : public BaseFilter {
    NullConditionFilter() = default;
    ~NullConditionFilter() = default;
    virtual void build() override;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareNullCondition condition;
    VirtualTable *table;
    int columnIndex;
};

struct ColumnConditionFilter : public BaseFilter {
    ColumnConditionFilter() = default;
    ~ColumnConditionFilter() = default;
    virtual void build() override;
    virtual std::pair<bool, bool> apply(Columns &columns) override;
    CompareColumnCondition condition;
    VirtualTable *table;
    int columnIndex1;
    int columnIndex2;
};

struct SelectFilter : public BaseFilter {
    struct Context {
        union {
            int intValue;
            float floatValue;
        } value;
        bool isNull = true;
        int count = 0;
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
    void build() override;
    virtual bool finalize(Columns &columns) override;
    std::vector<QuerySelector> selectors;
    std::vector<int> selectIndexes;
    std::vector<Context> selectContexts;
    bool isAggregated;
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
    virtual void build() override;
    std::vector<BaseFilter *> filters;
};

}  // namespace Internal
}  // namespace SimpleDB
#endif