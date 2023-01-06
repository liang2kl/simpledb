#ifndef _SIMPLEDB_QUERY_BUILDER_H
#define _SIMPLEDB_QUERY_BUILDER_H

#include <memory>
#include <string>
#include <vector>

#include "internal/QueryDataSource.h"
#include "internal/QueryFilter.h"

namespace SimpleDB {
namespace Internal {

// Build a query schedule using a series of selectors.
class QueryBuilder : public QueryDataSource {
public:
    using Result = std::vector<std::pair<RecordID, Columns>>;
    QueryBuilder(QueryDataSource *dataSource);
    QueryBuilder(std::shared_ptr<QueryDataSource> dataSource);
    QueryBuilder() = default;

    QueryBuilder &condition(const CompareValueCondition &condition);
    QueryBuilder &condition(const std::string &columnName, CompareOp op,
                            const char *string);
    QueryBuilder &condition(const std::string &columnName, CompareOp op,
                            int value);
    QueryBuilder &condition(const ColumnId &columnId, CompareOp op,
                            const char *string);
    QueryBuilder &condition(const ColumnId &columnId, CompareOp op,
                            const ColumnValue &value);

    QueryBuilder &condition(const CompareColumnCondition &condition);
    QueryBuilder &nullCondition(const CompareNullCondition &condition);
    QueryBuilder &nullCondition(const std::string &columnName, bool isNull);
    QueryBuilder &nullCondition(const ColumnId &columnId, bool isNull);
    QueryBuilder &select(const QuerySelector &selector);
    QueryBuilder &select(const std::string &column);
    QueryBuilder &select(const ColumnId &id);
    QueryBuilder &limit(int count);
    QueryBuilder &offset(int offset);

    [[nodiscard]] Result execute();

    // QueryDataSource requirements, allowing chained pipelines.
    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;

    bool validForUpdateOrDelete() const;
    QueryDataSource *getDataSource();

private:
    std::vector<ValueConditionFilter> valueConditionFilters;
    std::vector<ColumnConditionFilter> columnConditionFilters;
    std::vector<NullConditionFilter> nullConditionFilters;
    SelectFilter selectFilter;
    LimitFilter limitFilter;
    OffsetFilter offsetFilter;

    QueryDataSource *dataSourceRawPtr = nullptr;
    std::shared_ptr<QueryDataSource> dataSourceSharedPtr;
    bool isRaw;
    VirtualTable virtualTable;

    void checkDataSource();
    AggregatedFilter aggregateAllFilters();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif