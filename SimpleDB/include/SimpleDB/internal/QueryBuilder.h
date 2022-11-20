#ifndef _SIMPLEDB_QUERY_BUILDER_H
#define _SIMPLEDB_QUERY_BUILDER_H

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
    QueryBuilder &condition(const CompareCondition &condition);
    QueryBuilder &condition(const std::string &columnName, CompareOp op,
                            const char *value);
    QueryBuilder &select(const std::string &column);
    QueryBuilder &limit(int count);

    [[nodiscard]] Result execute();

    // QueryDataSource requirements, allowing chained pipelines.
    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnMeta() override;

private:
    std::vector<ConditionFilter> conditionFilters;
    SelectFilter selectFilter;
    LimitFilter limitFilter;

    QueryDataSource *dataSource = nullptr;
    VirtualTable virtualTable;

    void checkDataSource();
    AggregatedFilter aggregateAllFilters();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif