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
    QueryBuilder &condition(const CompareValueCondition &condition);
    QueryBuilder &condition(const std::string &columnName, CompareOp op,
                            const char *string);
    QueryBuilder &nullCondition(const CompareNullCondition &condition);
    QueryBuilder &nullCondition(const std::string &columnName, bool isNull);
    QueryBuilder &select(const std::string &column);
    QueryBuilder &limit(int count);

    [[nodiscard]] Result execute();

    // QueryDataSource requirements, allowing chained pipelines.
    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;

private:
    std::vector<ValueConditionFilter> valueConditionFilters;
    std::vector<NullConditionFilter> nullConditionFilters;
    SelectFilter selectFilter;
    LimitFilter limitFilter;

    QueryDataSource *dataSourceRawPtr = nullptr;
    std::shared_ptr<QueryDataSource> dataSourceSharedPtr;
    bool isRaw;
    QueryDataSource *getDataSource();
    VirtualTable virtualTable;

    void checkDataSource();
    AggregatedFilter aggregateAllFilters();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif