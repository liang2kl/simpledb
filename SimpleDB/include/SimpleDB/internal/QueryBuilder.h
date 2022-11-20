#ifndef _SIMPLEDB_QUERY_BUILDER_H
#define _SIMPLEDB_QUERY_BUILDER_H

#include <string>
#include <vector>

#include "internal/QueryFilter.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

// Build a query schedule using a series of selectors.
class QueryBuilder {
public:
    using Result = std::vector<std::pair<RecordID, Columns>>;
    QueryBuilder() = default;
    QueryBuilder &scan(Table *table);
    QueryBuilder &condition(const CompareCondition &condition);
    QueryBuilder &condition(const std::string &columnName, CompareOp op,
                            const char *value);
    QueryBuilder &select(const std::string &column);
    QueryBuilder &limit(int count);

    [[nodiscard]] Result execute();
    void iterativeExecute(
        std::function<void(const std::pair<RecordID, Columns> &)>);

private:
    std::vector<ConditionFilter> conditionFilters;
    SelectFilter selectFilter;
    LimitFilter limitFilter;

    Table *scannedTable = nullptr;
    VirtualTable virtualTable;

    void checkProvider();
    AggregatedFilter aggregateAllFilters();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif