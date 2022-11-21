#include "internal/QueryBuilder.h"

#include <SimpleDB/internal/Column.h>

#include <vector>

#include "Error.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/QueryFilter.h"

namespace SimpleDB {
namespace Internal {

QueryBuilder::QueryBuilder(QueryDataSource *source) : dataSource(source) {}

QueryBuilder &QueryBuilder::condition(const CompareCondition &condition) {
    ConditionFilter filter;
    filter.condition = condition;
    conditionFilters.push_back(filter);
    return *this;
}

QueryBuilder &QueryBuilder::condition(const std::string &columnName,
                                      CompareOp op, const char *value) {
    return condition(CompareCondition(columnName.c_str(), op, value));
}

QueryBuilder &QueryBuilder::select(const std::string &column) {
    // TODO: Pre-check.
    selectFilter.columnNames.insert(column);
    return *this;
}

QueryBuilder &QueryBuilder::limit(int count) {
    if (limitFilter.limit != -1) {
        Logger::log(WARNING,
                    "QueryBuilder: limit is already set (%d) and is "
                    "overwritten by %d",
                    limitFilter.limit, count);
    }
    if (count < 0) {
        throw InvalidLimitError();
    }
    limitFilter.limit = count;
    return *this;
}

QueryBuilder::Result QueryBuilder::execute() {
    Result result;

    this->iterate([&](RecordID rid, Columns &columns) {
        result.emplace_back(rid, columns);
        return true;
    });

    return result;
}

// TODO: Add tests for this.
void QueryBuilder::iterate(IterateCallback callback) {
    checkDataSource();

    AggregatedFilter filter = aggregateAllFilters();

    dataSource->iterate([&](RecordID rid, Columns &columns) {
        auto [accept, continue_] = filter.apply(columns);
        if (accept) {
            return callback(rid, columns) && continue_;
        }
        return continue_;
    });
}

std::vector<ColumnInfo> QueryBuilder::getColumnMeta() {
    checkDataSource();
    auto columnMetas = dataSource->getColumnMeta();
    std::vector<ColumnInfo> result;

    // We should "apply" SelectFilter here.
    for (auto &columnMeta : columnMetas) {
        if (selectFilter.columnNames.empty() ||
            selectFilter.columnNames.find(columnMeta.name) !=
                selectFilter.columnNames.end()) {
            result.push_back(columnMeta);
        }
    }

    return result;
}

void QueryBuilder::checkDataSource() {
    if (dataSource == nullptr) {
        throw NoScanDataSourceError();
    }
}

AggregatedFilter QueryBuilder::aggregateAllFilters() {
    // Create a virtual table.
    virtualTable.columns = dataSource->getColumnMeta();

    AggregatedFilter filter;
    // Condition filters comes before selectors.
    for (int i = 0; i < conditionFilters.size(); i++) {
        conditionFilters[i].table = &virtualTable;
        filter.filters.push_back(&conditionFilters[i]);
    }
    // The select filter comes after.
    if (selectFilter.columnNames.size() > 0) {
        selectFilter.table = &virtualTable;
        filter.filters.push_back(&selectFilter);
    }
    // Add limit filter at the back.
    filter.filters.push_back(&limitFilter);

    return filter;
}

}  // namespace Internal
}  // namespace SimpleDB