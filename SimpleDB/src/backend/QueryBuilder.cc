#include "internal/QueryBuilder.h"

#include <vector>

#include "Error.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"

namespace SimpleDB {
namespace Internal {
QueryBuilder &QueryBuilder::scan(Table *table) {
    if (scannedTable != nullptr) {
        throw MultipleScanError();
    }
    scannedTable = table;
    return *this;
}

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
    // TODO: Index-based scan.
    checkProvider();

    Result result;
    Columns bufColumns;
    AggregatedFilter filter = aggregateAllFilters();

    for (int page = 1; page < scannedTable->meta.numUsedPages; page++) {
        PageHandle *handle = scannedTable->getHandle(page);
        for (int slot = 1; slot < NUM_SLOT_PER_PAGE; slot++) {
            if (scannedTable->occupied(*handle, slot)) {
                RecordID rid = {page, slot};

                // TODO: Optimization: only get necessary columns.
                scannedTable->get(rid, bufColumns);
                if (filter.apply(bufColumns)) {
                    result.push_back({rid, bufColumns});
                }
            }
        }
    }

    return result;
}

void QueryBuilder::checkProvider() {
    // Check query source.
    // TODO: Other sources.
    if (scannedTable == nullptr) {
        throw NoScanProviderError();
    }
}

AggregatedFilter QueryBuilder::aggregateAllFilters() {
    // Create a virtual table.
    // TODO: other source.
    // for (int i = 0)
    virtualTable.columns.resize(scannedTable->meta.numColumn);
    for (int i = 0; i < scannedTable->meta.numColumn; i++) {
        ColumnMeta &column = scannedTable->meta.columns[i];
        virtualTable.columns[i] = {
            .name = column.name, .type = column.type, .size = column.size};
    }

    std::vector<BaseFilter *> filters;
    // Condition filters comes before selectors.
    for (int i = 0; i < conditionFilters.size(); i++) {
        conditionFilters[i].table = &virtualTable;
        filters.push_back(&conditionFilters[i]);
    }
    // The select filter comes after.
    if (selectFilter.columnNames.size() > 0) {
        selectFilter.table = &virtualTable;
        filters.push_back(&selectFilter);
    }
    // Add limit filter at the back.
    filters.push_back(&limitFilter);

    return AggregatedFilter(filters);
}

// int QueryBuilder::VirtualTable

}  // namespace Internal
}  // namespace SimpleDB