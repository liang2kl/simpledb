#include "internal/QueryBuilder.h"

#include <vector>

#include "Error.h"
#include "internal/Column.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/QueryFilter.h"

namespace SimpleDB {
namespace Internal {

QueryBuilder::QueryBuilder(QueryDataSource *source)
    : dataSourceRawPtr(source), isRaw(true) {}

QueryBuilder::QueryBuilder(std::shared_ptr<QueryDataSource> source)
    : dataSourceSharedPtr(source), isRaw(false) {}

QueryDataSource *QueryBuilder::getDataSource() {
    return isRaw ? dataSourceRawPtr : dataSourceSharedPtr.get();
}

QueryBuilder &QueryBuilder::condition(const CompareValueCondition &condition) {
    if (getDataSource()->acceptCondition(condition)) {
        return *this;
    }
    ValueConditionFilter filter;
    filter.condition = condition;
    valueConditionFilters.push_back(filter);
    return *this;
}

QueryBuilder &QueryBuilder::condition(const ColumnId &columnId, CompareOp op,
                                      const ColumnValue &value) {
    return condition(CompareValueCondition{columnId, op, value});
}

QueryBuilder &QueryBuilder::condition(const std::string &columnName,
                                      CompareOp op, const char *string) {
    return condition(ColumnId{.columnName = columnName.c_str()}, op, string);
}

QueryBuilder &QueryBuilder::condition(const std::string &columnName,
                                      CompareOp op, int value) {
    CompareValueCondition cond;
    cond.value = ColumnValue{.intValue = value};
    cond.op = op;
    cond.columnId = {.columnName = columnName};
    return condition(cond);
}

QueryBuilder &QueryBuilder::condition(const ColumnId &id, CompareOp op,
                                      const char *string) {
    return condition(CompareValueCondition(id, op, string));
}

QueryBuilder &QueryBuilder::condition(const CompareColumnCondition &condition) {
    ColumnConditionFilter filter;
    filter.condition = condition;
    columnConditionFilters.push_back(filter);
    return *this;
}

QueryBuilder &QueryBuilder::nullCondition(
    const CompareNullCondition &condition) {
    NullConditionFilter filter;
    filter.condition = condition;
    nullConditionFilters.push_back(filter);
    return *this;
}

QueryBuilder &QueryBuilder::nullCondition(const std::string &columnName,
                                          bool isNull) {
    return nullCondition({.columnName = columnName.c_str()}, isNull);
}

QueryBuilder &QueryBuilder::nullCondition(const ColumnId &id, bool isNull) {
    return nullCondition(CompareNullCondition(id, isNull));
}

QueryBuilder &QueryBuilder::select(const QuerySelector &selector) {
    selectFilter.selectors.push_back(selector);
    return *this;
}

QueryBuilder &QueryBuilder::select(const std::string &column) {
    return select(ColumnId{.columnName = column.c_str()});
}

QueryBuilder &QueryBuilder::select(const ColumnId &id) {
    selectFilter.selectors.push_back({QuerySelector::COLUMN, id});
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

QueryBuilder &QueryBuilder::offset(int offset) {
    offsetFilter.offset = offset;
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

    // TODO: Check if is all about COUNT(*)

    AggregatedFilter filter = aggregateAllFilters();

    getDataSource()->iterate([&](RecordID rid, Columns &columns) {
        auto [accept, continue_] = filter.apply(columns);
        if (accept) {
            return callback(rid, columns) && continue_;
        }
        return continue_;
    });

    Columns columns;
    bool ret = filter.finalize(columns);
    if (ret) {
        callback(RecordID::NULL_RECORD, columns);
    }
}

std::vector<ColumnInfo> QueryBuilder::getColumnInfo() {
    checkDataSource();
    auto columnMetas = getDataSource()->getColumnInfo();

    std::vector<ColumnInfo> result;

    if (selectFilter.selectors.size() == 0) {
        return columnMetas;
    }

    for (const auto &selector : selectFilter.selectors) {
        if (selector.type == QuerySelector::COUNT_STAR) {
            result.push_back({.tableName = std::string(), /* TODO */
                              .columnName = selector.getColumnName(),
                              .type = INT});
        } else {
            int index = -1;
            for (int i = 0; i < columnMetas.size(); i++) {
                if (columnMetas[i].columnName == selector.column.columnName) {
                    if (selector.column.tableName.empty() && index != -1) {
                        throw AmbiguousColumnError(selector.column.getDesc());
                    }
                    if (selector.column.tableName.empty() ||
                        selector.column.tableName == columnMetas[i].tableName) {
                        index = i;
                    }
                }
            }

            if (index == -1) {
                throw ColumnNotFoundError(selector.column.getDesc());
            }
            if (selector.type == QuerySelector::COLUMN) {
                result.push_back(columnMetas[index]);
            } else {
                DataType type;
                switch (selector.type) {
                    case QuerySelector::AVG:
                        type = FLOAT;
                        break;
                    case QuerySelector::COUNT_COL:
                    case QuerySelector::COUNT_STAR:
                        type = INT;
                    default:
                        type = columnMetas[index].type;
                }
                result.push_back({.tableName = std::string(), /* TODO */
                                  .columnName = selector.getColumnName(),
                                  .type = type});
            }
        }
    }

    return result;
}

bool QueryBuilder::validForUpdateOrDelete() const {
    return selectFilter.selectors.size() == 0 && limitFilter.limit < 0 &&
           offsetFilter.offset == 0;
}

void QueryBuilder::checkDataSource() {
    if (getDataSource() == nullptr) {
        throw NoScanDataSourceError();
    }
}

AggregatedFilter QueryBuilder::aggregateAllFilters() {
    // Create a virtual table.
    virtualTable = VirtualTable(getDataSource()->getColumnInfo());

    AggregatedFilter filter;
    // Condition filters comes before selectors.
    for (int i = 0; i < nullConditionFilters.size(); i++) {
        nullConditionFilters[i].table = &virtualTable;
        filter.filters.push_back(&nullConditionFilters[i]);
    }
    for (int i = 0; i < valueConditionFilters.size(); i++) {
        valueConditionFilters[i].table = &virtualTable;
        filter.filters.push_back(&valueConditionFilters[i]);
    }
    for (int i = 0; i < columnConditionFilters.size(); i++) {
        columnConditionFilters[i].table = &virtualTable;
        filter.filters.push_back(&columnConditionFilters[i]);
    }
    // The select filters comes after.
    if (selectFilter.selectors.size() > 0) {
        selectFilter.table = &virtualTable;
        filter.filters.push_back(&selectFilter);
    }

    // Only apply limit and offset filters when is not aggregated.
    if (!selectFilter.isAggregated) {
        // Only apply limit and offset filters when is not aggregated.
        filter.filters.push_back(&offsetFilter);
        filter.filters.push_back(&limitFilter);
    }

    filter.build();

    return filter;
}

}  // namespace Internal
}  // namespace SimpleDB