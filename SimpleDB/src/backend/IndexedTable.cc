#include "internal/IndexedTable.h"

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdint>
#include <vector>

namespace SimpleDB {
namespace Internal {

IndexedTable::IndexedTable(Table *table, GetIndexFunc getIndex)
    : table(table), getIndex(getIndex) {}

void IndexedTable::iterate(IterateCallback callback) {
    collapseRanges();

    if (emptySet) {
        return;
    }

    if (index == nullptr) {
        return table->iterate(callback);
    }

    Columns columns;

    for (auto &range : ranges) {
        index->iterateRange(range, [&](RecordID id) {
            table->get(id, columns);
            return callback(id, columns);
        });
    }
}

bool IndexedTable::acceptCondition(const CompareValueCondition &condition) {
    if (!condition.columnId.tableName.empty() &&
        condition.columnId.tableName != table->meta.name) {
        return false;
    }
    int columnIndex =
        table->getColumnIndex(condition.columnId.columnName.c_str());
    if (columnIndex < 0) {
        return false;
    }
    const ColumnMeta &column = table->meta.columns[columnIndex];

    if (column.type != INT) {
        return false;
    }

    if (index == nullptr) {
        index = getIndex(table->meta.name, condition.columnId.columnName);
        columnName = condition.columnId.columnName;

        if (index == nullptr) {
            return false;
        }
    } else if (columnName != condition.columnId.columnName) {
        // Only taking the first (indexed) column from the conditions.
        return false;
    }

    ranges.push_back(makeRange(condition));

    return true;
}

std::vector<ColumnInfo> IndexedTable::getColumnInfo() {
    return table->getColumnInfo();
}

Table *IndexedTable::getTable() { return table; }

Index::Range IndexedTable::makeRange(const CompareValueCondition &condition) {
    int value = condition.value.intValue;

    switch (condition.op) {
        case EQ:
            return {value, value};
        case NE:
            return {value + 1, value - 1};
        case LT:
            return {INT_MIN, value - 1};
        case LE:
            return {INT_MIN, value};
        case GT:
            return {value + 1, INT_MAX};
        case GE:
            return {value, INT_MAX};
    }
}

void IndexedTable::collapseRanges() {
    Index::Range collapsedRange = {INT_MIN, INT_MAX};
    std::vector<int> neValues;

    for (const auto &range : ranges) {
        if (range.first > range.second) {
            assert(range.first == range.second + 2);
            neValues.push_back(range.first - 1);
            continue;
        }

        collapsedRange.first = std::max(collapsedRange.first, range.first);
        collapsedRange.second = std::min(collapsedRange.second, range.second);

        if (collapsedRange.first > collapsedRange.second) {
            emptySet = true;
            return;
        }
    }

    ranges.clear();

    std::sort(neValues.begin(), neValues.end());

    for (auto neVal : neValues) {
        if (neVal < collapsedRange.first || neVal > collapsedRange.second) {
            continue;
        }

        if (neVal == collapsedRange.first) {
            collapsedRange.first++;
        } else if (neVal == collapsedRange.second) {
            collapsedRange.second--;
        } else {
            ranges.push_back({collapsedRange.first, neVal - 1});
            collapsedRange.first = neVal + 1;
        }

        if (collapsedRange.first > collapsedRange.second) {
            emptySet = true;
            return;
        }
    }

    ranges.push_back(collapsedRange);
}

}  // namespace Internal
}  // namespace SimpleDB