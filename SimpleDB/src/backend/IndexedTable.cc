#include "internal/IndexedTable.h"

#include <sys/_types/_int64_t.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <vector>

namespace SimpleDB {
namespace Internal {

IndexedTable::IndexedTable(Table *table, GetIndexFunc getIndex)
    : table(table), getIndex(getIndex) {}

void IndexedTable::iterate(IterateCallback callback) {
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

std::vector<CompareValueCondition> IndexedTable::acceptConditions(
    const std::vector<CompareValueCondition> &conditions) {
    std::vector<CompareValueCondition> untakenConditions;

    for (const auto &condition : conditions) {
        int columnIndex = table->getColumnIndex(condition.columnName.c_str());
        assert(columnIndex != -1);
        const ColumnMeta &column = table->meta.columns[columnIndex];

        if (column.type != INT) {
            untakenConditions.push_back(condition);
            continue;
        }

        // Only taking the first (indexed) column from the conditions.
        if (index != nullptr && columnName != condition.columnName) {
            untakenConditions.push_back(condition);
            continue;
        }

        index = getIndex(table->meta.name, condition.columnName);
        if (index == nullptr) {
            untakenConditions.push_back(condition);
            continue;
        }

        ranges.push_back(makeRange(condition));
    }

    collapseRanges();

    return untakenConditions;
}

std::vector<ColumnInfo> IndexedTable::getColumnInfo() {
    return table->getColumnInfo();
}

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
        }

        collapsedRange.first = std::max(collapsedRange.first, range.first);
        collapsedRange.second = std::min(collapsedRange.second, range.second);

        if (collapsedRange.first > collapsedRange.second) {
            emptySet = true;
            return;
        }
    }

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