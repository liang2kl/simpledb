#include "internal/JoinedTable.h"

#include <cassert>
#include <memory>
#include <vector>

namespace SimpleDB {
namespace Internal {

void JoinedTable::append(std::shared_ptr<IndexedTable> table) {
    assert(tables.size() < 2);
    tables.push_back(table);
}

bool JoinedTable::acceptCondition(const CompareValueCondition &condition) {
    for (auto table : tables) {
        if (table->acceptCondition(condition)) {
            return true;
        }
    }
    return false;
}

std::vector<ColumnInfo> JoinedTable::getColumnInfo() {
    std::vector<ColumnInfo> result;
    for (auto table : tables) {
        auto columns = table->getColumnInfo();
        result.insert(result.end(), columns.begin(), columns.end());
    }
    return result;
}

void JoinedTable::iterate(IterateCallback callback) {
    // Only support <= 2 tables.
    if (tables.size() == 1) {
        tables[0]->iterate(callback);
    } else if (tables.size() == 2) {
        // A simple nested, pipelined loop join.
        // TODO: Decide join order based on table sizes, indexes...
        tables[0]->iterate([&](RecordID id, const Columns &columns1) {
            bool continue_ = true;
            tables[1]->iterate([&](RecordID id, const Columns &columns2) {
                Columns columns;
                columns.insert(columns.end(), columns1.begin(), columns1.end());
                columns.insert(columns.end(), columns2.begin(), columns2.end());
                continue_ = callback(id, columns);
                return continue_;
            });
            return continue_;
        });
    }
}

}  // namespace Internal
}  // namespace SimpleDB