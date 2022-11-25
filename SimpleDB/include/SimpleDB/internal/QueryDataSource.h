#ifndef _SIMPLEDB_QUERY_DATASOURCE_H
#define _SIMPLEDB_QUERY_DATASOURCE_H

#include <functional>
#include <vector>

#include "internal/Column.h"

namespace SimpleDB {
namespace Internal {

class QueryDataSource {
public:
    using IterateCallback = std::function<bool(RecordID, Columns &columns)>;
    virtual ~QueryDataSource() = default;
    virtual void iterate(IterateCallback callback) = 0;
    virtual std::vector<ColumnInfo> getColumnInfo() = 0;
    virtual std::vector<struct CompareValueCondition> acceptConditions(
        const std::vector<struct CompareValueCondition> &conditions) {
            return conditions;
        }
};

}  // namespace Internal
}  // namespace SimpleDB

#endif