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
    virtual std::vector<ColumnInfo> getColumnMeta() = 0;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif