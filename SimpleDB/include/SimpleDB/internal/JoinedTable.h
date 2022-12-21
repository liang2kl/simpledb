#ifndef _SIMPLEDB_JOINED_TABLE_H
#define _SIMPLEDB_JOINED_TABLE_H

#include <memory>
#include <vector>

#include "internal/IndexedTable.h"
#include "internal/QueryDataSource.h"

namespace SimpleDB {
namespace Internal {

class JoinedTable : public QueryDataSource {
public:
    JoinedTable() = default;
    void append(std::shared_ptr<IndexedTable> table);
    void close();

    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;
    virtual bool acceptCondition(
        const CompareValueCondition &condition) override;

private:
    std::vector<std::shared_ptr<IndexedTable>> tables;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif