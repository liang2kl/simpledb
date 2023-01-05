#ifndef _SIMPLEDB_INDEXED_TABLE_H
#define _SIMPLEDB_INDEXED_TABLE_H

#include <memory>
#include <string>
#include <vector>

#include "internal/Index.h"
#include "internal/QueryDataSource.h"
#include "internal/QueryFilter.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

class IndexedTable : public QueryDataSource {
public:
    using GetIndexFunc = std::function<std::shared_ptr<Index>(
        const std::string &table, const std::string &column)>;
    IndexedTable(Table *table, GetIndexFunc getIndex);

    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;
    virtual bool acceptCondition(
        const CompareValueCondition &condition) override;

    Table *getTable();

#if !TESTING
private:
#endif
    Table *table;
    GetIndexFunc getIndex;
    std::shared_ptr<Index> index;
    std::string columnName;
    std::vector<Index::Range> ranges;
    bool emptySet = false;

    Index::Range makeRange(const CompareValueCondition &condition);
    void collapseRanges();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif