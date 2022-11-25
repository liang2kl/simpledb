#ifndef _SIMPLEDB_INDEXED_TABLE_H
#define _SIMPLEDB_INDEXED_TABLE_H

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
    using GetIndexFunc = std::function<Index *(const std::string &table,
                                               const std::string &column)>;
    IndexedTable(Table *table, GetIndexFunc getIndex);

    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;
    virtual std::vector<CompareValueCondition> acceptConditions(
        const std::vector<CompareValueCondition> &conditions) override;

#if !TESTING
private:
#endif
    Table *table;
    GetIndexFunc getIndex;
    Index *index = nullptr;
    std::string columnName;
    std::vector<Index::Range> ranges;
    bool emptySet = false;

    Index::Range makeRange(const CompareValueCondition &condition);
    void collapseRanges();
};

}  // namespace Internal
}  // namespace SimpleDB

#endif