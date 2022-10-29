#ifndef _SIMPLEDB_RECORD_ITERATOR_H
#define _SIMPLEDB_RECORD_ITERATOR_H

#include <utility>
#include <vector>

namespace SimpleDB {

// Forward declaration.
class Table;
struct Column;
using Columns = Column *;
using RecordSlot = std::pair<int, int>;

enum CompareOp {
    EQ,  // =
    NE,  // <>
    LT,  // <
    LE,  // <=
    GT,  // >
    GE,  // >=
};

struct CompareCondition {
    const char *columnName;
    CompareOp op;
    const char *value;

    CompareCondition(const char *columnName, CompareOp op, const char *value)
        : columnName(columnName), op(op), value(value) {}
    CompareCondition() = default;
};

using CompareConditions = std::vector<CompareCondition>;

class RecordIterator {
public:
    RecordIterator(Table *table);
    using IteratorFunc = std::function<void(int index)>;
    using GetNextSlotFunc = std::function<RecordSlot(void)>;

    // Iterate through all records of the table.
    int iterate(Columns bufColumns, CompareConditions conditions,
                IteratorFunc callback);
    int iterate(Columns bufColumns, CompareConditions conditions,
                GetNextSlotFunc getNext, IteratorFunc callback);

#ifdef TESTING
    RecordIterator() = default;
#endif

private:
    Table *table;

    bool validate(const Columns columns, const CompareConditions &conditions);
};

}  // namespace SimpleDB

#endif