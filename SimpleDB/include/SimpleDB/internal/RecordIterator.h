#ifndef _SIMPLEDB_RECORD_ITERATOR_H
#define _SIMPLEDB_RECORD_ITERATOR_H

#include <utility>
#include <vector>

namespace SimpleDB {
namespace Internal {

// Forward declaration.
class Table;
struct Column;
struct RecordID;
using Columns = Column *;

enum CompareOp {
    EQ,        // =
    NE,        // <>
    LT,        // <
    LE,        // <=
    GT,        // >
    GE,        // >=
    IS_NULL,   // IS NULL
    NOT_NULL,  // NOT NULL
    LIKE,      // LIKE
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
    using GetNextRecordFunc = std::function<RecordID(void)>;

    // Iterate through all records of the table.
    int iterate(Columns bufColumns, CompareConditions conditions,
                IteratorFunc callback);
    int iterate(Columns bufColumns, CompareConditions conditions,
                GetNextRecordFunc getNext, IteratorFunc callback);

#if TESTING
    RecordIterator() = default;
#endif

private:
    Table *table;

    bool validate(const Columns columns, const CompareConditions &conditions);
};

}  // namespace Internal
}  // namespace SimpleDB

#endif