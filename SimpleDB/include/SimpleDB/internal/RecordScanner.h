#ifndef _SIMPLEDB_RECORD_SCANNER_H
#define _SIMPLEDB_RECORD_SCANNER_H

#include <utility>
#include <vector>

namespace SimpleDB {
namespace Internal {

// Forward declaration.
class Table;
struct Column;
struct RecordID;
using Columns = std::vector<Column>;

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

class RecordScanner {
public:
    RecordScanner(Table *table);
    using IteratorFunc =
        std::function<bool(int index, RecordID id, const Columns bufColumns)>;
    using GetNextRecordFunc = std::function<RecordID(void)>;

    // Iterate through all records of the table.
    int iterate(CompareConditions conditions, IteratorFunc callback);
    int iterate(CompareConditions conditions, GetNextRecordFunc getNext,
                IteratorFunc callback);

    std::pair<RecordID, Columns> findFirst(CompareConditions conditions);

#if TESTING
    RecordScanner() = default;
#endif

private:
    Table *table;

    bool validate(const Columns &columns, const CompareConditions &conditions);
};

}  // namespace Internal
}  // namespace SimpleDB

#endif