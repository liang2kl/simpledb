#ifndef _SIMPLEDB_TABLE_H
#define _SIMPLEDB_TABLE_H

#include <stdint.h>

#include "Macros.h"
#include "internal/FileCoordinator.h"

namespace SimpleDB {

using ColumnSizeType = uint32_t;

enum DataType {
    INT,     // 32 bit integer
    FLOAT,   // 32 bit float
    VARCHAR  // length-variable string
};

struct ColumnMeta {
    DataType type;
    ColumnSizeType size;
};

struct TableMeta {
    uint8_t numColumn;
    ColumnMeta columns[MAX_COLUMNS];
};

// The metadata of a table should be fit into the first slot.
static_assert(sizeof(TableMeta) < RECORD_SLOT_SIZE);

// A Table holds the metadata of a certain table, which should be unique
// thourghout the program, and be stored in memory once created for the sake of
// metadata reading/writing performance.
class Table {
public:
    // The metadata is not initialized in this constructor.
    Table() = default;
    ~Table();

#ifndef TESTING
private:
#endif
    bool initialized = false;
    FileDescriptor fd;
    TableMeta meta;

    void init(const std::string &file) noexcept(false);
    void flushMeta() noexcept(false);
};

}  // namespace SimpleDB
#endif