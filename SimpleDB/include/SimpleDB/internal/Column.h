#ifndef _SIMPLEDB_COLUMN_H
#define _SIMPLEDB_COLUMN_H

#include <string>
#include <vector>

#include "internal/Macros.h"

namespace SimpleDB {
namespace Internal {

using ColumnSizeType = uint32_t;

enum DataType {
    INT,     // 32 bit integer
    FLOAT,   // 32 bit float
    VARCHAR  // length-variable string
};

union ColumnValue {
    int intValue;
    float floatValue;
    char stringValue[MAX_COLUMN_SIZE];
};

struct Column {
    DataType type;
    ColumnSizeType size;
    bool isNull = false;
    ColumnValue data;

    // Initialize a null column.
    static Column nullColumn(DataType type, ColumnSizeType size);
    static Column nullIntColumn();
    static Column nullFloatColumn();
    static Column nullVarcharColumn(ColumnSizeType size);

    // Convenienve constructors.
    Column(int data);
    Column(float data);
    Column(const char *data, int maxLength);

    Column() = default;
};

using Columns = std::vector<Column>;
using ColumnBitmap = int16_t;

static_assert(sizeof(ColumnBitmap) == sizeof(COLUMN_BITMAP_ALL));

struct ColumnInfo {
    std::string tableName;
    std::string columnName;
    DataType type;

    bool operator==(const ColumnInfo &rhs) {
        return tableName == rhs.tableName && columnName == rhs.columnName;
    }

    std::string getDesc() const {
        return tableName.empty() ? columnName : tableName + "." + columnName;
    }
};

struct RecordID {
    int page;
    int slot;

    bool operator==(const RecordID &rhs) const;
    bool operator!=(const RecordID &rhs) const;
    bool operator>(const RecordID &rhs) const;
    static const RecordID NULL_RECORD;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif