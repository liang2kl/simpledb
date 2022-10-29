#ifndef _SIMPLEDB_TABLE_H
#define _SIMPLEDB_TABLE_H

#include <stdint.h>

#include <map>
#include <utility>

#include "Macros.h"
#include "RecordIterator.h"
#include "internal/FileCoordinator.h"

namespace SimpleDB {

using ColumnSizeType = uint32_t;

enum DataType {
    INT,     // 32 bit integer
    FLOAT,   // 32 bit float
    VARCHAR  // length-variable string
};

struct Column {
    DataType type;
    ColumnSizeType size;
    bool isNull = false;
    char data[MAX_COLUMN_SIZE];

    int deserializeInt();
    float deserializeFloat();
    void deserializeVarchar(char *dest);

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

struct ColumnMeta {
    DataType type;
    ColumnSizeType size;
    bool nullable;
    char name[MAX_COLUMN_NAME_LEN];

    bool hasDefault;
    char defaultValue[MAX_COLUMN_SIZE];
};

using Columns = Column *;
using ColumnBitmap = int16_t;

static_assert(sizeof(ColumnBitmap) == sizeof(COLUMN_BITMAP_ALL));

struct RecordID {
    int page;
    int slot;
};

// A Table holds the metadata of a certain table, which should be unique
// thourghout the program, and be stored in memory once created for the sake of
// metadata reading/writing performance.
class Table {
    friend class RecordIterator;

public:
    // The metadata is not initialized in this constructor.
    Table() = default;
    ~Table();

    // Open the table from a file, which must be created by `create()` before.
    void open(const std::string &file) noexcept(false);

    // Create a new table in a file.
    void create(const std::string &file, const std::string &name, int numColumn,
                ColumnMeta *columns) noexcept(false);

    // Get record.
    void get(RecordID id, Columns columns,
             ColumnBitmap columnBitmap = COLUMN_BITMAP_ALL);

    // Insert record, returns (page, slot) of the inserted record.
    RecordID insert(Columns columns);

    // Update record.
    void update(RecordID id, Columns columns,
                ColumnBitmap columnBitmap = COLUMN_BITMAP_ALL);

    // Remove record.
    void remove(RecordID id);

    void close();

    int getColumnIndex(const char *name);
    void getColumnName(int index, char *name);

    RecordIterator getIterator() { return RecordIterator(this); }

#ifndef TESTING
private:
#endif
    struct TableMeta {
        // Keep first.
        uint16_t headCanary = TABLE_META_CANARY;

        char name[MAX_TABLE_NAME_LEN + 1];

        uint32_t numColumn;
        ColumnMeta columns[MAX_COLUMNS];
        uint16_t numUsedPages;
        uint16_t firstFree;

        // Keep last.
        uint16_t tailCanary = TABLE_META_CANARY;
    };

    struct PageMeta {
        // Keep first.
        uint16_t headCanary = PAGE_META_CANARY;

        using BitmapType = int16_t;
        BitmapType occupied = 0;

        static_assert(sizeof(BitmapType) * 8 >= NUM_SLOT_PER_PAGE);
        static_assert(sizeof(BitmapType) == sizeof(SLOT_FULL_MASK));

        uint16_t nextFree;

        // Keep last.
        uint16_t tailCanary = PAGE_META_CANARY;
    };

    // The metadata should be fit into the first slot.
    static_assert(sizeof(TableMeta) < PAGE_SIZE);
    static_assert(sizeof(PageMeta) < RECORD_SLOT_SIZE);

    struct RecordMeta {
        ColumnBitmap nullBitmap;
    };

    bool initialized = false;
    FileDescriptor fd;
    // TODO: Pin meta page?
    TableMeta meta;
    std::map<int, PageHandle *> pageHandleMap;
    std::map<std::string, int> columnNameMap;

    void checkInit() noexcept(false);
    void flushMeta() noexcept(false);
    void flushPageMeta(int page, const PageMeta &meta);

    PageHandle *getHandle(int page);

    void deserialize(const char *srcData, Columns destObjects,
                     ColumnBitmap columnBitmap);
    void serialize(const Columns srcObjects, char *destData, ColumnBitmap map);

    // Must ensure that the handle is valid.
    bool occupied(const PageHandle &handle, int slot);

    // Side effect: might create a new page, thus modifying meta,
    // firstFreePagMeta, etc.
    RecordID getEmptySlot();

    void validateSlot(int page, int slot);
};

}  // namespace SimpleDB
#endif