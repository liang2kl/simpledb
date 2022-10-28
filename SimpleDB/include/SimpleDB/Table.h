#ifndef _SIMPLEDB_TABLE_H
#define _SIMPLEDB_TABLE_H

#include <stdint.h>

#include <map>
#include <utility>

#include "Macros.h"
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
    char data[MAX_COLUMN_SIZE];

    int deserializeInt();
    float deserializeFloat();
    // The size of `dest` should be at least `size + 1` to accommodate '\0'.
    void deserializeVarchar(char *dest);

    // Convenienve constructors.
    Column(int data);
    Column(float data);
    Column(const char *data, int maxLength);

    Column() = default;
};

struct ColumnMeta {
    DataType type;
    ColumnSizeType size;
    char name[MAX_COLUMN_NAME_LEN];
};

using Columns = Column *;

// A Table holds the metadata of a certain table, which should be unique
// thourghout the program, and be stored in memory once created for the sake of
// metadata reading/writing performance.
class Table {
public:
    // The metadata is not initialized in this constructor.
    Table();
    ~Table();

    void open(const std::string &file) noexcept(false);
    void create(const std::string &file, int numColumn,
                ColumnMeta *columns) noexcept(false);

    void get(int page, int slot, Columns columns);
    std::pair<int, int> insert(Columns columns);
    void update(int page, int slot, Columns columns);
    void remove(int page, int slot);

    void close();

    int getColumn(const char *name);

#ifndef TESTING
private:
#endif
    struct TableMeta {
        // Keep first.
        uint64_t headCanary = TABLE_META_CANARY;

        uint8_t numColumn;
        ColumnMeta columns[MAX_COLUMNS];

        using BitmapType = int8_t;
        BitmapType occupied[MAX_PAGE_PER_TABLE];

        static_assert(sizeof(BitmapType) * 8 >= NUM_SLOT_PER_PAGE);

        // Keep last.
        uint64_t tailCanary = TABLE_META_CANARY;
    };

    // The metadata of a table should be fit into the first page.
    static_assert(sizeof(TableMeta) < PAGE_SIZE);

    bool initialized = false;
    FileDescriptor fd;
    TableMeta meta;
    PageHandle *handles[MAX_PAGE_PER_TABLE];
    std::map<std::string, int> columnNameMap;

    void checkInit() noexcept(false);
    void flushMeta() noexcept(false);

    PageHandle *getHandle(int page);

    void deserialize(const char *srcData, Columns destObjects);
    void serialize(const Columns srcObjects, char *destData);

    bool occupied(int page, int slot);
    std::pair<int, int> getEmptySlot();

    void validateSlot(int page, int slot);
};

}  // namespace SimpleDB
#endif