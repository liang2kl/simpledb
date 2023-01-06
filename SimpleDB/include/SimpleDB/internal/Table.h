#ifndef _SIMPLEDB_TABLE_H
#define _SIMPLEDB_TABLE_H

#include <stdint.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "internal/Column.h"
#include "internal/FileCoordinator.h"
#include "internal/Macros.h"
#include "internal/QueryDataSource.h"

namespace SimpleDB {
class DBMS;

namespace Internal {

struct ForeignKey {
    std::string name;
    std::string table;
    std::string ref;
    DataType type;  // The referencing column's type, for validation.
};

struct ColumnMeta {
    DataType type;
    ColumnSizeType size;
    bool nullable;
    char name[MAX_COLUMN_NAME_LEN];

    bool hasDefault;
    ColumnValue defaultValue;

    std::string typeDesc() const;
    std::string defaultValDesc() const;
};

// A Table holds the metadata of a certain table, which should be unique
// thourghout the program, and be stored in memory once created for the sake of
// metadata reading/writing performance.
class Table : public QueryDataSource {
    friend class QueryBuilder;
    friend class ::SimpleDB::DBMS;
    friend class IndexedTable;

public:
    // The metadata is not initialized in this constructor.
    Table() = default;
    ~Table();

    // Open the table from a file, which must be created by `create()` before.
    void open(const std::string &file) noexcept(false);

    // Create a new table in a file.
    void create(
        const std::string &file, const std::string &name,
        const std::vector<ColumnMeta> &columns,
        const std::string &primaryKey = {},
        const std::vector<ForeignKey> &foreignKeys = {}) noexcept(false);

    // Get record.
    [[nodiscard]] Columns get(RecordID id,
                              ColumnBitmap columnBitmap = COLUMN_BITMAP_ALL);
    void get(RecordID id, Columns &columns,
             ColumnBitmap columnBitmap = COLUMN_BITMAP_ALL);

    // Insert record, returns (page, slot) of the inserted record.
    RecordID insert(const Columns &values,
                    ColumnBitmap bitmap = COLUMN_BITMAP_ALL);

    // Update record.
    void update(RecordID id, const Columns &columns,
                ColumnBitmap columnBitmap = COLUMN_BITMAP_ALL);

    // Remove record.
    void remove(RecordID id);

    // Set primary key.
    void setPrimaryKey(const std::string &field);
    void dropPrimaryKey(const std::string &field);

    void close();

    int getColumnIndex(const char *name) const;
    std::string getColumnName(int index) const;

    // QueryDataSource requirements.
    virtual void iterate(IterateCallback callback) override;
    virtual std::vector<ColumnInfo> getColumnInfo() override;

#if !TESTING
private:
#endif
    struct TableMeta {
        // Keep first.
        uint16_t headCanary = TABLE_META_CANARY;

        char name[MAX_TABLE_NAME_LEN + 1];

        uint32_t numColumn;
        ColumnMeta columns[MAX_COLUMNS];
        int primaryKeyIndex;
        ForeignKey foreignKeys[MAX_FOREIGN_KEYS];
        uint16_t numUsedPages;
        uint16_t firstFree;
        int recordSize;

        // Keep last.
        uint16_t tailCanary = TABLE_META_CANARY;
    };

    struct PageMeta {
        // Keep first.
        uint16_t headCanary = PAGE_META_CANARY;

        using BitmapType = int64_t;
        BitmapType occupied = 0;

        static_assert(sizeof(BitmapType) * 8 >= MAX_SLOT_PER_PAGE);

        uint16_t nextFree;

        // Keep last.
        uint16_t tailCanary = PAGE_META_CANARY;
    };

    // The metadata should be fit into the first slot.
    static_assert(sizeof(TableMeta) < PAGE_SIZE);
    static_assert(sizeof(PageMeta) < PAGE_SIZE);

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

    void deserialize(const char *srcData, Columns &destObjects,
                     ColumnBitmap columnBitmap);
    void serialize(const Columns &srcObjects, char *destData, ColumnBitmap map,
                   bool all);

    // Must ensure that the handle is valid.
    bool occupied(const PageHandle &handle, int slot);

    // Side effect: might create a new page, thus modifying meta,
    // firstFreePagMeta, etc.
    RecordID getEmptySlot();

    int slotSize();
    int numSlotPerPage();

    bool isPageFull(PageMeta *pageMeta);

    void validateSlot(int page, int slot);
    void validateColumnBitmap(const Columns &columns, ColumnBitmap bitmap,
                              bool isUpdate);
};

}  // namespace Internal
}  // namespace SimpleDB
#endif