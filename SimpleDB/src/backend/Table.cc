#include "internal/Table.h"

#include <SimpleDB/Error.h>
#include <string.h>

#include <cmath>
#include <cstdio>
#include <cstring>

#include "internal/Logger.h"
#include "internal/Macros.h"
#include "internal/PageFile.h"

namespace SimpleDB {
namespace Internal {

const RecordID RecordID::NULL_RECORD = {-1, -1};
bool RecordID::operator==(const RecordID &rhs) const {
    return page == rhs.page && slot == rhs.slot;
}
bool RecordID::operator!=(const RecordID &rhs) const { return !(*this == rhs); }

Table::~Table() { close(); }

void Table::open(const std::string &file) {
    Logger::log(VERBOSE, "Table: initializing table from %s\n", file.c_str());

    if (initialized) {
        Logger::log(WARNING,
                    "Table: table is already initailized, but attempting to "
                    "re-initialize from %s\n",
                    file.c_str());
    }

    // Initialize table metadata from the file.
    try {
        // Open the file.
        fd = PF::open(file);
        // The metadata is written in the first page.
        PageHandle handle = PF::getHandle(fd, 0);
        meta = *PF::loadRaw<TableMeta *>(handle);
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to read table metadata from file %d\n",
                    fd.value);
        throw Internal::ReadTableError();
    }

    // Check the vadality of the meta.
    if (meta.headCanary != TABLE_META_CANARY ||
        meta.tailCanary != TABLE_META_CANARY) {
        Logger::log(ERROR,
                    "Table: fail to read table metadata from file %d: "
                    "invalid canary values\n",
                    fd.value);
        throw Internal::ReadTableError();
    }

    // Initialize name mapping.
    for (int i = 0; i < meta.numColumn; i++) {
        columnNameMap[meta.columns[i].name] = i;
    }

    initialized = true;
}

void Table::create(const std::string &file, const std::string &name,
                   int numColumn, ColumnMeta *columns) {
    Logger::log(VERBOSE, "Table: initializing empty table to %s\n",
                file.c_str());

    if (initialized) {
        Logger::log(WARNING,
                    "Table: table is already initailized, but attempting to "
                    "re-initialize an empty table to %s\n",
                    file.c_str());
    }

    if (numColumn > MAX_COLUMNS) {
        Logger::log(
            ERROR,
            "Table: fail to create table: too many columns: %d, max %d\n",
            numColumn, MAX_COLUMNS);
        throw Internal::TooManyColumnsError();
    }

    try {
        // Create and open the file.
        PF::create(file);
        fd = PF::open(file);
    } catch (Internal::FileExistsError) {
        Logger::log(
            WARNING,
            "Table: creating an empty table to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Internal::CreateTableError();
    }

    if (name.size() > MAX_TABLE_NAME_LEN) {
        Logger::log(
            ERROR,
            "Table: fail to create table to %s: table name %s too long\n",
            file.c_str(), name.c_str());
        throw Internal::CreateTableError();
    }

    meta.firstFree = 1;
    meta.numUsedPages = 1;
    meta.numColumn = numColumn;
    strcpy(meta.name, name.c_str());

    int totalSize = 0;
    for (int i = 0; i < numColumn; i++) {
        // Validate column meta.
        bool sizeValid = columns[i].type == VARCHAR
                             ? columns[i].size <= MAX_VARCHAR_LEN
                             : columns[i].size == 4;

        if (!sizeValid) {
            Logger::log(ERROR,
                        "Table: insert failed: column %d has size %d, which is "
                        "larger than "
                        "maximum size %d\n",
                        i, columns[i].size, MAX_COLUMN_SIZE);
            throw Internal::InvalidColumnSizeError();
        }

        if (columnNameMap.find(columns[i].name) != columnNameMap.end()) {
            Logger::log(ERROR,
                        "Table: insert failed: duplicate column name %s\n",
                        columns[i].name);
            throw Internal::DuplicateColumnNameError();
        }

        meta.columns[i] = columns[i];
        columnNameMap[columns[i].name] = i;

        totalSize += columns[i].size;
    }

    if (totalSize > RECORD_SLOT_SIZE - sizeof(RecordMeta)) {
        Logger::log(ERROR,
                    "Table: total size of columns is %d, which is larger than "
                    "maximum size %d\n",
                    totalSize, RECORD_SLOT_SIZE - int(sizeof(RecordMeta)));
        throw Internal::InvalidColumnSizeError();
    }

    initialized = true;
}

void Table::flushMeta() {
    PageHandle handle = PF::getHandle(fd, 0);
    memcpy(PF::loadRaw(handle), &meta, sizeof(TableMeta));
    PF::markDirty(handle);
}

void Table::flushPageMeta(int page, const PageMeta &meta) {
    PageHandle handle = PF::getHandle(fd, page);
    memcpy(PF::loadRaw(handle), &meta, sizeof(PageMeta));
    PF::markDirty(handle);
}

void Table::get(RecordID id, Columns &columns, ColumnBitmap columnBitmap) {
    Logger::log(VERBOSE, "Table: get record from page %d slot %d\n", id.page,
                id.slot);

    checkInit();
    validateSlot(id.page, id.slot);

    PageHandle *handle = getHandle(id.page);

    if (!occupied(*handle, id.slot)) {
        Logger::log(
            ERROR,
            "Table: fail to get record: page %d slot %d is not occupied\n",
            id.page, id.slot);
        throw Internal::InvalidSlotError();
    }

    char *start = PF::loadRaw(*handle) + id.slot * RECORD_SLOT_SIZE;
    deserialize(start, columns, columnBitmap);
}

Columns Table::get(RecordID id, ColumnBitmap columnBitmap) {
    Columns columns;
    get(id, columns, columnBitmap);
    return columns;
}

RecordID Table::insert(const Columns &columns, ColumnBitmap bitmap) {
    checkInit();

    // Find an empty slot.
    auto id = getEmptySlot();

    Logger::log(VERBOSE, "Table: insert record to page %d slot %d\n", id.page,
                id.slot);

    // Validate the bitmap.
    validateColumnBitmap(columns, bitmap, /*isUpdate=*/false);

    PageHandle *handle = getHandle(id.page);
    char *start = PF::loadRaw(*handle) + id.slot * RECORD_SLOT_SIZE;

    serialize(columns, start, bitmap, /*all=*/true);

    // Mark the page as dirty.
    PF::markDirty(*handle);

    // We don't need to flush meta here.

    return id;
}

void Table::update(RecordID id, const Columns &columns, ColumnBitmap bitmap) {
    Logger::log(VERBOSE, "Table: updating record from page %d slot %d\n",
                id.page, id.slot);

    checkInit();
    validateSlot(id.page, id.slot);

    PageHandle *handle = getHandle(id.page);

    if (!occupied(*handle, id.slot)) {
        Logger::log(
            ERROR,
            "Table: fail to update record: page %d slot %d is not occupied\n",
            id.page, id.slot);
        throw Internal::InvalidSlotError();
    }

    // Validate the bitmap.
    validateColumnBitmap(columns, bitmap, /*isUpdate=*/true);

    char *start = PF::loadRaw(*handle) + id.slot * RECORD_SLOT_SIZE;
    serialize(columns, start, bitmap, /*all=*/false);

    // Mark dirty.
    PF::markDirty(*handle);
}

void Table::remove(RecordID id) {
    Logger::log(VERBOSE, "Table: removing record from page %d slot %d\n",
                id.page, id.slot);

    checkInit();
    validateSlot(id.page, id.slot);

    PageHandle *handle = getHandle(id.page);

    if (!occupied(*handle, id.slot)) {
        Logger::log(
            ERROR,
            "Table: fail to remove record: page %d slot %d is not occupied\n",
            id.page, id.slot);
        throw Internal::InvalidSlotError();
    }

    PageMeta *pageMeta = PF::loadRaw<PageMeta *>(*handle);

    // Check previous occupation.
    if (pageMeta->occupied == SLOT_FULL_MASK) {
        // The page now will have an empty slot. Add it to the free list.
        pageMeta->nextFree = meta.firstFree;
        meta.firstFree = id.page;
    }

    // Mark the slot as unoccupied.
    pageMeta->occupied &= ~(1L << id.slot);

    if ((pageMeta->occupied & SLOT_OCCUPY_MASK) == 0) {
        // TODO: If the page is empty, we can free it.
    }

    // As we are dealing with the pointer directly, we don't need to flush.
    PF::markDirty(*handle);
    assert(handle->validate());
}

void Table::close() {
    if (!initialized) {
        return;
    }
    Logger::log(VERBOSE, "Table: closing table\n");

    flushMeta();

    PF::close(fd);
    for (auto &iter : pageHandleMap) {
        if (iter.second != nullptr) {
            delete iter.second;
        }
    }

    initialized = false;
    columnNameMap.clear();
    pageHandleMap.clear();
}

int Table::getColumnIndex(const char *name) {
    auto iter = columnNameMap.find(name);
    return iter == columnNameMap.end() ? -1 : iter->second;
}

void Table::getColumnName(int index, char *name) {
    if (index < 0 || index >= meta.numColumn) {
        Logger::log(ERROR, "Table: column index %d out of range [0, %d)\n",
                    index, meta.numColumn);
        throw Internal::InvalidColumnIndexError();
    }
    strcpy(name, meta.columns[index].name);
}

PageHandle *Table::getHandle(int page) {
    auto iter = pageHandleMap.find(page);
    if (iter != pageHandleMap.end()) {
        if (iter->second->validate()) {
            return iter->second;
        } else {
            delete iter->second;
            // Fall through to create a new handle.
        }
    }

    PageHandle handle = PF::getHandle(fd, page);
    // Create a handle in the heap to allow updating in PF::read().
    PageHandle *newHandle = new PageHandle(handle);
    pageHandleMap[page] = newHandle;
    return newHandle;
}

void Table::checkInit() {
    if (!initialized) {
        Logger::log(ERROR, "Table: not initialized yet\n");
        throw Internal::TableNotInitializedError();
    }
}

void Table::deserialize(const char *srcData, Columns &destObjects,
                        ColumnBitmap bitmap) {
    // First, fetch record meta.
    RecordMeta *recordMeta = (RecordMeta *)srcData;
    srcData += sizeof(RecordMeta);

    destObjects.resize(meta.numColumn);

    int index = 0;
    for (int i = 0; i < meta.numColumn; i++) {
        if ((bitmap & (ColumnBitmap(1) << i)) == 0) {
            srcData += meta.columns[i].size;
            continue;
        }

        Column &column = destObjects[index];
        column.size = meta.columns[i].size;
        column.type = meta.columns[i].type;

        if (recordMeta->nullBitmap & (1L << i)) {
            // The column is null.
#ifdef DEBUG
            if (!meta.columns[i].nullable) {
                Logger::log(ERROR,
                            "Table: internal error: column %s is not nullable, "
                            "but a null value is found\n",
                            meta.columns[i].name);
                throw Internal::NullValueFoundInNotNullColumnError();
            }
#endif
            column.isNull = true;
        } else {
            // We must copy the data here as we are directly using the buffer,
            // which can be invalidated.
            memcpy(column.data.stringValue, srcData, column.size);
            column.isNull = false;
            if (column.type == VARCHAR) {
                column.data.stringValue[column.size] = '\0';
            }
        }

        srcData += meta.columns[i].size;
        index++;
    };

    destObjects.resize(index);
#if DEBUG
    assert(destObjects.size() == index);
#endif
}

void Table::serialize(const Columns &srcObjects, char *destData,
                      ColumnBitmap bitmap, bool all) {
    RecordMeta *recordMeta = (RecordMeta *)destData;
    destData += sizeof(RecordMeta);

    // The actual index in `srcObjects`.
    int index = 0;
    for (int i = 0; i < meta.numColumn; i++) {
        if ((bitmap & (ColumnBitmap(1) << i)) == 0) {
            if (all) {
                if (!meta.columns[i].hasDefault) {
                    Logger::log(
                        ERROR,
                        "Table: internal error: column %s has "
                        "no default value but is required to serialize\n",
                        meta.columns[i].name);
                    throw Internal::ValueNotGivenError();
                }
                // Use default value
                memcpy(destData, meta.columns[i].defaultValue,
                       meta.columns[i].size);
            }
            destData += meta.columns[i].size;
            continue;
        }
        const Column &column = srcObjects[index];
        if (column.type != meta.columns[i].type) {
            Logger::log(ERROR,
                        "Table: column type mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].type, column.type);
            throw Internal::ColumnSerializationError();
        } else if (column.size != meta.columns[i].size) {
            Logger::log(ERROR,
                        "Table: column size mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].size, column.size);
            throw Internal::ColumnSerializationError();
        }

        if (column.isNull) {
            if (!meta.columns[i].nullable) {
                Logger::log(ERROR,
                            "Table: column %s is not nullable, but a null "
                            "value is given\n",
                            meta.columns[i].name);
                throw Internal::NullValueGivenForNotNullColumnError();
            }
            recordMeta->nullBitmap |= (1L << i);
        } else {
            recordMeta->nullBitmap &= ~(1L << i);
            memcpy(destData, column.data.stringValue, column.size);
            if (column.type == VARCHAR) {
                destData[column.size] = '\0';
            }
        }

        destData += meta.columns[i].size;
        index++;
    }
}

bool Table::occupied(const PageHandle &handle, int slot) {
    PageMeta *meta = PF::loadRaw<PageMeta *>(handle);
    return meta->occupied & (1L << slot);
}

void Table::validateSlot(int page, int slot) {
    // The first slot of each page (except 0) is for metadata.
    bool valid = page >= 1 && page < meta.numUsedPages && slot >= 1 &&
                 slot < NUM_SLOT_PER_PAGE;
    if (!valid) {
        Logger::log(
            ERROR,
            "Table: page/slot pair (%d, %d) is not valid, must be in range [0, "
            "%d) X [1, %d)\n",
            page, slot, meta.numUsedPages, NUM_SLOT_PER_PAGE);
        throw Internal::InvalidSlotError();
    }
}

void Table::validateColumnBitmap(const Columns &columns, ColumnBitmap bitmap,
                                 bool isUpdate) {
    int numColumns = 0;
    for (int i = 0; i < meta.numColumn; i++) {
        if ((bitmap & (1L << i)) == 0) {
            // The bit is not set, thus must having a default value.
            if (!isUpdate && !meta.columns[i].hasDefault) {
                Logger::log(ERROR,
                            "Table: column %d has no default value, but the "
                            "column is not set in the bitmap\n",
                            i);
                throw ValueNotGivenError();
            }
        } else {
            numColumns++;
        }
    }

    if (numColumns != columns.size()) {
        Logger::log(ERROR,
                    "Table: bitmap has %d columns, but %ld columns are given\n",
                    numColumns, columns.size());
        throw IncorrectColumnNumError();
    }
}

RecordID Table::getEmptySlot() {
    if (meta.numUsedPages == meta.firstFree) {
        // All pages are full, create a new page.
        Logger::log(VERBOSE,
                    "Table: all pages are full, creating a new page %d\n",
                    meta.firstFree);

        PageMeta pageMeta;
        pageMeta.nextFree = meta.firstFree + 1;
        pageMeta.occupied =
            0b11;  // The first slot (starting from 2) is now occupied.

        flushPageMeta(meta.firstFree, pageMeta);

        meta.numUsedPages++;
        // The firstFree of table remain unchanged.

        return {meta.firstFree, 1};
    } else {
        // One of the allocated pages has empty slots.
        Logger::log(VERBOSE, "Table: got first free page %d\n", meta.firstFree);

        int page = meta.firstFree;
        PageHandle *handle = getHandle(page);
        PageMeta *pageMeta = (PageMeta *)(PF::loadRaw(*handle));

        if (pageMeta->headCanary != PAGE_META_CANARY ||
            pageMeta->tailCanary != PAGE_META_CANARY) {
            Logger::log(ERROR,
                        "Table: page %d meta corrupted: head canary %d, tail "
                        "canary %d\n",
                        page, pageMeta->headCanary, pageMeta->tailCanary);
            throw Internal::InvalidPageMetaError();
        }

        int index = ffs(int(~pageMeta->occupied) & SLOT_OCCUPY_MASK);

        if (index == 0) {
            Logger::log(
                ERROR, "Table: page %d is full but not marked as full\n", page);
            throw Internal::InvalidPageMetaError();
        }

        index--;

        pageMeta->occupied |= (1L << index);

        if (pageMeta->occupied == SLOT_FULL_MASK) {
            // This page is full, modify meta.
            meta.firstFree = pageMeta->nextFree;
        }

        // Mark the page as dirty.
        assert(handle->validate());
        PF::markDirty(*handle);

        return {page, index};
    }
}

// ==== Column ====

Column Column::nullColumn(DataType type, ColumnSizeType size) {
    Column column;
    column.type = type;
    column.size = size;
    column.isNull = true;
    return column;
}

Column Column::nullIntColumn() { return nullColumn(INT, 4); }
Column Column::nullFloatColumn() { return nullColumn(FLOAT, 4); };
Column Column::nullVarcharColumn(ColumnSizeType size) {
    return nullColumn(VARCHAR, size);
};

Column::Column(int data) {
    size = 4;
    type = INT;
    this->data.intValue = data;
}

Column::Column(float data) {
    size = 4;
    type = FLOAT;
    this->data.floatValue = data;
}

Column::Column(const char *data, int maxLength) {
    if (maxLength > MAX_VARCHAR_LEN) {
        Logger::log(ERROR,
                    "Column: fail to create varchar column: length %d exceeds "
                    "maximum length %d\n",
                    maxLength, MAX_VARCHAR_LEN);
        throw Internal::InvalidColumnSizeError();
    }

    size = maxLength;
    type = VARCHAR;
    size_t copiedSize = std::min(strlen(data), size_t(maxLength));
    memcpy(this->data.stringValue, data, copiedSize);
    this->data.stringValue[copiedSize] = '\0';
}

}  // namespace Internal
}  // namespace SimpleDB
