#include "Table.h"

#include <string.h>

#include "Logger.h"
#include "Macros.h"
#include "PageFile.h"
#include "cmath"

namespace SimpleDB {

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
        meta = *(TableMeta *)PF::loadRaw(handle);
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to read table metadata from file %d\n",
                    fd.value);
        throw Error::ReadTableError();
    }

    // Check the vadality of the meta.
    if (meta.headCanary != TABLE_META_CANARY ||
        meta.tailCanary != TABLE_META_CANARY) {
        Logger::log(ERROR,
                    "Table: fail to read table metadata from file %d: "
                    "invalid canary values\n",
                    fd.value);
        throw Error::ReadTableError();
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
        throw Error::TooManyColumnsError();
    }

    try {
        // Create and open the file.
        PF::create(file);
        fd = PF::open(file);
    } catch (Error::FileExistsError) {
        Logger::log(
            WARNING,
            "Table: creating an empty table to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Error::CreateTableError();
    }

    if (name.size() > MAX_TABLE_NAME_LEN) {
        Logger::log(
            ERROR,
            "Table: fail to create table to %s: table name %s too long\n",
            file.c_str(), name.c_str());
        throw Error::CreateTableError();
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
            throw Error::InvalidColumnSizeError();
        }

        if (columnNameMap.find(columns[i].name) != columnNameMap.end()) {
            Logger::log(ERROR,
                        "Table: insert failed: duplicate column name %s\n",
                        columns[i].name);
            throw Error::DuplicateColumnNameError();
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
        throw Error::InvalidColumnSizeError();
    }

    flushMeta();
    initialized = true;
}

void Table::flushMeta() {
    PageHandle handle = PF::getHandle(fd, 0);
    memcpy(PF::loadRaw(handle), &meta, sizeof(TableMeta));
    PF::modify(handle);
}

void Table::flushPageMeta(int page, const PageMeta &meta) {
    PageHandle handle = PF::getHandle(fd, page);
    memcpy(PF::loadRaw(handle), &meta, sizeof(PageMeta));
    PF::modify(handle);
}

void Table::get(int page, int slot, Columns columns,
                ColumnBitmap columnBitmap) {
    Logger::log(VERBOSE, "Table: get record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    PageHandle *handle = getHandle(page);

    if (!occupied(*handle, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to get record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }

    char *start = PF::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    deserialize(start, columns, columnBitmap);
}

RecordSlot Table::insert(Columns columns) {
    checkInit();

    // Find an empty slot.
    auto slotPair = getEmptySlot();
    int page = slotPair.first;
    int slot = slotPair.second;

    Logger::log(VERBOSE, "Table: insert record to page %d slot %d\n", page,
                slot);

    PageHandle *handle = getHandle(page);
    char *start = PF::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    serialize(columns, start, COLUMN_BITMAP_ALL);

    // Mark the page as dirty.
    PF::modify(*handle);

    // We don't need to flush meta here.

    return slotPair;
}

void Table::update(int page, int slot, Columns columns, ColumnBitmap bitmap) {
    Logger::log(VERBOSE, "Table: updating record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    PageHandle *handle = getHandle(page);

    if (!occupied(*handle, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to update record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }

    char *start = PF::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    serialize(columns, start, bitmap);

    // Mark dirty.
    PF::modify(*handle);
}

void Table::remove(int page, int slot) {
    Logger::log(VERBOSE, "Table: removing record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    PageHandle *handle = getHandle(page);

    if (!occupied(*handle, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to remove record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }

    PageMeta *pageMeta = (PageMeta *)PF::loadRaw(*handle);

    // Check previous occupation.
    if (pageMeta->occupied == SLOT_FULL_MASK) {
        // The page now will have an empty slot. Add it to the free list.
        pageMeta->nextFree = meta.firstFree;
        meta.firstFree = page;
        flushMeta();
    }

    // Mark the slot as unoccupied.
    pageMeta->occupied &= ~(1L << slot);

    if ((pageMeta->occupied & SLOT_OCCUPY_MASK) == 0) {
        // TODO: If the page is empty, we can free it.
    }

    // As we are dealing with the pointer directly, we don't need to flush.
    assert(handle->validate());
}

void Table::close() {
    if (!initialized) {
        return;
    }
    Logger::log(VERBOSE, "Table: closing table\n");

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
        throw Error::InvalidColumnIndexError();
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
        throw Error::TableNotInitializedError();
    }
}

void Table::deserialize(const char *srcData, Columns destObjects,
                        ColumnBitmap bitmap) {
    // First, fetch record meta.
    RecordMeta *recordMeta = (RecordMeta *)srcData;
    srcData += sizeof(RecordMeta);

    // Actual index of `destObjects`.
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
#ifdef _DEBUG
            if (!meta.columns[i].nullable) {
                Logger::log(ERROR,
                            "Table: internal error: column %s is not nullable, "
                            "but a null value is found\n",
                            meta.columns[i].name);
                throw Error::NullValueFoundInNotNullColumnError();
            }
#endif
            column.isNull = true;
        } else {
            // We must copy the data here as we are directly using the buffer,
            // which can be invalidated.
            memcpy(column.data, srcData, column.size);
            column.isNull = false;
            if (column.type == VARCHAR) {
                column.data[column.size] = '\0';
            }
        }

        srcData += meta.columns[i].size;
        index++;
    }
}

void Table::serialize(const Columns srcObjects, char *destData,
                      ColumnBitmap bitmap) {
    RecordMeta *recordMeta = (RecordMeta *)destData;
    destData += sizeof(RecordMeta);

    // The actual index in `srcObjects`.
    int index = 0;
    for (int i = 0; i < meta.numColumn; i++) {
        if ((bitmap & (ColumnBitmap(1) << i)) == 0) {
            destData += meta.columns[i].size;
            continue;
        }
        const Column &column = srcObjects[index];
        if (column.type != meta.columns[i].type) {
            Logger::log(ERROR,
                        "Table: column type mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].type, column.type);
            throw Error::ColumnSerializationError();
        } else if (column.size != meta.columns[i].size) {
            Logger::log(ERROR,
                        "Table: column size mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].size, column.size);
            throw Error::ColumnSerializationError();
        }

        if (column.isNull) {
            if (!meta.columns[i].nullable) {
                Logger::log(ERROR,
                            "Table: column %s is not nullable, but a null "
                            "value is given\n",
                            meta.columns[i].name);
                throw Error::NullValueGivenForNotNullColumnError();
            }
            recordMeta->nullBitmap |= (1L << i);
        } else {
            recordMeta->nullBitmap &= ~(1L << i);
            memcpy(destData, column.data, column.size);
            if (column.type == VARCHAR) {
                destData[column.size] = '\0';
            }
        }

        destData += meta.columns[i].size;
        index++;
    }
}

bool Table::occupied(const PageHandle &handle, int slot) {
    PageMeta *meta = (PageMeta *)PF::loadRaw(handle);
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
        throw Error::InvalidSlotError();
    }
}

RecordSlot Table::getEmptySlot() {
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

        return std::make_pair(meta.firstFree, 1);
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
            throw Error::InvalidPageMetaError();
        }

        int index = ffs(int(~pageMeta->occupied) & SLOT_OCCUPY_MASK);

        if (index == 0) {
            Logger::log(
                ERROR, "Table: page %d is full but not marked as full\n", page);
            throw Error::InvalidPageMetaError();
        }

        index--;

        pageMeta->occupied |= (1L << index);

        if (pageMeta->occupied == SLOT_FULL_MASK) {
            // This page is full, modify meta.
            meta.firstFree = pageMeta->nextFree;
            flushMeta();
        }

        // Mark the page as dirty.
        assert(handle->validate());
        PF::modify(*handle);

        return std::make_pair(page, index);
    }
}

// ==== Column ====

int Column::deserializeInt() {
    if (type != INT) {
        Logger::log(ERROR, "Column: fail to deserialize int: type mismatch\n");
        throw Error::ColumnSerializationError();
    }
    return *(int *)data;
}

float Column::deserializeFloat() {
    if (type != FLOAT) {
        Logger::log(ERROR,
                    "Column: fail to deserialize float: type mismatch\n");
        throw Error::ColumnSerializationError();
    }
    return *(float *)data;
}

void Column::deserializeVarchar(char *dest) {
    if (type != VARCHAR) {
        Logger::log(ERROR,
                    "Column: fail to deserialize varchar: type mismatch\n");
        throw Error::ColumnSerializationError();
    }
    memcpy(dest, data, size);
}

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
    *(int *)this->data = data;
}

Column::Column(float data) {
    size = 4;
    type = FLOAT;
    *(float *)this->data = data;
}

Column::Column(const char *data, int maxLength) {
    if (maxLength > MAX_VARCHAR_LEN) {
        Logger::log(ERROR,
                    "Column: fail to create varchar column: length %d exceeds "
                    "maximum length %d\n",
                    maxLength, MAX_VARCHAR_LEN);
        throw Error::InvalidColumnSizeError();
    }

    size = maxLength;
    type = VARCHAR;
    size_t copiedSize = std::min(strlen(data), size_t(maxLength));
    memcpy(this->data, data, copiedSize);
    this->data[copiedSize] = '\0';
}

}  // namespace SimpleDB