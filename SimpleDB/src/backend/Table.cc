#include "internal/Table.h"

#include <string.h>

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "Error.h"
#include "internal/Column.h"
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
bool RecordID::operator>(const RecordID &rhs) const {
    return page > rhs.page || (page == rhs.page && slot > rhs.slot);
}

std::string ColumnMeta::typeDesc() const {
    switch (type) {
        case INT:
            return "INT";
        case FLOAT:
            return "FLOAT";
        case VARCHAR:
            return "VARCHAR(" + std::to_string(size) + ")";
    }
}

std::string ColumnMeta::defaultValDesc() const {
    if (!hasDefault) {
        return std::string();
    }
    switch (type) {
        case INT:
            return std::to_string(defaultValue.intValue);
        case FLOAT:
            return std::to_string(defaultValue.floatValue);
        case VARCHAR:
            return std::string(defaultValue.stringValue);
    }
}

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
                   const std::vector<ColumnMeta> &columns,
                   const std::string &primaryKey,
                   const std::vector<ForeignKey> &foreignKeys) {
    Logger::log(VERBOSE, "Table: initializing empty table to %s\n",
                file.c_str());

    if (initialized) {
        Logger::log(WARNING,
                    "Table: table is already initailized, but attempting to "
                    "re-initialize an empty table to %s\n",
                    file.c_str());
    }

    if (columns.size() > MAX_COLUMNS) {
        Logger::log(
            ERROR,
            "Table: fail to create table: too many columns: %ld, max %d\n",
            columns.size(), MAX_COLUMNS);
        throw Internal::TooManyColumnsError();
    }

    // Check primary key.
    int primaryKeyIndex = -1;
    if (!primaryKey.empty()) {
        // Find the column index.
        for (int i = 0; i < columns.size(); i++) {
            const auto &column = columns[i];
            if (column.name == primaryKey) {
                if (column.type != INT) {
                    throw InvalidPrimaryKeyError(
                        "VARCHAR or FLOAT is not supported for primary key");
                }
                primaryKeyIndex = i;
                break;
            }
        }
        if (primaryKeyIndex == -1) {
            throw InvalidPrimaryKeyError("field not exists");
        }
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
    meta.numColumn = columns.size();
    meta.primaryKeyIndex = primaryKeyIndex;
    strcpy(meta.name, name.c_str());

    int totalSize = 0;
    for (int i = 0; i < columns.size(); i++) {
        // Validate column meta.
        bool sizeValid = columns[i].type == VARCHAR
                             ? columns[i].size <= MAX_VARCHAR_LEN
                             : true;

        if (!sizeValid) {
            Logger::log(
                ERROR,
                "Table: create table failed: column %d has size %d, which is "
                "larger than maximum size %d\n",
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
        meta.columns[i].size = columns[i].type == VARCHAR ? columns[i].size : 4;
        columnNameMap[columns[i].name] = i;

        totalSize += columns[i].size;
    }

    // Check and add foreign keys.
    if (foreignKeys.size() > MAX_FOREIGN_KEYS) {
        Logger::log(
            ERROR,
            "Table: fail to create table: too many foreign keys: %ld, max %d\n",
            foreignKeys.size(), MAX_FOREIGN_KEYS);
        throw Internal::TooManyForeignKeysError();
    }

    for (int i = 0; i < foreignKeys.size(); i++) {
        const auto &foreignKey = foreignKeys[i];
        auto pair = columnNameMap.find(foreignKey.name);
        if (pair == columnNameMap.end()) {
            Logger::log(ERROR,
                        "Table: fail to create table: foreign key column %s "
                        "does not exist\n",
                        foreignKey.name.c_str());
            throw Internal::ColumnNotFoundError(foreignKey.name);
        }
        // Check type.
        if (meta.columns[pair->second].type != foreignKey.type) {
            Logger::log(ERROR,
                        "Table: fail to create table: foreign key column %s "
                        "has type %d, but foreign key has type %d\n",
                        foreignKey.name.c_str(),
                        meta.columns[pair->second].type, foreignKey.type);
            throw Internal::InvalidForeignKeyError(foreignKey.name);
        }
        meta.columns[pair->second].nullable = true;
    }

    // Set the primary key as not nullable.
    if (primaryKeyIndex != -1) {
        meta.columns[primaryKeyIndex].nullable = false;
    }

    if (totalSize > PAGE_SIZE - sizeof(RecordMeta)) {
        Logger::log(ERROR,
                    "Table: total size of columns is %d, which is larger than "
                    "maximum size %d\n",
                    totalSize, PAGE_SIZE - int(sizeof(RecordMeta)));
        throw Internal::InvalidColumnSizeError();
    }

    meta.recordSize = totalSize;

    try {
        // Create and open the file.
        PF::create(file);
    } catch (Internal::FileExistsError) {
        Logger::log(
            WARNING,
            "Table: creating an empty table to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Internal::CreateTableError();
    }

    fd = PF::open(file);

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

    char *start = PF::loadRaw(*handle) + id.slot * slotSize();
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
    char *start = PF::loadRaw(*handle) + id.slot * slotSize();

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

    char *start = PF::loadRaw(*handle) + id.slot * slotSize();
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
    if (isPageFull(pageMeta)) {
        // The page now will have an empty slot. Add it to the free list.
        pageMeta->nextFree = meta.firstFree;
        meta.firstFree = id.page;
    }

    // Mark the slot as unoccupied.
    pageMeta->occupied &= ~(1L << id.slot);

    // As we are dealing with the pointer directly, we don't need to flush.
    PF::markDirty(*handle);
    assert(handle->validate());
}

void Table::setPrimaryKey(const std::string &field) {
    checkInit();

    if (meta.primaryKeyIndex != -1) {
        throw PrimaryKeyExistsError(getColumnName(meta.primaryKeyIndex));
    }

    int columnIndex = getColumnIndex(field.c_str());

    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(field);
    }

    // Validate the column type.
    if (meta.columns[columnIndex].type != INT) {
        throw InvalidPrimaryKeyError(
            "VARCHAR or FLOAT is not supported for primary key");
    }

    // Validate the nullability.
    if (meta.columns[columnIndex].nullable) {
        throw InvalidPrimaryKeyError(
            "nullable column is not supported for primary key");
    }

    // Check the uniqueness of the primary key.
    std::set<int> keys;
    iterate([&](RecordID, Columns &columns) {
        int key = columns[columnIndex].data.intValue;
        if (keys.find(key) != keys.end()) {
            throw InvalidPrimaryKeyError("primary key has duplicated values");
        }
        keys.insert(key);
        return true;
    });

    meta.primaryKeyIndex = columnIndex;
}

void Table::dropPrimaryKey(const std::string &field) {
    checkInit();

    if (meta.primaryKeyIndex == -1) {
        throw PrimaryKeyNotExistsError();
    }

    if (!field.empty() && getColumnIndex(field.c_str()) < 0) {
        throw Internal::ColumnNotFoundError(field);
    }

    meta.primaryKeyIndex = -1;
}

void Table::close() {
    if (!initialized) {
        return;
    }
    Logger::log(VERBOSE, "Table: closing table %s\n", meta.name);

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

int Table::getColumnIndex(const char *name) const {
    auto iter = columnNameMap.find(name);
    return iter == columnNameMap.end() ? -1 : iter->second;
}

std::string Table::getColumnName(int index) const {
    if (index < 0 || index >= meta.numColumn) {
        Logger::log(ERROR, "Table: column index %d out of range [0, %d)\n",
                    index, meta.numColumn);
        throw Internal::InvalidColumnIndexError();
    }
    return meta.columns[index].name;
}

void Table::iterate(IterateCallback callback) {
    Columns bufColumns;

    for (int page = 1; page < meta.numUsedPages; page++) {
        PageHandle *handle = getHandle(page);
        for (int slot = 1; slot < numSlotPerPage(); slot++) {
            if (!handle->validate()) {
                handle = getHandle(page);
            }
            if (occupied(*handle, slot)) {
                RecordID rid = {page, slot};

                // TODO: Optimization: only get necessary columns.
                get(rid, bufColumns);
                bool _continue = callback(rid, bufColumns);
                if (!_continue) {
                    return;
                }
            }
        }
    }
}

std::vector<ColumnInfo> Table::getColumnInfo() {
    std::vector<ColumnInfo> columns;
    for (int i = 0; i < meta.numColumn; i++) {
        auto &column = meta.columns[i];
        columns.push_back({.tableName = meta.name,
                           .columnName = column.name,
                           .type = column.type});
    }
    return columns;
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
#if DEBUG
                assert(meta.columns[i].hasDefault);
#endif
                // Use default value
                memcpy(destData, meta.columns[i].defaultValue.stringValue,
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
            throw Internal::ColumnSerializationError("mismatched data type");
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
            memcpy(destData, column.data.stringValue, meta.columns[i].size);
            if (column.type == VARCHAR) {
                destData[meta.columns[i].size] = '\0';
            }
        }

        destData += meta.columns[i].size;
        index++;
    }

    if (index != srcObjects.size()) {
        Logger::log(WARNING,
                    "Table: column bitmap does not match the number of "
                    "columns: expected %d, actual %ld\n",
                    index, srcObjects.size());
    }
}

bool Table::occupied(const PageHandle &handle, int slot) {
    PageMeta *meta = PF::loadRaw<PageMeta *>(handle);
    return meta->occupied & (1L << slot);
}

void Table::validateSlot(int page, int slot) {
    // The first slot of each page (except 0) is for metadata.
    bool valid = page >= 1 && page < meta.numUsedPages && slot >= 1 &&
                 slot < numSlotPerPage();
    if (!valid) {
        Logger::log(
            ERROR,
            "Table: page/slot pair (%d, %d) is not valid, must be in range [0, "
            "%d) X [1, %d)\n",
            page, slot, meta.numUsedPages, numSlotPerPage());
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

        int index = ffsll(~pageMeta->occupied);

        if (index == 0 || index > numSlotPerPage()) {
            Logger::log(
                ERROR, "Table: page %d is full but not marked as full\n", page);
            throw Internal::InvalidPageMetaError();
        }

        index--;

        pageMeta->occupied |= (1LL << index);

        if (isPageFull(pageMeta)) {
            // This page is full, modify meta.
            meta.firstFree = pageMeta->nextFree;
        }

        // Mark the page as dirty.
        assert(handle->validate());
        PF::markDirty(*handle);

        return {page, index};
    }
}

int Table::slotSize() { return sizeof(PageMeta) + meta.recordSize; }

int Table::numSlotPerPage() {
    int nativeNum = PAGE_SIZE / slotSize();
    return nativeNum > MAX_SLOT_PER_PAGE ? MAX_SLOT_PER_PAGE : nativeNum;
}

bool Table::isPageFull(PageMeta *pageMeta) {
    int index = ffsll(~pageMeta->occupied);
    return index == 0 || index > numSlotPerPage();
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
