#include "Table.h"

#include <string.h>

#include "IO.h"
#include "Logger.h"
#include "Macros.h"
#include "cmath"

namespace SimpleDB {

Table::Table() {
    for (int i = 0; i < MAX_PAGE_PER_TABLE; i++) {
        handles[i] = nullptr;
    }
}

Table::~Table() { close(); }

void Table::initFromFile(const std::string &file) {
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
        fd = IO::open(file);
        // The metadata is written in the first page.
        PageHandle handle = IO::getHandle(fd, 0);
        meta = *(TableMeta *)IO::loadRaw(handle);

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

void Table::initEmpty(const std::string &file, int numColumn,
                      ColumnMeta *columns) {
    Logger::log(VERBOSE, "Table: initializing empty table to %s\n",
                file.c_str());

    if (initialized) {
        Logger::log(WARNING,
                    "Table: table is already initailized, but attempting to "
                    "re-initialize an empty table to %s\n",
                    file.c_str());
    }

    try {
        // Create and open the file.
        IO::create(file);
        fd = IO::open(file);
    } catch (Error::FileExistsError) {
        Logger::log(
            WARNING,
            "Table: creating an empty table to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Error::CreateTableError();
    }

    meta.numColumn = numColumn;

    for (int i = 0; i < numColumn; i++) {
        // Validate column meta.
        if (columns[i].size > MAX_COLUMN_SIZE) {
            Logger::log(ERROR,
                        "Table: column %d has size %d, which is larger than "
                        "maximum size %d\n",
                        i, columns[i].size, MAX_COLUMN_SIZE);
            throw Error::InvalidColumnSizeError();
        }
        meta.columns[i] = columns[i];
        columnNameMap[columns[i].name] = i;
    }

    flushMeta();
    initialized = true;
}

void Table::flushMeta() {
    PageHandle handle = IO::getHandle(fd, 0);
    memcpy(IO::loadRaw(handle), &meta, sizeof(TableMeta));
    IO::modify(handle);
}

void Table::get(int page, int slot, Columns columns) {
    Logger::log(VERBOSE, "Table: get record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    if (!occupied(page, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to get record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }
    PageHandle *handle = getHandle(page);
    char *start = IO::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    deserialize(start, columns);
}

std::pair<int, int> Table::insert(Columns columns) {
    Logger::log(VERBOSE, "Table: inserting record\n");

    checkInit();

    // Find an empty slot.
    auto slotPair = getEmptySlot();
    int page = slotPair.first;
    int slot = slotPair.second;

    if (page == -1) {
        Logger::log(
            ERROR,
            "Table: fail to insert record: fail to find an empty slot\n");
        throw Error::SlotFullError();
    }

    Logger::log(VERBOSE, "Table: insert record to page %d slot %d\n", page,
                slot);

    PageHandle *handle = getHandle(page);
    char *start = IO::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    serialize(columns, start);

    // Mark the page as dirty.
    IO::modify(*handle);

    // Update metadata.
    meta.occupied[page] |= (1L << slot);
    flushMeta();

    return slotPair;
}

void Table::update(int page, int slot, Columns columns) {
    Logger::log(VERBOSE, "Table: updating record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    if (!occupied(page, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to update record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }

    PageHandle *handle = getHandle(page);
    char *start = IO::loadRaw(*handle) + slot * RECORD_SLOT_SIZE;
    serialize(columns, start);

    // Mark dirty.
    IO::modify(*handle);
}

void Table::remove(int page, int slot) {
    Logger::log(VERBOSE, "Table: removing record from page %d slot %d\n", page,
                slot);

    checkInit();
    validateSlot(page, slot);

    if (!occupied(page, slot)) {
        Logger::log(
            ERROR,
            "Table: fail to remove record: page %d slot %d is not occupied\n",
            page, slot);
        throw Error::InvalidSlotError();
    }

    meta.occupied[page] &= ~(1L << slot);
    flushMeta();
}

void Table::close() {
    if (!initialized) {
        return;
    }
    Logger::log(VERBOSE, "Table: closing table\n");

    IO::close(fd);
    for (int i = 0; i < MAX_PAGE_PER_TABLE; i++) {
        if (handles[i] != nullptr) {
            delete handles[i];
            handles[i] = nullptr;
        }
    }
    initialized = false;
    columnNameMap.clear();
}

int Table::getColumn(const char *name) {
    auto iter = columnNameMap.find(name);
    return iter == columnNameMap.end() ? -1 : iter->second;
}

PageHandle *Table::getHandle(int page) {
    if (handles[page] != nullptr && handles[page]->validate()) {
        return handles[page];
    }
    PageHandle handle = IO::getHandle(fd, page);
    handles[page] = new PageHandle(handle);
    return handles[page];
}

void Table::checkInit() {
    if (!initialized) {
        Logger::log(ERROR, "Table: not initialized yet\n");
        throw Error::TableNotInitializedError();
    }
}

void Table::deserialize(const char *srcData, Columns destObjects) {
    for (int i = 0; i < meta.numColumn; i++) {
        destObjects[i].size = meta.columns[i].size;
        destObjects[i].type = meta.columns[i].type;
        // We must copy the data here as we are directly using the buffer, which
        // can be invalidated.
        memcpy(destObjects[i].data, srcData, destObjects[i].size);
        srcData += meta.columns[i].size;
    }
}

void Table::serialize(const Columns srcObjects, char *destData) {
    for (int i = 0; i < meta.numColumn; i++) {
        if (srcObjects[i].type != meta.columns[i].type) {
            Logger::log(ERROR,
                        "Table: column type mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].type, srcObjects[i].type);
            throw Error::ColumnSerializationError();
        } else if (srcObjects[i].size != meta.columns[i].size) {
            Logger::log(ERROR,
                        "Table: column size mismatch when serializing data of "
                        "column %d: expected %d, actual %d\n",
                        i, meta.columns[i].size, srcObjects[i].size);
            throw Error::ColumnSerializationError();
        }
        memcpy(destData, srcObjects[i].data, srcObjects[i].size);
        destData += srcObjects[i].size;
    }
}

bool Table::occupied(int page, int slot) {
    return meta.occupied[page] & (1L << slot);
}

void Table::validateSlot(int page, int slot) {
    bool valid = page > 0 && page < MAX_PAGE_PER_TABLE;
    if (!valid) {
        Logger::log(ERROR,
                    "Table: page %d is not valid, must be in range [1, %d)\n",
                    page, MAX_PAGE_PER_TABLE);
        throw Error::InvalidSlotError();
    }
}

std::pair<int, int> Table::getEmptySlot() {
    for (int i = 1; i < MAX_PAGE_PER_TABLE; i++) {
        int index = ffs(int(~meta.occupied[i]) & SLOT_OCCUPY_MASK);
        if (index == 0) {
            continue;
        }
        return std::make_pair(i, index - 1);
    }
    return std::make_pair(-1, -1);
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
    dest[size] = '\0';
}

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
        throw Error::ColumnSerializationError();
    }

    size = maxLength;
    type = VARCHAR;
    size_t copiedSize = std::min(strlen(data), size_t(maxLength));
    memcpy(this->data, data, copiedSize);
}

}  // namespace SimpleDB