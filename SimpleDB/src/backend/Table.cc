#include "Table.h"

#include "FileCoordinator.h"
#include "Logger.h"
#include "Macros.h"

namespace SimpleDB {

static char pageBuf[PAGE_SIZE];

Table::~Table() {
    if (initialized) {
        // Release the file descriptor.
        FileCoordinator::shared.closeFile(fd);
    }
}

void Table::init(const std::string &file) {
    // Initialize table metadata from the file.
    try {
        // Open the file.
        fd = FileCoordinator::shared.openFile(file);
        // The metadata is written in the first page.
        FileCoordinator::shared.readPage(fd, 0, pageBuf);
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to read table metadata from file %d\n",
                    fd.value);
        throw Error::ReadTableError();
    }

    meta = *(TableMeta *)pageBuf;
    initialized = true;
}

}  // namespace SimpleDB