#include "Table.h"

#include "FileCoordinator.h"
#include "Logger.h"
#include "Macros.h"

namespace SimpleDB {

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
        PageHandle handle = GET_HANDLE(fd, 0);
        meta = *(TableMeta *)LOAD_H_RAW(handle);
        initialized = true;
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to read table metadata from file %d\n",
                    fd.value);
        throw Error::ReadTableError();
    }
}

}  // namespace SimpleDB