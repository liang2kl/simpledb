#include "Table.h"

#include "IO.h"
#include "Logger.h"
#include "Macros.h"

namespace SimpleDB {

Table::~Table() {
    if (initialized) {
        // Release the file descriptor.
        IO::close(fd);
    }
}

void Table::init(const std::string &file) {
    // Initialize table metadata from the file.
    try {
        // Open the file.
        fd = IO::open(file);
        // The metadata is written in the first page.
        PageHandle handle = IO::getHandle(fd, 0);
        meta = *(TableMeta *)IO::loadRaw(handle);
        initialized = true;
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to read table metadata from file %d\n",
                    fd.value);
        throw Error::ReadTableError();
    }
}

}  // namespace SimpleDB