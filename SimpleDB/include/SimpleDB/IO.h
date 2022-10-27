#ifndef _SIMPLEDB_IO_H
#define _SIMPLEDB_IO_H

#include "internal/FileCoordinator.h"

namespace SimpleDB {
// Public interfaces for using FileCoordinator::shared.
namespace IO {

inline void create(const std::string &fileName) {
    FileCoordinator::shared.createFile(fileName);
}

inline FileDescriptor open(const std::string &fileName) {
    return FileCoordinator::shared.openFile(fileName);
}

inline void close(FileDescriptor fd) { FileCoordinator::shared.closeFile(fd); }

inline void remove(const std::string &fileName) {
    FileCoordinator::shared.deleteFile(fileName);
}

inline PageHandle getHandle(FileDescriptor fd, int page) {
    return FileCoordinator::shared.getHandle(fd, page);
}

inline char *load(PageHandle *handle) {
    return FileCoordinator::shared.load(handle);
}

inline char *loadRaw(const PageHandle &handle) {
    return FileCoordinator::shared.loadRaw(handle);
}

inline void modify(const PageHandle &handle) {
    FileCoordinator::shared.modify(handle);
}

}  // namespace IO
}  // namespace SimpleDB

#endif