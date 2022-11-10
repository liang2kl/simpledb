#ifndef _SIMPLEDB_PAGE_FILE_H
#define _SIMPLEDB_PAGE_FILE_H

#include "internal/FileCoordinator.h"

namespace SimpleDB {
namespace Internal {

// The PageFile interfaces.
namespace PF {

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

template <typename P>
inline P load(PageHandle *handle) {
    return reinterpret_cast<P>(load(handle));
}

template <typename P>
inline P loadRaw(const PageHandle &handle) {
    return reinterpret_cast<P>(loadRaw(handle));
}

inline void markDirty(const PageHandle &handle) {
    FileCoordinator::shared.markDirty(handle);
}

inline PageHandle renew(const PageHandle &handle) {
    return FileCoordinator::shared.renew(handle);
}

}  // namespace PF
}  // namespace Internal
}  // namespace SimpleDB

#endif