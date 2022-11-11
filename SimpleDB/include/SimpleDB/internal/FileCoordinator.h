#ifndef _SIMPLEDB_FILE_COORDINATOR_H
#define _SIMPLEDB_FILE_COORDINATOR_H

#include "internal/CacheManager.h"
#include "internal/FileDescriptor.h"
#include "internal/FileManager.h"
#include "internal/PageHandle.h"

namespace SimpleDB {
namespace Internal {

// A proxy class to coordinate page access in FileManager and CacheManger, which
// should be the only interface to access the storage. It is not responsible for
// error handling.
class FileCoordinator {
public:
    ~FileCoordinator();

    static FileCoordinator shared;

    void createFile(const std::string &fileName);
    FileDescriptor openFile(const std::string &fileName);
    void closeFile(FileDescriptor fd);
    void deleteFile(const std::string &fileName);
    PageHandle getHandle(FileDescriptor fd, int page);
    // A safe method to load a page with a handle (valid/invalid). Note that the
    // handle might be renewed.
    char *load(PageHandle *handle);
    // Directly return the buffer without validation.
    inline char *loadRaw(const PageHandle &handle) {
        return cacheManager->loadRaw(handle);
    }
    void markDirty(const PageHandle &handle);
    PageHandle renew(const PageHandle &handle);

#if !TESTING
private:
#endif
    FileCoordinator();

    FileManager *fileManager;
    CacheManager *cacheManager;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif