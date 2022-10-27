#ifndef _SIMPLEDB_FILE_COORDINATOR_H
#define _SIMPLEDB_FILE_COORDINATOR_H

#include "CacheManager.h"
#include "FileDescriptor.h"
#include "FileManager.h"

namespace SimpleDB {

#define GET_HANDLE(fd, page) FileCoordinator::shared.getHandle(fd, page)
#define GET_BUF(handle) FileCoordinator::shared.read(handle)
#define GET_RAW_BUF(handle) FileCoordinator::shared.readRaw(handle)
#define RENEW_H(handle) (handle = FileCoordinator::shared.renew(handle))

// A proxy class to coordinate page access in FileManager and CacheManger, which
// should be the only interface to access the storage. It is not responsible for
// error handling.
class FileCoordinator {
public:
    FileCoordinator(FileManager *manager = nullptr);
    ~FileCoordinator();

    static FileCoordinator shared;

    void createFile(const std::string &fileName);
    FileDescriptor openFile(const std::string &fileName);
    void closeFile(FileDescriptor fd);
    void deleteFile(const std::string &fileName);
    PageHandle getHandle(FileDescriptor fd, int page);
    // A safe method to read handle (valid/invalid). Note that the handle might
    // be renewed.
    char *load(PageHandle *handle);
    // Directly return the buffer without validation.
    inline char *readRaw(const PageHandle &handle) {
        return cacheManager->loadRaw(handle);
    }
    void modify(const PageHandle &handle);

#ifndef TESTING
private:
#endif
    bool shouldFreeFileManager;
    FileManager *fileManager;
    CacheManager *cacheManager;
};

}  // namespace SimpleDB

#endif