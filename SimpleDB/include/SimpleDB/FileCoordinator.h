#ifndef _SIMPLEDB_FILE_COORDINATOR_H
#define _SIMPLEDB_FILE_COORDINATOR_H

#include "CacheManager.h"
#include "FileDescriptor.h"
#include "FileManager.h"

namespace SimpleDB {

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
    void readPage(FileDescriptor fd, int page, char *data);
    void writePage(FileDescriptor fd, int page, char *data);

#ifndef TESTING
private:
#endif
    bool shouldFreeFileManager;
    FileManager *fileManager;
    CacheManager *cacheManager;
};

}  // namespace SimpleDB

#endif