#ifndef _SIMPLEDB_FILE_MANAGER_H
#define _SIMPLEDB_FILE_MANAGER_H

#include <string>
#include <vector>

#include "BaseError.h"
#include "FileDescriptor.h"
#include "Macros.h"

namespace SimpleDB {

class FileManager {
public:
    // Error of file operations.
    struct Error : BaseError {
        enum Type {
            FAIL_OPEN_FILE,
            FAIL_CREATE_FILE,
            FAIL_CLOSE_FILE,
            FAIL_READ_FILE,
            FAIL_WRITE_FILE,
            FAIL_DELETE_FILE,
            INVALID_DESCRIPTOR,
            OPEN_FILES_EXCEEDED,
        };
        Type type;
    };

    // The maximum number of files that can be opened at the same time.
    static const int MAX_OPEN_FILES = 64;

    FileDescriptor createFile(const std::string &fileName);
    FileDescriptor openFile(const std::string &fileName);
    void closeFile(FileDescriptor fd);
    void deleteFile(const std::string &fileName);
    void readPage(FileDescriptor fd, int page, char *data);
    void writePage(FileDescriptor fd, int page, char *data);

private:
    struct OpenedFile {
        std::string fileName;
        FILE *fd = nullptr;
    };
    OpenedFile openedFiles[MAX_OPEN_FILES];
    uint64_t descriptorBitmap = 0;

    FileDescriptor genNewDescriptor(FILE *fd, const std::string &fileName);
};

}  // namespace SimpleDB

#endif