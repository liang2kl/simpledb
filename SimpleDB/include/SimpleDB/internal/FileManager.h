#ifndef _SIMPLEDB_FILE_MANAGER_H
#define _SIMPLEDB_FILE_MANAGER_H

#include <string>
#include <vector>

#include "Error.h"
#include "internal/FileDescriptor.h"
#include "internal/Macros.h"

namespace SimpleDB {
namespace Internal {

class FileManager {
public:
    // Error of file operations.

    // The maximum number of files that can be opened at the same time.
    static const int MAX_OPEN_FILES = 64;

    void createFile(const std::string &fileName) noexcept(false);
    FileDescriptor openFile(const std::string &fileName) noexcept(false);
    void closeFile(FileDescriptor fd) noexcept(false);
    void deleteFile(const std::string &fileName) noexcept(false);
    void readPage(FileDescriptor fd, int page, char *data,
                  bool couldFail = false) noexcept(false);
    void writePage(FileDescriptor fd, int page, char *data) noexcept(false);

    // Check if the file descriptor is valid.
    bool validate(FileDescriptor fd);

#if !TESTING
private:
#endif
    struct OpenedFile {
        std::string fileName;
        FILE *fd = nullptr;
    };
    OpenedFile openedFiles[MAX_OPEN_FILES];
    uint64_t descriptorBitmap = 0;

    FileDescriptor genNewDescriptor(FILE *fd, const std::string &fileName);
};

}  // namespace Internal
}  // namespace SimpleDB

#endif