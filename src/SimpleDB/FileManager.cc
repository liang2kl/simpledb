#include "FileManager.h"

#include <iostream>

#include "Logger.h"

namespace SimpleDB {

FileDescriptor FileManager::createFile(const std::string &fileName) {
    FILE *fd = fopen(fileName.c_str(), "a+");

    if (fd == nullptr) {
        Logger::log(ERROR, "FileManager: failed to create file %s\n",
                    fileName.c_str());
        throw Error::FAIL_CREATE_FILE;
    }

    Logger::log(VERBOSE, "FileManager: created file %s\n", fileName.c_str());
    return genNewDescriptor(fd, fileName);
}

FileDescriptor FileManager::openFile(const std::string &fileName) {
    FILE *fd = fopen(fileName.c_str(), "a+");

    if (fd == nullptr) {
        Logger::log(ERROR, "FileManager: failed to open file %s\n",
                    fileName.c_str());
        throw Error::FAIL_OPEN_FILE;
    }

    Logger::log(VERBOSE, "FileManager: opened file %s\n", fileName.c_str());
    return genNewDescriptor(fd, fileName);
}

void FileManager::closeFile(FileDescriptor descriptor) {
    if (descriptor < 0 || descriptor >= MAX_OPEN_FILES ||
        (descriptorBitmap & (1 << descriptor)) == 0) {
        Logger::log(ERROR,
                    "FileManager: fail to close file: invalid descriptor %d\n",
                    descriptor);
        throw Error::INVALID_DESCRIPTOR;
    }

    OpenedFile &file = openedFiles[descriptor];
    int err = fclose(file.fd);
    file.fd = nullptr;
    descriptorBitmap &= ~(1 << descriptor);

    if (err) {
        Logger::log(ERROR, "FileManager: fail to close file %s: %s\n",
                    file.fileName.c_str(), strerror(errno));
        throw Error::FAIL_CLOSE_FILE;
    }

    Logger::log(VERBOSE, "FileManager: closed file %s\n",
                file.fileName.c_str());
}

void FileManager::deleteFile(const std::string &fileName) {
    int err = remove(fileName.c_str());

    if (err) {
        Logger::log(ERROR, "FileManager: fail to delete file %s: %s\n",
                    fileName.c_str(), strerror(errno));
        throw Error::FAIL_DELETE_FILE;
    }

    Logger::log(VERBOSE, "FileManager: deleted file %s\n", fileName.c_str());
}

void FileManager::readPage(FileDescriptor descriptor, int page, char *data) {
    if (descriptor < 0 || descriptor >= MAX_OPEN_FILES ||
        (descriptorBitmap & (1 << descriptor)) == 0) {
        Logger::log(ERROR,
                    "FileManager: fail to read page: invalid descriptor %d\n",
                    descriptor);
        throw Error::INVALID_DESCRIPTOR;
    }

    const OpenedFile &file = openedFiles[descriptor];
    FILE *fd = file.fd;

    int err = fseek(fd, page * PAGE_SIZE, SEEK_SET);
    if (err) {
        Logger::log(ERROR,
                    "FileManager: fail to read page of file %s: seek to page "
                    "%d failed: %s\n",
                    file.fileName.c_str(), page, strerror(errno));
        throw Error::FAIL_READ_FILE;
    }

    size_t readSize = fread(data, 1, PAGE_SIZE, fd);
    if (readSize != PAGE_SIZE) {
        Logger::log(ERROR,
                    "FileManager: fail to read page of file %s: read page %d "
                    "failed: %s\n",
                    file.fileName.c_str(), page, strerror(errno));
        throw Error::FAIL_READ_FILE;
    }

    Logger::log(VERBOSE, "FileManager: read page %d from file %s\n", page,
                file.fileName.c_str());
}

void FileManager::writePage(FileDescriptor descriptor, int page, char *data) {
    if (descriptor < 0 || descriptor >= MAX_OPEN_FILES ||
        (descriptorBitmap & (1 << descriptor)) == 0) {
        Logger::log(ERROR,
                    "FileManager: fail to write page: invalid descriptor %d\n",
                    descriptor);
        throw Error::INVALID_DESCRIPTOR;
    }

    const OpenedFile &file = openedFiles[descriptor];
    FILE *fd = file.fd;

    int err = fseek(fd, page * PAGE_SIZE, SEEK_SET);
    if (err) {
        Logger::log(ERROR,
                    "FileManager: fail to write page of file %s: seek to page "
                    "%d failed: %s\n",
                    file.fileName.c_str(), page, strerror(errno));
        throw Error::FAIL_WRITE_FILE;
    }

    size_t writeSize = fwrite(data, sizeof(char), PAGE_SIZE, fd);
    if (writeSize != PAGE_SIZE) {
        Logger::log(ERROR,
                    "FileManager: fail to write page of file %s: write page %d "
                    "failed: %s\n",
                    file.fileName.c_str(), page, strerror(errno));
        throw Error::FAIL_WRITE_FILE;
    }

    Logger::log(VERBOSE, "FileManager: wrote page %d to file %s", page,
                file.fileName.c_str());
}

FileDescriptor FileManager::genNewDescriptor(FILE *fd,
                                             const std::string &fileName) {
    // Find the first unset bit.
    int index = ffsll(~descriptorBitmap);

    if (index == 0) {
        Logger::log(ERROR, "FileManager: Number of opened files exceeded.\n",
                    index);
        throw Error::OPEN_FILES_EXCEEDED;
    }

    // ffsll returns a 1-based index.
    index--;

    openedFiles[index] = {fileName, fd};
    descriptorBitmap |= (1L << index);
    return FileDescriptor(index);
}

}  // namespace SimpleDB