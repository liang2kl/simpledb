#include "internal/FileManager.h"

#include <string.h>

#include <filesystem>
#include <iostream>

#include "internal/Logger.h"

namespace SimpleDB {
namespace Internal {

void FileManager::createFile(const std::string &fileName) {
    if (std::filesystem::exists(fileName)) {
        Logger::log(ERROR, "FileManager: file %s already exists\n",
                    fileName.c_str());
        throw Internal::FileExistsError();
    }

    FILE *fd = fopen(fileName.c_str(), "a");

    if (fd == nullptr) {
        Logger::log(ERROR, "FileManager: failed to create file %s\n",
                    fileName.c_str());
        throw Internal::CreateFileError();
    }

    Logger::log(VERBOSE, "FileManager: created file %s\n", fileName.c_str());
    fclose(fd);
}

FileDescriptor FileManager::openFile(const std::string &fileName) {
    FILE *fd = fopen(fileName.c_str(), "rb+");

    if (fd == nullptr) {
        Logger::log(ERROR, "FileManager: failed to open file %s\n",
                    fileName.c_str());
        throw Internal::OpenFileError();
    }

    Logger::log(VERBOSE, "FileManager: opened file %s\n", fileName.c_str());
    return genNewDescriptor(fd, fileName);
}

void FileManager::closeFile(FileDescriptor descriptor) {
    if (!validate(descriptor)) {
        Logger::log(ERROR,
                    "FileManager: fail to close file: invalid descriptor %d\n",
                    descriptor.value);
        throw Internal::InvalidDescriptorError();
    }

    OpenedFile &file = openedFiles[descriptor];
    int err = fclose(file.fd);
    file.fd = nullptr;
    descriptorBitmap &= ~(1 << descriptor);

    if (err) {
        Logger::log(ERROR, "FileManager: fail to close file %s: %s\n",
                    file.fileName.c_str(), strerror(errno));
        throw Internal::CloseFileError();
    }

    Logger::log(VERBOSE, "FileManager: closed file %s\n",
                file.fileName.c_str());
}

void FileManager::deleteFile(const std::string &fileName) {
    int err = remove(fileName.c_str());

    if (err) {
        Logger::log(ERROR, "FileManager: fail to delete file %s: %s\n",
                    fileName.c_str(), strerror(errno));
        throw Internal::DeleteFileError();
    }

    Logger::log(VERBOSE, "FileManager: deleted file %s\n", fileName.c_str());
}

void FileManager::readPage(FileDescriptor descriptor, int page, char *data,
                           bool couldFail) {
    if (!validate(descriptor)) {
        Logger::log(ERROR,
                    "FileManager: fail to read page: invalid descriptor %d\n",
                    descriptor.value);
        throw Internal::InvalidDescriptorError();
    }

    if (page < 0) {
        Logger::log(ERROR,
                    "FileManager: fail to read page: invalid page number %d\n",
                    page);
        throw Internal::InvalidPageNumberError();
    }

    const OpenedFile &file = openedFiles[descriptor];
    FILE *fd = file.fd;

    int err = fseek(fd, page * PAGE_SIZE, SEEK_SET);
    if (err) {
        if (!couldFail) {
            Logger::log(
                ERROR,
                "FileManager: fail to read page of file %s: seek to page "
                "%d failed: %s\n",
                file.fileName.c_str(), page, strerror(errno));
            throw Internal::ReadFileError();
        } else {
            return;
        }
    }

    size_t readSize = fread(data, 1, PAGE_SIZE, fd);
    if (readSize != PAGE_SIZE) {
        if (!couldFail) {
            Logger::log(
                ERROR,
                "FileManager: fail to read page of file %s: read page %d "
                "failed (read size %lu)\n",
                file.fileName.c_str(), page, readSize);
            throw Internal::ReadFileError();
        } else {
            return;
        }
    }

    Logger::log(VERBOSE, "FileManager: read page %d from file %s\n", page,
                file.fileName.c_str());
}

void FileManager::writePage(FileDescriptor descriptor, int page, char *data) {
    if (!validate(descriptor)) {
        Logger::log(ERROR,
                    "FileManager: fail to write page: invalid descriptor %d\n",
                    descriptor.value);
        throw Internal::InvalidDescriptorError();
    }

    if (page < 0) {
        Logger::log(ERROR,
                    "FileManager: fail to write page: invalid page number %d\n",
                    page);
        throw Internal::InvalidPageNumberError();
    }

    const OpenedFile &file = openedFiles[descriptor];
    FILE *fd = file.fd;

    int err = fseek(fd, page * PAGE_SIZE, SEEK_SET);
    if (err) {
        Logger::log(ERROR,
                    "FileManager: fail to write page of file %s: seek to page "
                    "%d failed: %s\n",
                    file.fileName.c_str(), page, strerror(errno));
        throw Internal::WriteFileError();
    }

    size_t writeSize = fwrite(data, 1, PAGE_SIZE, fd);
    if (writeSize != PAGE_SIZE) {
        Logger::log(
            ERROR,
            "FileManager: fail to write page %d of file %s (write size: %lu)\n",
            page, file.fileName.c_str(), writeSize);
        throw Internal::WriteFileError();
    }

    Logger::log(VERBOSE, "FileManager: wrote page %d to file %s\n", page,
                file.fileName.c_str());
}

bool FileManager::validate(FileDescriptor fd) {
    return fd >= 0 && fd < MAX_OPEN_FILES &&
           (descriptorBitmap & (1L << fd)) != 0;
}

FileDescriptor FileManager::genNewDescriptor(FILE *fd,
                                             const std::string &fileName) {
    // Find the first unset bit.
    int index = ffsll(~descriptorBitmap);

    if (index == 0) {
        Logger::log(ERROR, "FileManager: Number of opened files exceeded.\n");
        throw Internal::OpenFileExceededError();
    }

    // ffsll returns a 1-based index.
    index--;

    openedFiles[index] = {fileName, fd};
    descriptorBitmap |= (1L << index);
    return FileDescriptor(index);
}

}  // namespace Internal
}  // namespace SimpleDB