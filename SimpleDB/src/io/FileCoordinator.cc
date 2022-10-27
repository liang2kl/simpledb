#include "FileCoordinator.h"

namespace SimpleDB {

FileCoordinator FileCoordinator::shared = FileCoordinator(new FileManager());

FileCoordinator::FileCoordinator(FileManager *manager) {
    if (manager != nullptr) {
        shouldFreeFileManager = false;
        fileManager = manager;
    } else {
        shouldFreeFileManager = true;
        fileManager = new FileManager();
    }

    cacheManager = new CacheManager(fileManager);
}

FileCoordinator::~FileCoordinator() {
    delete cacheManager;
    if (shouldFreeFileManager) {
        delete fileManager;
    }
}

void FileCoordinator::createFile(const std::string &fileName) {
    fileManager->createFile(fileName);
}

FileDescriptor FileCoordinator::openFile(const std::string &fileName) {
    return fileManager->openFile(fileName);
}

void FileCoordinator::closeFile(FileDescriptor fd) {
    cacheManager->onCloseFile(fd);
    fileManager->closeFile(fd);
}

void FileCoordinator::deleteFile(const std::string &fileName) {
    fileManager->deleteFile(fileName);
}

PageHandle FileCoordinator::getHandle(FileDescriptor fd, int page) {
    return cacheManager->getHandle(fd, page);
}

char *FileCoordinator::read(const PageHandle &handle) {
    return cacheManager->read(handle);
}

void FileCoordinator::modify(const PageHandle &handle) {
    cacheManager->modify(handle);
}

// PageHandle FileCoordinator

}  // namespace SimpleDB