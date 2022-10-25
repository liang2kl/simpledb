#include "FileCoordinator.h"

namespace SimpleDB {

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

FileDescriptor FileCoordinator::createFile(const std::string &fileName) {
    return fileManager->createFile(fileName);
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

void FileCoordinator::readPage(FileDescriptor fd, int page, char *data) {
    cacheManager->readPage(fd, page, data);
}

void FileCoordinator::writePage(FileDescriptor fd, int page, char *data) {
    cacheManager->writePage(fd, page, data);
}

}