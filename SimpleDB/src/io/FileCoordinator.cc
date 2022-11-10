#include "internal/FileCoordinator.h"

#include "internal/PageHandle.h"

namespace SimpleDB {
namespace Internal {

FileCoordinator FileCoordinator::shared = FileCoordinator();

FileCoordinator::FileCoordinator() {
    fileManager = new FileManager();
    cacheManager = new CacheManager(fileManager);
}

FileCoordinator::~FileCoordinator() {
    delete cacheManager;
    delete fileManager;
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

char *FileCoordinator::load(PageHandle *handle) {
    *handle = cacheManager->renew(*handle);
    return cacheManager->loadRaw(*handle);
}

void FileCoordinator::markDirty(const PageHandle &handle) {
    cacheManager->markDirty(handle);
}

PageHandle FileCoordinator::renew(const PageHandle &handle) {
    return cacheManager->renew(handle);
}

}  // namespace Internal
}