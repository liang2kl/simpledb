#include "CacheManager.h"

#include "Logger.h"

namespace SimpleDB {

CacheManager::CacheManager(FileManager *fileManager) {
    this->fileManager = fileManager;
    cacheBuf = new PageCache[NUM_BUFFER_PAGE];
    activeCacheMapVec.resize(FileManager::MAX_OPEN_FILES);
    for (int i = 0; i < NUM_BUFFER_PAGE; i++) {
        cacheBuf[i].id = i;
        freeCache.insertHead(&cacheBuf[i]);
    }
}

CacheManager::~CacheManager() { close(); }

void CacheManager::onCloseFile(FileDescriptor fd) {
    if (!fileManager->validateFileDescriptor(fd)) {
        Logger::log(
            ERROR,
            "CacheManager: fail on closing file: invalid file descriptor: %d",
            fd.value);
        throw Error::InvalidDescriptorError();
    }

    auto &map = activeCacheMapVec[fd];

    // Store the reference to avoid iterator invalidation.
    std::vector<PageCache *> caches;
    for (auto &pair : map) {
        caches.push_back(pair.second);
    }

    for (auto cache : caches) {
        writeBack(cache);
    }
}

void CacheManager::close() {
    if (closed) {
        return;
    }

    std::vector<PageCache *> caches;

    for (auto &map : activeCacheMapVec) {
        for (auto &pair : map) {
            caches.push_back(pair.second);
        }
    }

    for (auto cache : caches) {
        writeBack(cache);
    }

    closed = true;
    delete[] cacheBuf;
}

CacheManager::PageCache *CacheManager::getPageCache(FileDescriptor fd,
                                                    int page) {
    if (!fileManager->validateFileDescriptor(fd)) {
        Logger::log(
            ERROR,
            "CacheManager: fail to get page cache: invalid file descriptor: %d",
            fd.value);

        throw Error::InvalidDescriptorError();
    }

    if (page < 0) {
        Logger::log(
            ERROR,
            "CacheManager: fail to get page cache: invalid page number %d\n",
            page);
        throw Error::InvalidPageNumberError();
    }

    // Check if the page is in the cache.
    auto &cacheMap = activeCacheMapVec[fd];
    auto iter = cacheMap.find(page);
    if (iter != cacheMap.end()) {
        // The page is cached.
        Logger::log(VERBOSE, "CacheManager: get cached page %d of file %d\n",
                    page, fd.value);
        PageCache *cache = iter->second;

        // Move the cache to the head.
        activeCache.remove(cache->nodeInActiveCache);
        cache->nodeInActiveCache = activeCache.insertHead(cache);

        return cache;
    }

    // The page is not cached.
    PageCache *cache;

    if (freeCache.size() > 0) {
        // The cache is not full.
        Logger::log(VERBOSE,
                    "CacheManager: get free cache for page %d of file %d\n",
                    page, fd.value);

        cache = freeCache.removeTail();
        assert(cache != nullptr);
    } else {
        // The cache is full. Select the last recently used page to replace.
        cache = activeCache.removeTail();
        assert(cache != nullptr);

        // Also remove from the map!
        cacheMap.erase(page);

        Logger::log(VERBOSE,
                    "CacheManager: replace cache of page %d of file %d for "
                    "page %d of file %d\n",
                    cache->meta.page, cache->meta.fd.value, page, fd.value);

        // Write back the original cache.
        writeBack(cache);
    }

    // Now we can claim this cache slot.
    cache->reset({fd, page});

    // Read the page from disk as it is not cached. Note that the page might not
    // exist yet, so we must tolerate the error.
    try {
        fileManager->readPage(fd, page, cache->buf, true);
    } catch (Error::ReadFileError) {
        // Do nothing...
    }

    // Now add this active cache to the map and the linked list.
    cacheMap[page] = cache;
    cache->nodeInActiveCache = activeCache.insertHead(cache);

    return cache;
}

void CacheManager::readPage(FileDescriptor fd, int page, char *data) {
    PageMeta meta = {fd, page};
    PageCache *cache = getPageCache(fd, page);

    memcpy(data, cache->buf, PAGE_SIZE);
}

void CacheManager::writePage(FileDescriptor fd, int page, char *data) {
    PageCache *cache = getPageCache(fd, page);
    // Set the dirty flag as we are writing.
    cache->dirty = true;

    memcpy(cache->buf, data, PAGE_SIZE);
}

void CacheManager::writeBack(PageCache *cache) {
    // As we are dealing with a valid pointer to the cache, we assume that the
    // descriptor is valid.
    Logger::log(VERBOSE, "CacheManager: write back page %d of file %d\n",
                cache->meta.page, cache->meta.fd.value);

    if (cache->dirty) {
        fileManager->writePage(cache->meta.fd, cache->meta.page, cache->buf);
    }

    // Discard the cache and add it back to the free list.
    activeCacheMapVec[cache->meta.fd].erase(cache->meta.page);
    activeCache.remove(cache->nodeInActiveCache);
    freeCache.insertHead(cache);
}

void CacheManager::writeBack(FileDescriptor fd, int page) {
    if (!fileManager->validateFileDescriptor(fd)) {
        Logger::log(
            ERROR,
            "CacheManager: fail to write back page: invalid file descriptor: "
            "%d",
            fd.value);
        throw Error::InvalidDescriptorError();
    }

    if (page < 0) {
        Logger::log(
            ERROR,
            "CacheManager: fail to write back page: invalid page number %d\n",
            page);
        throw Error::InvalidPageNumberError();
    }

    auto &cacheMap = activeCacheMapVec[fd];
    auto iter = cacheMap.find(page);
    if (iter != cacheMap.end()) {
        writeBack(iter->second);
    }
}

}  // namespace SimpleDB