#include "CacheManager.h"

#include "Logger.h"

namespace SimpleDB {

CacheManager::CacheManager(FileManager *fileManager) {
    this->fileManager = fileManager;
    cacheBuf = new PageCache[NUM_BUFFER_PAGE];
    for (int i = 0; i < NUM_BUFFER_PAGE; i++) {
        cacheBuf[i].id = i;
        freeCache.insert(&cacheBuf[i]);
    }
}

CacheManager::~CacheManager() { close(); }

void CacheManager::onCloseFile(FileDescriptor fd) {
    // FIXME: Need a better data structure to find associated caches.
    for (auto &pair : activeCacheMap) {
        if (pair.first.fd == fd) {
            writeBack(pair.second);
        }
    }
}

void CacheManager::close() {
    if (closed) {
        return;
    }
    for (auto &pair : activeCacheMap) {
        writeBack(pair.second);
    }
    closed = true;
    delete[] cacheBuf;
}

CacheManager::PageCache *CacheManager::getPageCache(FileDescriptor fd,
                                                    int page) {
    PageMeta meta = {fd, page};

    // Check if the page is in the cache.
    auto iter = activeCacheMap.find(meta);
    if (iter != activeCacheMap.end()) {
        // The page is cached.
        Logger::log(VERBOSE, "CacheManager: get cached page %d of file %d\n",
                    page, fd.value);
        PageCache *cache = iter->second;

        // Move the cache to the head.
        activeCache.remove(cache->nodeInActiveCache);
        cache->nodeInActiveCache = activeCache.insert(cache);

        return cache;
    }

    // The page is not cached.
    PageCache *cache;

    if (freeCache.size() > 0) {
        // The cache is not full.
        Logger::log(VERBOSE,
                    "CacheManager: get free cache for page %d of file %d\n",
                    page, fd.value);

        cache = freeCache.removeLRU();
        assert(cache != nullptr);
    } else {
        // The cache is full. Select the last recently used page to replace.
        cache = activeCache.removeLRU();
        assert(cache != nullptr);

        // Also remove from the map!
        activeCacheMap.erase(cache->meta);

        Logger::log(VERBOSE,
                    "CacheManager: replace cache of page %d of file %d for "
                    "page %d of file %d\n",
                    cache->meta.page, cache->meta.fd.value, page, fd.value);

        // Write back the original cache.
        writeBack(cache);
        // Now we can claim this cache slot.
        cache->reset(meta);
    }

    // Read the page from disk as it is not cached.
    fileManager->readPage(fd, page, cache->buf);

    // Now add this active cache to the map and the linked list.
    activeCacheMap[meta] = cache;
    cache->nodeInActiveCache = activeCache.insert(cache);

    return cache;
}

void CacheManager::readPage(FileDescriptor fd, int page, char *data) {
    PageMeta meta = {fd, page};
    PageCache *cache = getPageCache(fd, page);

    memcpy(data, cache->buf, PAGE_SIZE);
}

void CacheManager::writePage(FileDescriptor fd, int page, char *data) {
    PageMeta meta = {fd, page};
    PageCache *cache = getPageCache(fd, page);
    // Set the dirty flag as we are writing.
    cache->dirty = true;

    memcpy(cache->buf, data, PAGE_SIZE);
}

void CacheManager::writeBack(PageCache *cache) {
    if (cache->dirty) {
        fileManager->writePage(cache->meta.fd, cache->meta.page, cache->buf);
    }

    // Discard the cache and add it back to the free list.
    activeCacheMap.erase(cache->meta);
    PageCache *cache = activeCache.remove(cache->nodeInActiveCache);
    freeCache.insert(cache);
}

void CacheManager::writeBack(FileDescriptor fd, int page) {
    PageMeta meta = {fd, page};
    auto iter = activeCacheMap.find(meta);
    if (iter != activeCacheMap.end()) {
        writeBack(iter->second);
    }
}

}  // namespace SimpleDB