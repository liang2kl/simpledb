#include "internal/CacheManager.h"

#include <string.h>

#include <cassert>

#include "internal/Logger.h"
#include "internal/PageHandle.h"

namespace SimpleDB {
namespace Internal {

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
    if (!fileManager->validate(fd)) {
        Logger::log(
            ERROR,
            "CacheManager: fail on closing file: invalid file descriptor: %d",
            fd.value);
        throw Internal::InvalidDescriptorError();
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
    if (!fileManager->validate(fd)) {
        Logger::log(ERROR,
                    "CacheManager: fail to get page cache: invalid file "
                    "descriptor: %d\n",
                    fd.value);

        throw Internal::InvalidDescriptorError();
    }

    if (page < 0) {
        Logger::log(
            ERROR,
            "CacheManager: fail to get page cache: invalid page number %d\n",
            page);
        throw Internal::InvalidPageNumberError();
    }

    // Check if the page is in the cache.
    // TODO: Optimization.
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
        PageCache *lastCache = activeCache.last();
        assert(lastCache != nullptr);

        Logger::log(VERBOSE,
                    "CacheManager: replace cache of page %d of file %d for "
                    "page %d of file %d\n",
                    lastCache->meta.page, lastCache->meta.fd.value, page,
                    fd.value);

        // Write back the original cache (the freed cache will be in the
        // `freeCache` list).
        writeBack(lastCache);
        cache = freeCache.removeTail();
    }

    // Now we can claim this cache slot.
    cache->reset({fd, page});

    // Read the page from disk as it is not cached. Note that the page might not
    // exist yet, so we must tolerate the error.
    fileManager->readPage(fd, page, cache->buf, true);

    // Now add this active cache to the map and the linked list.
    cacheMap[page] = cache;
    cache->nodeInActiveCache = activeCache.insertHead(cache);

    return cache;
}

PageHandle CacheManager::getHandle(FileDescriptor fd, int page) {
    PageCache *cache = getPageCache(fd, page);

    return PageHandle(cache);
}

PageHandle CacheManager::renew(const PageHandle &handle) {
    if (handle.validate()) {
        return handle;
    }

    return getHandle(handle.cache->meta.fd, handle.cache->meta.page);
}

char *CacheManager::load(const PageHandle &handle) {
    if (!handle.validate()) {
        Logger::log(DEBUG_,
                    "CacheManager: trying to read data with an outdated page "
                    "handle for page %d of file %d\n",
                    handle.cache->meta.page, handle.cache->meta.fd.value);
        return nullptr;
    }

    return handle.cache->buf;
}

char *CacheManager::loadRaw(const PageHandle &handle) {
#ifdef DEBUG
    if (!handle.validate()) {
        Logger::log(ERROR,
                    "CacheManager: trying to read data with an outdated page "
                    "handle for page %d of file %d\n",
                    handle.cache->meta.page, handle.cache->meta.fd.value);
        assert(false);
        throw Internal::InvalidPageHandleError();
    }
#endif
    return handle.cache->buf;
}

void CacheManager::markDirty(const PageHandle &handle) {
    PageCache *cache = handle.cache;

    if (!handle.validate()) {
        Logger::log(
            ERROR,
            "CacheManager: fail to modify page %d of file %d: "
            "possible outdated page handle: current generation %d, got %d\n",
            cache->meta.fd.value, cache->meta.page, cache->generation,
            handle.generation);
        throw Internal::InvalidPageHandleError();
    }

    cache->dirty = true;
}

void CacheManager::writeBack(PageCache *cache) {
    // As we are dealing with a valid pointer to the cache, we assume that the
    // descriptor is valid.

    if (cache->dirty) {
        Logger::log(VERBOSE,
                    "CacheManager: write back dirty page %d of file %d\n",
                    cache->meta.page, cache->meta.fd.value);
        fileManager->writePage(cache->meta.fd, cache->meta.page, cache->buf);
    } else {
        Logger::log(VERBOSE, "CacheManager: discarding page %d of file %d\n",
                    cache->meta.page, cache->meta.fd.value);
    }

    // Discard the cache and add it back to the free list.
    activeCacheMapVec[cache->meta.fd].erase(cache->meta.page);
    activeCache.remove(cache->nodeInActiveCache);
    freeCache.insertHead(cache);
    // Don't forget to bump the generation number, as the previous cache is no
    // longer valid.
    cache->generation++;
}

void CacheManager::writeBack(const PageHandle &handle) {
    PageCache *cache = handle.cache;

    if (!handle.validate()) {
        Logger::log(
            ERROR,
            "CacheManager: fail to write back page %d of file %d: "
            "possible outdated page handle: current generation %d, got %d\n",
            cache->meta.fd.value, cache->meta.page, cache->generation,
            handle.generation);
        throw Internal::InvalidPageHandleError();
    }

    writeBack(cache);
}

#if TESTING
// ==== Testing-only methods ====
void CacheManager::discard(FileDescriptor fd, int page) {
    auto &cacheMap = activeCacheMapVec[fd];
    auto iter = cacheMap.find(page);
    if (iter != cacheMap.end()) {
        PageCache *cache = iter->second;
        activeCacheMapVec[cache->meta.fd].erase(cache->meta.page);
        activeCache.remove(cache->nodeInActiveCache);
        freeCache.insertHead(cache);
        cache->generation++;
    }
}

void CacheManager::discardAll(FileDescriptor fd) {
    auto &map = activeCacheMapVec[fd];
    std::vector<PageCache *> caches;
    for (auto &pair : map) {
        caches.push_back(pair.second);
    }
    for (auto cache : caches) {
        discard(cache->meta.fd, cache->meta.page);
    }
}

void CacheManager::discardAll() {
    std::vector<PageCache *> caches;
    for (auto &map : activeCacheMapVec) {
        for (auto &pair : map) {
            caches.push_back(pair.second);
        }
    }
    for (auto cache : caches) {
        discard(cache->meta.fd, cache->meta.page);
    }
}
#endif

}  // namespace Internal
}  // namespace SimpleDB