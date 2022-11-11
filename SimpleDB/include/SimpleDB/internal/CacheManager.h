#ifndef _SIMPLEDB_CACHE_MANAGER_H
#define _SIMPLEDB_CACHE_MANAGER_H

#include <map>

#include "Error.h"
#include "internal/FileManager.h"
#include "internal/LinkedList.h"

namespace SimpleDB {
namespace Internal {

// Forward declaration.
struct PageHandle;

class CacheManager {
    friend struct PageHandle;

public:
    CacheManager(FileManager *fileManager);
    ~CacheManager();

    // Load a page from the cache (or the disk).
    PageHandle getHandle(FileDescriptor fd, int page);

    // Renew the page handle, if it's invalidated.
    PageHandle renew(const PageHandle &handle);

    // Get the pointer to the buffer, NULL if the handle is invalid (i.e.
    // outdated), which indicates that the caller should re-load the page using
    // renew(). The result can be cached between two cache operations for
    // better performance and convenience.
    char *load(const PageHandle &handle);

    // The unsafe version of `load`, in which the validity of the handle is not
    // examined.
    char *loadRaw(const PageHandle &handle);

    // Mark the page as dirty, should be called after every write to the buffer.
    // The handle must be validated via validate() before calling this function,
    // otherwise InvalidPageHandleError might be thrown.
    void markDirty(const PageHandle &handle);

    // Write the cache back to the disk if it is dirty, and remove the cache. If
    // the cache does not exist, do nothing. The handle must be validated via
    // validate() before calling this function, otherwise InvalidPageHandleError
    // might be thrown.
    void writeBack(const PageHandle &handle);

    // A handler to do some cleanup before the file manager closes the file.
    void onCloseFile(FileDescriptor fd) noexcept(false);

    // Write back all pages and destroy the cache manager.
    void close();

#if TESTING
    // ==== Testing-only methods ====
    void discard(FileDescriptor fd, int page);
    void discardAll(FileDescriptor fd);
    void discardAll();
#endif

#if !TESTING
private:
#endif
    FileManager *fileManager;

    struct PageMeta {
        // The associated file descriptor.
        FileDescriptor fd;
        // The page number.
        int page;

        PageMeta() : fd(-1), page(-1) {}
        PageMeta(FileDescriptor fd, int page) : fd(fd), page(page) {}
    };

    struct PageCache {
        PageMeta meta;
        int id;  // The index in the buffer.
        bool dirty;
        char buf[PAGE_SIZE];

        int generation = 0;

        // A reverse pointer to the node in the linked list.
        LinkedList<PageCache>::Node *nodeInActiveCache = nullptr;

        // Replace this cache with another page.
        void reset(const PageMeta &meta) {
            this->meta = meta;
            dirty = false;
            nodeInActiveCache = nullptr;
            // We don't need to bump the generation number here. It's done
            // during write back.
        }
    };

    std::vector<std::map<int, PageCache *>> activeCacheMapVec;

    LinkedList<PageCache> freeCache;
    LinkedList<PageCache> activeCache;

    PageCache *cacheBuf;
    bool closed = false;

    // Write the cache back to the disk if it is dirty, and remove the cache.
    void writeBack(PageCache *cache);

    // Get the cache for certain page. Claim a slot (and load from disk) if it
    // is not cached.
    PageCache *getPageCache(FileDescriptor fd, int page) noexcept(false);
};

}  // namespace Internal
}  // namespace SimpleDB

#endif