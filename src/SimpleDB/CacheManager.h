#ifndef _SIMPLEDB_CACHE_MANAGER_H
#define _SIMPLEDB_CACHE_MANAGER_H

#include <map>

#include "FileManager.h"
#include "LinkedList.h"

namespace SimpleDB {

class CacheManager {
public:
    CacheManager(FileManager *fileManager);
    ~CacheManager();

    // Read a page from the cache (or the disk).
    void readPage(FileDescriptor fd, int page, char *data);

    // Write a page to the cache.
    void writePage(FileDescriptor fd, int page, char *data);

    // Write the cache back to the disk if it is dirty, and remove the cache. If
    // the cache does not exist, do nothing.
    void writeBack(FileDescriptor fd, int page);

    // A handler to do some cleanup before the file manager closes the file.
    void onCloseFile(FileDescriptor fd);

    // Write back all pages and destroy the cache manager.
    void close();

private:
    FileManager *fileManager;

    struct PageMeta {
        // The associated file descriptor.
        FileDescriptor fd;
        // The page number.
        int page;

        PageMeta() : fd(-1), page(-1) {}
        PageMeta(FileDescriptor fd, int page) : fd(fd), page(page) {}

        bool operator<(const PageMeta &rhs) const {
            return fd.value * MAX_PAGE_NUM + page <
                   rhs.fd.value * MAX_PAGE_NUM + rhs.page;
        }
    };

    struct PageCache {
        PageMeta meta;
        int id;  // The index in the buffer.
        bool dirty;
        char buf[PAGE_SIZE];

        // A reverse pointer to the node in the linked list.
        LinkedList<PageCache>::Node *nodeInActiveCache = nullptr;

        void reset(const PageMeta &meta) {
            this->meta = meta;
            dirty = false;
            nodeInActiveCache = nullptr;
        }
    };

    std::map<PageMeta, PageCache *> activeCacheMap;

    LinkedList<PageCache> freeCache;
    LinkedList<PageCache> activeCache;

    PageCache *cacheBuf;
    bool closed = false;

    // Write the cache back to the disk if it is dirty, and remove the cache.
    void writeBack(PageCache *cache);

    // Get the cache for certain page. Claim a slot (and read from disk) if it
    // is not cached.
    PageCache *getPageCache(FileDescriptor fd, int page);
};

}  // namespace SimpleDB

#endif