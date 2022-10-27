#ifndef _SIMPLEDB_PAGE_HANDLE_H
#define _SIMPLEDB_PAGE_HANDLE_H

#include "internal/CacheManager.h"

namespace SimpleDB {

// A handle of a page cache used to access a page.
struct PageHandle {
    friend class CacheManager;

public:
    // This constructor is only for declaration, and must be initialized before
    // use.
    PageHandle() = default;
    bool validate() const { return cache->generation == generation; }

#ifndef TESTING
private:
#else
public:
#endif
    PageHandle(CacheManager::PageCache *cache)
        : cache(cache), generation(cache->generation) {}
    int generation = -1;
    CacheManager::PageCache *cache;
};

}  // namespace SimpleDB

#endif