#ifndef _SIMPLEDB_PAGE_HANDLE_H
#define _SIMPLEDB_PAGE_HANDLE_H

#include "internal/CacheManager.h"

namespace SimpleDB {
namespace Internal {

// A handle of a page cache used to access a page.
struct PageHandle {
    friend class CacheManager;

public:
    // This constructor is only for declaration, and must be initialized before
    // use.
    PageHandle() = default;
    bool validate() const { return cache->generation == generation; }

#if !TESTING
private:
#else
public:
#endif
    PageHandle(CacheManager::PageCache *cache)
        : generation(cache->generation), cache(cache) {}
    int generation = -1;
    CacheManager::PageCache *cache;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif