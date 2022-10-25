#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

// Make private members public for testing.
#ifdef TESTING
#define private public
#define protected public
#endif

namespace SimpleDB {
const int PAGE_SIZE = 8192;
const int NUM_BUFFER_PAGE = 60000;

// Only serve as an indication to compare different pages in different files (to
// make them hashable).
const int MAX_PAGE_NUM = 1000000;
}  // namespace SimpleDB

#endif