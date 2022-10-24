#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

#define PAGE_SIZE 8192
#define NUM_BUFFER_PAGE 60000

// Only serve as an indication to compare different pages in different files (to
// make them hashable).
#define MAX_PAGE_NUM 1000000

#endif