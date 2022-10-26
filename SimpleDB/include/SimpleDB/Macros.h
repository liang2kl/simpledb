#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

namespace SimpleDB {
const int PAGE_SIZE = 8192;
const int NUM_BUFFER_PAGE = 60000;

const int MAX_VARCHAR_LEN = 128;
const int MAX_COLUMNS = 16;

// Can be fit into a single page.
const int RECORD_SLOT_SIZE = MAX_VARCHAR_LEN * MAX_COLUMNS;

static_assert(RECORD_SLOT_SIZE <= PAGE_SIZE);
}  // namespace SimpleDB

#endif