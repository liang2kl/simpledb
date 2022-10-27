#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

namespace SimpleDB {
const int PAGE_SIZE = 8192;
const int NUM_BUFFER_PAGE = 60000;

const int MAX_VARCHAR_LEN = 128;
const int MAX_COLUMNS = 16;
const int MAX_COLUMN_SIZE = MAX_VARCHAR_LEN;
const int MAX_COLUMN_NAME_LEN = 64;

// Can be fit into a single page.
const int RECORD_SLOT_SIZE = MAX_VARCHAR_LEN * MAX_COLUMNS;
const int NUM_SLOT_PER_PAGE = PAGE_SIZE / RECORD_SLOT_SIZE;
const int MAX_PAGE_PER_TABLE = 1500;
const int MAX_RECORD_PER_TABLE = NUM_SLOT_PER_PAGE * MAX_PAGE_PER_TABLE;

static_assert(RECORD_SLOT_SIZE <= PAGE_SIZE);

const uint64_t TABLE_META_CANARY = 81249122;
}  // namespace SimpleDB

#endif