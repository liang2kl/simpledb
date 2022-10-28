#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

namespace SimpleDB {
const int PAGE_SIZE = 8192;
const int NUM_BUFFER_PAGE = 1024;

const int MAX_VARCHAR_LEN = 256;
const int MAX_COLUMN_SIZE = MAX_VARCHAR_LEN;
const int MAX_COLUMNS = 16;
const int MAX_COLUMN_NAME_LEN = 64;
const int MAX_TABLE_NAME_LEN = 64;

// Can be fit into a single page.
const int RECORD_SLOT_SIZE = 1024;
static_assert(RECORD_SLOT_SIZE <= PAGE_SIZE);

const int NUM_SLOT_PER_PAGE = PAGE_SIZE / RECORD_SLOT_SIZE;
// Should update when NUM_SLOT_PER_PAGE changes.
const int SLOT_OCCUPY_MASK = 0b11111110;
static_assert((SLOT_OCCUPY_MASK & 1) == 0);
const int16_t SLOT_FULL_MASK = 0x00FF;

const uint16_t TABLE_META_CANARY = 0xDDBB;
const uint16_t PAGE_META_CANARY = 0xDBDB;

}  // namespace SimpleDB

#endif