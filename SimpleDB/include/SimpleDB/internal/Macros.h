#ifndef _SIMPLEDB_MACROS_H
#define _SIMPLEDB_MACROS_H

#include <stdint.h>

#include <limits>

namespace SimpleDB {
namespace Internal {

const int PAGE_SIZE = 8192;
const int NUM_BUFFER_PAGE = 1024;

const int MAX_VARCHAR_LEN = 256 - 1;
const int MAX_COLUMN_SIZE = MAX_VARCHAR_LEN + 1;
const int MAX_COLUMNS = 16;
const int MAX_COLUMN_NAME_LEN = 64;
const int MAX_TABLE_NAME_LEN = 64;
const int MAX_DATABASE_NAME_LEN = MAX_VARCHAR_LEN;
const int MAX_FOREIGN_KEYS = 12;

const int MAX_SLOT_PER_PAGE = 64;
const int16_t COLUMN_BITMAP_ALL = ~0;
static_assert(MAX_SLOT_PER_PAGE < NUM_BUFFER_PAGE);

const uint16_t TABLE_META_CANARY = 0xDDBB;
const uint16_t PAGE_META_CANARY = 0xDBDB;
const uint16_t INDEX_META_CANARY = 0xDADA;
const uint16_t EMPTY_INDEX_PAGE_CANARY = 0xDCDC;

const int INDEX_SLOT_SIZE = 424;
const int NUM_INDEX_SLOT = 18;
const int MAX_NUM_CHILD_PER_NODE = 20;
const int MIN_NUM_CHILD_PER_NODE = (MAX_NUM_CHILD_PER_NODE + 1) / 2;
const int MAX_NUM_ENTRY_PER_NODE = MAX_NUM_CHILD_PER_NODE - 1;
const int MIN_NUM_ENTRY_PER_NODE = MIN_NUM_CHILD_PER_NODE - 1;
static_assert(NUM_INDEX_SLOT * INDEX_SLOT_SIZE <= PAGE_SIZE);
static_assert(MAX_NUM_CHILD_PER_NODE + MAX_NUM_ENTRY_PER_NODE + 3 <=
              NUM_BUFFER_PAGE);

const float EQUAL_PRECISION = std::numeric_limits<float>::epsilon();

// ==== Index ====
const int INDEX_SIZE = 4;

}  // namespace Internal
}  // namespace SimpleDB

#endif