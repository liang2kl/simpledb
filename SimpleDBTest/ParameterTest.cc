#ifndef TESTING
#define TESTING 1
#endif
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

using namespace SimpleDB;
using namespace SimpleDB::Internal;

TEST(ParameterTest, TestSlotOccupyMask) {
    for (int i = 1; i < NUM_SLOT_PER_PAGE; i++) {
        EXPECT_TRUE(SLOT_OCCUPY_MASK & (1 << i));
    }
    for (int i = NUM_SLOT_PER_PAGE; i < 32; i++) {
        EXPECT_FALSE(SLOT_OCCUPY_MASK & (1 << i));
    }
}
