#include "Util.h"

#include <gtest/gtest.h>

void compareColumns(SimpleDB::Column *columns, SimpleDB::Column *readColumns,
                    int num) {
    for (int i = 0; i < num; i++) {
        EXPECT_EQ(columns[i].type, readColumns[i].type);
        EXPECT_EQ(columns[i].size, readColumns[i].size);
        if (columns[i].isNull) {
            EXPECT_TRUE(readColumns[i].isNull);
            continue;
        } else {
            EXPECT_FALSE(readColumns[i].isNull);
        }
        if (columns[i].type == SimpleDB::VARCHAR) {
            EXPECT_EQ(memcmp(columns[i].data, readColumns[i].data,
                             strlen(columns[i].data)),
                      0);
        } else {
            EXPECT_EQ(
                memcmp(columns[i].data, readColumns[i].data, columns[i].size),
                0);
        }
    }
}