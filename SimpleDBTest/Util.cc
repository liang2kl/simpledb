#include "Util.h"

#include <gtest/gtest.h>

void compareColumns(const SimpleDB::Internal::Columns &columns,
                    const SimpleDB::Internal::Columns &readColumns) {
    ASSERT_EQ(readColumns.size(), columns.size());
    for (int i = 0; i < columns.size(); i++) {
        EXPECT_EQ(columns[i].type, readColumns[i].type);
        EXPECT_EQ(columns[i].size, readColumns[i].size);
        if (columns[i].isNull) {
            EXPECT_TRUE(readColumns[i].isNull);
            continue;
        } else {
            EXPECT_FALSE(readColumns[i].isNull);
        }
        if (columns[i].type == SimpleDB::Internal::VARCHAR) {
            EXPECT_EQ(memcmp(columns[i].data.stringValue,
                             readColumns[i].data.stringValue,
                             strlen(columns[i].data.stringValue)),
                      0);
        } else {
            EXPECT_EQ(memcmp(columns[i].data.stringValue,
                             readColumns[i].data.stringValue, columns[i].size),
                      0);
        }
    }
}