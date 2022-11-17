#include <SimpleDB/internal/RecordScanner.h>
#ifndef TESTING
#define TESTING 1
#endif
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class TableTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override {
        table.close();
        std::filesystem::remove_all("tmp");
    }

    Table table;

    const int numColumn = 4;

    ColumnMeta columnMetas[4] = {
        {.type = INT, .size = 4, .nullable = false, .name = "int_val"},
        {.type = FLOAT, .size = 4, .nullable = false, .name = "float_val"},
        {.type = VARCHAR,
         .size = 100,
         .nullable = false,
         .name = "varchar_val"},
        // A nullable column.
        {.type = INT, .size = 4, .nullable = true, .name = "int_val_nullable"},
    };

    const char *testVarChar = "Hello, world";

    Columns testColumns = {Column(1), Column(1.1F), Column(testVarChar, 100),
                           Column::nullIntColumn()};

    const char *tableName = "table_name";

    void initTable() {
        ASSERT_NO_THROW(
            table.create("tmp/table", tableName, numColumn, columnMetas));
    }
};

TEST_F(TableTest, TestUninitializeAccess) {
    EXPECT_THROW(auto _ = table.get({0, 0}),
                 Internal::TableNotInitializedError);
    EXPECT_THROW(table.insert(Columns()), Internal::TableNotInitializedError);
    EXPECT_THROW(table.update({0, 0}, Columns()),
                 Internal::TableNotInitializedError);
    EXPECT_THROW(table.remove({0, 0}), Internal::TableNotInitializedError);
}

TEST_F(TableTest, TestCreateNewTable) {
    initTable();

    EXPECT_EQ(table.meta.numColumn, numColumn);
    EXPECT_EQ(strcmp(tableName, table.meta.name), 0);
}

TEST_F(TableTest, TestCloseReset) {
    initTable();
    table.close();

    ASSERT_NO_THROW(table.open("tmp/table"));
}

TEST_F(TableTest, TestInitFromInvalidFile) {
    const char *fileName = "tmp/invalid_file";
    PF::create(fileName);
    EXPECT_THROW(table.open(fileName), Internal::ReadTableError);
}

TEST_F(TableTest, TestInitWithDuplicateColumnName) {
    ColumnMeta columnMetas[2] = {
        {.type = INT, .size = 4, .name = "int_val"},
        {.type = INT, .size = 4, .name = "int_val"},
    };
    EXPECT_THROW(table.create("tmp/table", tableName, 2, columnMetas),
                 Internal::DuplicateColumnNameError);
}

TEST_F(TableTest, TestInsertGet) {
    initTable();

    // Write a few pages.
    for (int page = 1; page <= 3; page++) {
        for (int i = 0; i < NUM_SLOT_PER_PAGE - 1; i++) {
            RecordID id;
            ASSERT_NO_THROW(id = table.insert(testColumns));

            Columns readColumns;
            ASSERT_NO_THROW(readColumns = table.get(id));
            compareColumns(testColumns, readColumns);

            // Check if the meta is flushed.
            PageHandle handle = PF::getHandle(table.fd, id.page);
            EXPECT_TRUE(table.occupied(handle, id.slot));
        }

        EXPECT_EQ(table.meta.firstFree, page + 1);
    }
}

TEST_F(TableTest, TestUpdate) {
    initTable();

    // Partial update.
    ColumnBitmap bitmap = 0b1101;
    Columns newColumns = {
        Column(2), Column("Thank you!", 100),
        Column(4)  // Not null now
    };

    RecordID id;
    ASSERT_NO_THROW(id = table.insert(testColumns));

    ASSERT_NO_THROW(table.update(id, newColumns, bitmap));

    Columns readColumns;
    ASSERT_NO_THROW(table.get(id, readColumns, bitmap));
    compareColumns(newColumns, readColumns);
}

TEST_F(TableTest, TestRemove) {
    initTable();

    RecordID id;
    ASSERT_NO_THROW(id = table.insert(testColumns));

    PageHandle handle = PF::getHandle(table.fd, id.page);

    ASSERT_NO_THROW(table.remove(id));
    EXPECT_FALSE(table.occupied(handle, id.slot));
    EXPECT_THROW(auto _ = table.get(id), Internal::InvalidSlotError);
}

TEST_F(TableTest, TestReleasePage) {
    initTable();

    // Write two pages (1, 2).
    for (int i = 0; i < 2 * (NUM_SLOT_PER_PAGE - 1); i++) {
        ASSERT_NO_THROW(table.insert(testColumns));
    }

    EXPECT_EQ(table.meta.firstFree, 3);

    // Release a slot from the first page (1).
    ASSERT_NO_THROW(table.remove({1, 1}));

    // The first free page should be 1.
    ASSERT_EQ(table.meta.firstFree, 1);
}

TEST_F(TableTest, TestColumnName) {
    initTable();

    char readName[MAX_COLUMN_NAME_LEN + 1];

    for (int i = 0; i < numColumn; i++) {
        EXPECT_EQ(table.getColumnIndex(columnMetas[i].name), i);
        EXPECT_NO_THROW(table.getColumnName(i, readName));
        EXPECT_EQ(strcmp(readName, columnMetas[i].name), 0);
    }

    EXPECT_THROW(table.getColumnName(-1, readName),
                 Internal::InvalidColumnIndexError);
    EXPECT_THROW(table.getColumnName(numColumn, readName),
                 Internal::InvalidColumnIndexError);
}

TEST_F(TableTest, TestInvalidVarcharSize) {
    ColumnMeta columnMetas[1] = {
        {.type = VARCHAR, .size = MAX_VARCHAR_LEN + 1, .name = "val"},
    };

    EXPECT_THROW(table.create("tmp/table", tableName, 1, columnMetas),
                 Internal::InvalidColumnSizeError);
}

TEST_F(TableTest, TestMaxVarcharSize) {
    char varchar[MAX_VARCHAR_LEN + 1];
    memset(varchar, 'a', MAX_VARCHAR_LEN);
    // This is an non-terminated string.
    varchar[MAX_VARCHAR_LEN] = 'c';

    ColumnMeta columnMetas[1] = {
        {.type = VARCHAR, .size = MAX_VARCHAR_LEN, .name = "val"},
    };

    Columns columns = {
        Column(varchar, MAX_VARCHAR_LEN),
    };

    RecordID id;

    ASSERT_NO_THROW(table.create("tmp/table", tableName, 1, columnMetas));
    ASSERT_NO_THROW(id = table.insert(columns));

    Columns readColumns;
    ASSERT_NO_THROW(table.get(id, readColumns));

    // Terminate this to compare.
    varchar[MAX_VARCHAR_LEN] = '\0';

    EXPECT_EQ(strcmp(readColumns[0].data.stringValue, varchar), 0);
}
