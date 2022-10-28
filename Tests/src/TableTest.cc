#define TESTING
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>

using namespace SimpleDB;

class TableTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    Table table;

    const int numColumn = 3;

    ColumnMeta columnMetas[3] = {
        {INT, 4, "int_val"},
        {FLOAT, 4, "float_val"},
        {VARCHAR, 100, "varchar_val"},
    };

    const char *testVarChar = "Hello, world";

    Column testColumns[3] = {
        Column(1),
        Column(1.1F),
        Column(testVarChar, 100),
    };

    const char *tableName = "table_name";

    void initTable() {
        ASSERT_NO_THROW(
            table.create("tmp/table", tableName, numColumn, columnMetas));
    }

    void compareColumns(Column *columns, Column *readColumns) {
        for (int i = 0; i < numColumn; i++) {
            EXPECT_EQ(columns[i].type, readColumns[i].type);
            EXPECT_EQ(columns[i].size, readColumns[i].size);
            if (columns[i].type == VARCHAR) {
                EXPECT_EQ(memcmp(columns[i].data, readColumns[i].data,
                                 strlen(columns[i].data)),
                          0);
            } else {
                EXPECT_EQ(memcmp(columns[i].data, readColumns[i].data,
                                 columns[i].size),
                          0);
            }
        }
    }
};

TEST_F(TableTest, TestUninitializeAccess) {
    EXPECT_THROW(table.get(0, 0, nullptr), Error::TableNotInitializedError);
    EXPECT_THROW(table.insert(nullptr), Error::TableNotInitializedError);
    EXPECT_THROW(table.update(0, 0, nullptr), Error::TableNotInitializedError);
    EXPECT_THROW(table.remove(0, 0), Error::TableNotInitializedError);
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
    EXPECT_THROW(table.open(fileName), Error::ReadTableError);
}

TEST_F(TableTest, TestInsertGet) {
    initTable();

    // Write a few pages.
    for (int page = 1; page <= 3; page++) {
        for (int i = 0; i < NUM_SLOT_PER_PAGE - 1; i++) {
            std::pair<int, int> slotPair;
            ASSERT_NO_THROW(slotPair = table.insert(testColumns));

            Column readColumns[3];
            ASSERT_NO_THROW(
                table.get(slotPair.first, slotPair.second, readColumns));
            compareColumns(testColumns, readColumns);

            // Check if the meta is flushed.
            PageHandle handle = PF::getHandle(table.fd, slotPair.first);
            EXPECT_TRUE(table.occupied(handle, slotPair.second));
        }

        EXPECT_EQ(table.meta.firstFree, page + 1);
    }
}

TEST_F(TableTest, TestUpdate) {
    initTable();

    Column newColumns[3] = {
        Column(2),
        Column(3.1F),
        Column("Thank you!", 100),
    };

    std::pair<int, int> slotPair;
    ASSERT_NO_THROW(slotPair = table.insert(testColumns));

    ASSERT_NO_THROW(table.update(slotPair.first, slotPair.second, newColumns));

    Column readColumns[3];
    ASSERT_NO_THROW(table.get(slotPair.first, slotPair.second, readColumns));
    compareColumns(newColumns, readColumns);
}

TEST_F(TableTest, TestRemove) {
    initTable();

    std::pair<int, int> slotPair;
    ASSERT_NO_THROW(slotPair = table.insert(testColumns));

    PageHandle handle = PF::getHandle(table.fd, slotPair.first);

    ASSERT_NO_THROW(table.remove(slotPair.first, slotPair.second));
    EXPECT_FALSE(table.occupied(handle, slotPair.second));
    EXPECT_THROW(table.get(slotPair.first, slotPair.second, nullptr),
                 Error::InvalidSlotError);
}

TEST_F(TableTest, TestReleasePage) {
    initTable();

    // Write two pages (1, 2).
    for (int i = 0; i < 2 * (NUM_SLOT_PER_PAGE - 1); i++) {
        ASSERT_NO_THROW(table.insert(testColumns));
    }

    EXPECT_EQ(table.meta.firstFree, 3);

    // Release a slot from the first page (1).
    ASSERT_NO_THROW(table.remove(1, 1));

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
                 Error::InvalidColumnIndexError);
    EXPECT_THROW(table.getColumnName(numColumn, readName),
                 Error::InvalidColumnIndexError);
}
