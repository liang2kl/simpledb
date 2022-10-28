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

    void initTable() {
        ASSERT_NO_THROW(table.create("tmp/table", numColumn, columnMetas));
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
    for (int page = 1; page < MAX_PAGE_PER_TABLE; page++) {
        EXPECT_EQ(table.meta.occupied[page], 0);
        for (int slot = 0; slot < NUM_SLOT_PER_PAGE; slot++) {
            EXPECT_EQ(table.occupied(page, slot), false);
        }
    }
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
    for (int i = 0; i < 3 * NUM_SLOT_PER_PAGE; i++) {
        std::pair<int, int> slotPair;
        ASSERT_NO_THROW(slotPair = table.insert(testColumns));

        Column readColumns[3];
        ASSERT_NO_THROW(
            table.get(slotPair.first, slotPair.second, readColumns));
        compareColumns(testColumns, readColumns);

        // Check if the meta is flushed.
        EXPECT_TRUE(table.meta.occupied[slotPair.first] &
                    (1 << slotPair.second));
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
    EXPECT_NO_THROW(slotPair = table.insert(testColumns));

    EXPECT_NO_THROW(table.update(slotPair.first, slotPair.second, newColumns));

    Column readColumns[3];
    EXPECT_NO_THROW(table.get(slotPair.first, slotPair.second, readColumns));
    compareColumns(newColumns, readColumns);
}

TEST_F(TableTest, TestRemove) {
    initTable();

    std::pair<int, int> slotPair;
    EXPECT_NO_THROW(slotPair = table.insert(testColumns));

    EXPECT_NO_THROW(table.remove(slotPair.first, slotPair.second));
    EXPECT_FALSE(table.occupied(slotPair.first, slotPair.second));
    EXPECT_THROW(table.get(slotPair.first, slotPair.second, nullptr),
                 Error::InvalidSlotError);
}

TEST_F(TableTest, TestColumnName) {
    initTable();
    for (int i = 0; i < numColumn; i++) {
        EXPECT_EQ(table.getColumn(columnMetas[i].name), i);
    }
}
