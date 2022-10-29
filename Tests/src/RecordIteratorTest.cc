#define TESTING
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

#include "Util.h"

using namespace SimpleDB;

class RecordIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory("tmp");
        initTable();
    }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    Table table;

    const int numColumn = 4;

    ColumnMeta columnMetas[4] = {
        {.type = INT, .size = 4, .nullable = true, .name = "int_val"},
        {.type = FLOAT, .size = 4, .nullable = true, .name = "float_val"},
        {.type = VARCHAR, .size = 100, .nullable = true, .name = "varchar_val"},
        // A nullable column.
        {.type = INT, .size = 4, .nullable = true, .name = "int_val_nullable"},
    };

    const char *testVarChar = "Hello, world";
    const char *tableName = "table_name";

    void initTable() {
        ASSERT_NO_THROW(
            table.create("tmp/table", tableName, numColumn, columnMetas));
        iter = table.getIterator();
    }

    RecordIterator iter;
};

TEST_F(RecordIteratorTest, TestCompareInt) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(5), Column(-1.1F),
                              Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns2[4] = {Column(3), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));
    ASSERT_NO_THROW(table.insert(testColumns2));

    Column readColumns[4];

    CompareConditions conditions = CompareConditions(1);

    // EQ.
    int value = 1;
    conditions[0] =
        CompareCondition(columnMetas[0].name, EQ, (const char *)(&value));

    int numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns0, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    // GT.
    value = 4;
    conditions[0] =
        CompareCondition(columnMetas[0].name, GT, (const char *)(&value));

    numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns1, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    // Emptyset.
    value = 10;
    conditions[0] =
        CompareCondition(columnMetas[0].name, GE, (const char *)(&value));
    numRecords =
        iter.iterate(readColumns, conditions, [](int) { ASSERT_TRUE(false); });
    EXPECT_EQ(numRecords, 0);

    // Two constraints (GE, LT).
    conditions.resize(2);
    int value1 = 3, value2 = 5;
    conditions[0] =
        CompareCondition(columnMetas[0].name, GE, (const char *)(&value1));
    conditions[1] =
        CompareCondition(columnMetas[0].name, LT, (const char *)(&value2));

    numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns2, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);
}

TEST_F(RecordIteratorTest, TestCompareFloat) {
    Column testColumns0[4] = {Column(0), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(0), Column(-1.1F),
                              Column(testVarChar, 100),
                              Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));

    Column readColumns[4];

    CompareConditions conditions = CompareConditions(1);

    // EQ and GE.
    std::vector<std::tuple<CompareOp, float, Column *>> pairs = {
        std::make_tuple(EQ, atof("1.1"), testColumns0),
        std::make_tuple(GE, atof("1.1"), testColumns0),
        std::make_tuple(GE, atof("1.0"), testColumns0),
        std::make_tuple(EQ, atof("-1.1"), testColumns1),
        std::make_tuple(LE, atof("-1.1"), testColumns1),
        std::make_tuple(LE, atof("-1.0"), testColumns1),
        std::make_tuple(GT, atof("1.0"), testColumns0),
        std::make_tuple(LT, atof("-1.0"), testColumns1)};

    for (auto &pair : pairs) {
        CompareOp op = std::get<0>(pair);
        float value = std::get<1>(pair);
        Column *testColumn = std::get<2>(pair);

        conditions[0] = CompareCondition(columnMetas[1].name, std::get<0>(pair),
                                         (const char *)(&value));

        int numRecords = iter.iterate(readColumns, conditions, [&](int index) {
            compareColumns(testColumn, readColumns, 4);
        });
        EXPECT_EQ(numRecords, 1);
    }
}

TEST_F(RecordIteratorTest, TestCompareVarchar) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column("aaabbb", 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(5), Column(-1.0F), Column("b", 100),
                              Column::nullIntColumn()};
    Column testColumns2[4] = {Column(1), Column(1.1F), Column("aaaaaa", 100),
                              Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));
    ASSERT_NO_THROW(table.insert(testColumns2));

    // EQ.
    CompareConditions conditions = CompareConditions(1);
    char compareStr[100] = "aaabbb";
    conditions[0] = CompareCondition(columnMetas[2].name, EQ, compareStr);

    Column readColumns[4];
    int numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns0, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    // GT.
    conditions[0].op = GT;
    numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns1, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    // LT.
    conditions[0].op = LT;
    numRecords = iter.iterate(readColumns, conditions, [&](int index) {
        compareColumns(testColumns2, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);
}

TEST_F(RecordIteratorTest, TestNullField) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};

    CompareConditions conditions = CompareConditions(2);
    int value = 1;
    conditions[0] =
        CompareCondition(columnMetas[0].name, EQ, (const char *)(&value));
    conditions[1] =
        CompareCondition(columnMetas[3].name, EQ, (const char *)(&value));

    Column readColumns[4];
    int numRecords = iter.iterate(readColumns, conditions, [&](int) {});

    EXPECT_EQ(numRecords, 0);
}

TEST_F(RecordIteratorTest, TestNullOp) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column(1)};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));

    CompareConditions conditions = CompareConditions(1);
    conditions[0] = CompareCondition(columnMetas[3].name, IS_NULL, nullptr);

    Column readColumns[4];
    int numRecords = iter.iterate(readColumns, conditions, [&](int) {
        compareColumns(testColumns0, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    conditions[0].op = NOT_NULL;
    numRecords = iter.iterate(readColumns, conditions, [&](int) {
        compareColumns(testColumns1, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);
}

TEST_F(RecordIteratorTest, TestNullValue) {
    Column testColumns0[4] = {Column(1), Column(1.1F),
                              Column::nullVarcharColumn(100), Column(2)};

    ASSERT_NO_THROW(table.insert(testColumns0));

    std::vector<std::pair<CompareOp, int>> testCases = {
        {EQ, 0}, {NE, 0},   {GT, 0},      {GE, 0},       {LT, 0},
        {LE, 0}, {LIKE, 0}, {IS_NULL, 1}, {NOT_NULL, 0},
    };

    CompareConditions conditions = CompareConditions(1);

    for (auto &testCase : testCases) {
        conditions[0] =
            CompareCondition(columnMetas[2].name, testCase.first, nullptr);

        Column readColumns[4];
        int numRecords = iter.iterate(readColumns, conditions, [&](int) {});
        EXPECT_EQ(numRecords, testCase.second);
    }
}

TEST_F(RecordIteratorTest, TestLikeOp) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column("123451", 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(1), Column(1.1F), Column("012314", 100),
                              Column(1)};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));

    // Matching the first but not second.
    const char *regex = "[1-9]+";

    CompareConditions conditions = CompareConditions(1);
    conditions[0] = CompareCondition(columnMetas[2].name, LIKE, regex);

    Column readColumns[4];
    int numRecords = iter.iterate(readColumns, conditions, [&](int) {
        compareColumns(testColumns0, readColumns, 4);
    });
    EXPECT_EQ(numRecords, 1);

    // Test invalid regex.
    regex = "[1-9";
    conditions[0].value = regex;
    EXPECT_THROW(iter.iterate(readColumns, conditions, [&](int) {}),
                 Error::InvalidRegexError);
}

TEST_F(RecordIteratorTest, TestInvalidLikeOperator) {
    Column testColumns0[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    ASSERT_NO_THROW(table.insert(testColumns0));

    CompareConditions conditions = CompareConditions(1);
    conditions[0] = CompareCondition(columnMetas[0].name, LIKE, "[0-9]+");

    Column readColumns[4];
    EXPECT_THROW(iter.iterate(readColumns, conditions, [&](int) {}),
                 Error::InvalidOperatorError);
}

TEST_F(RecordIteratorTest, TestIndexedScan) {
    Column testColumns0[4] = {Column(0), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns1[4] = {Column(1), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns2[4] = {Column(2), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};
    Column testColumns3[4] = {Column(3), Column(1.1F), Column(testVarChar, 100),
                              Column::nullIntColumn()};

    Column *columns[] = {testColumns0, testColumns1, testColumns2,
                         testColumns3};

    std::vector<RecordID> records;
    ASSERT_NO_THROW(records.push_back(table.insert(testColumns0)));
    ASSERT_NO_THROW(records.push_back(table.insert(testColumns1)));
    ASSERT_NO_THROW(records.push_back(table.insert(testColumns2)));
    ASSERT_NO_THROW(records.push_back(table.insert(testColumns3)));

    // Empty conditions.
    CompareConditions conditions;
    Column readColumns[4];

    for (int i = 0; i < records.size(); i++) {
        bool got = false;
        int numRecords = iter.iterate(
            readColumns, conditions,
            [&]() -> RecordID {
                // Only request for records[i].
                if (got) return {-1, -1};
                got = true;
                return records[i];
            },
            [&](int) { compareColumns(columns[i], readColumns, 4); });
        EXPECT_EQ(numRecords, 1);
    }
}
