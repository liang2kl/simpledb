#include <SimpleDB/SimpleDB.h>
#include <SimpleDB/internal/QueryBuilder.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class QueryConditionTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory("tmp");
        initTable();
    }
    void TearDown() override {
        table.close();
        std::filesystem::remove_all("tmp");
    }

    Table table;

    std::vector<ColumnMeta> columnMetas = {
        {.type = INT, .size = 4, .nullable = true, .name = "int_val"},
        {.type = FLOAT, .size = 4, .nullable = true, .name = "float_val"},
        {.type = VARCHAR, .size = 100, .nullable = true, .name = "varchar_val"},
        // A nullable column.
        {.type = INT, .size = 4, .nullable = true, .name = "int_val_nullable"},
    };

    const char *testVarChar = "Hello, world";
    const char *tableName = "table_name";

    void initTable() {
        ASSERT_NO_THROW(table.create("tmp/table", tableName, columnMetas));
    }

    QueryBuilder getBaseBuilder() { return QueryBuilder(&table); }
};

TEST_F(QueryConditionTest, TestCompareInt) {
    Columns testColumns0 = {Column(1), Column(1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};
    Columns testColumns1 = {Column(5), Column(-1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};
    Columns testColumns2 = {Column(3), Column(1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));
    ASSERT_NO_THROW(table.insert(testColumns2));

    QueryBuilder::Result result;

    // EQ.
    int value = 1;
    QueryBuilder builder = getBaseBuilder();
    builder.condition(columnMetas[0].name, EQ, (const char *)(&value));

    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(result[0].second, testColumns0);

    // GT.
    value = 4;
    builder = getBaseBuilder();
    builder.condition(columnMetas[0].name, GT, (const char *)(&value));
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(result[0].second, testColumns1);

    // Emptyset.
    value = 10;
    builder = getBaseBuilder();
    builder.condition(columnMetas[0].name, GE, (const char *)(&value));
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 0);

    // Two constraints (GE, LT).
    int value1 = 3, value2 = 5;
    builder = getBaseBuilder();
    builder.condition(columnMetas[0].name, GE, (const char *)(&value1));
    builder.condition(columnMetas[0].name, LT, (const char *)(&value2));
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(result[0].second, testColumns2);
}

TEST_F(QueryConditionTest, TestCompareFloat) {
    Columns testColumns0 = {Column(0), Column(1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};
    Columns testColumns1 = {Column(0), Column(-1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));

    // EQ and GE.
    std::vector<std::tuple<CompareOp, float, Columns>> pairs = {
        std::make_tuple(EQ, atof("1.1"), testColumns0),
        std::make_tuple(GE, atof("1.1"), testColumns0),
        std::make_tuple(GE, atof("1.0"), testColumns0),
        std::make_tuple(EQ, atof("-1.1"), testColumns1),
        std::make_tuple(LE, atof("-1.1"), testColumns1),
        std::make_tuple(LE, atof("-1.0"), testColumns1),
        std::make_tuple(GT, atof("1.0"), testColumns0),
        std::make_tuple(LT, atof("-1.0"), testColumns1)};

    for (auto &pair : pairs) {
        float value = std::get<1>(pair);
        const Columns &testColumn = std::get<2>(pair);

        QueryBuilder builder = getBaseBuilder();
        builder.condition(columnMetas[1].name, std::get<0>(pair),
                          (const char *)(&value));

        QueryBuilder::Result result;
        ASSERT_NO_THROW(result = builder.execute());
        ASSERT_EQ(result.size(), 1);
        compareColumns(result[0].second, testColumn);
    }
}

TEST_F(QueryConditionTest, TestCompareVarchar) {
    Columns testColumns0 = {Column(1), Column(1.1F), Column("aaabbb", 100),
                            Column::nullIntColumn()};
    Columns testColumns1 = {Column(5), Column(-1.0F), Column("b", 100),
                            Column::nullIntColumn()};
    Columns testColumns2 = {Column(1), Column(1.1F), Column("aaaaaa", 100),
                            Column::nullIntColumn()};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));
    ASSERT_NO_THROW(table.insert(testColumns2));

    QueryBuilder::Result result;

    // EQ.
    char compareStr[100] = "aaabbb";

    QueryBuilder builder = getBaseBuilder();
    builder.condition(columnMetas[2].name, EQ, compareStr);
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(testColumns0, result[0].second);

    // GT.
    builder = getBaseBuilder();
    builder.condition(columnMetas[2].name, GT, compareStr);
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(testColumns1, result[0].second);

    // LT.
    builder = getBaseBuilder();
    builder.condition(columnMetas[2].name, LT, compareStr);
    ASSERT_NO_THROW(result = builder.execute());
    ASSERT_EQ(result.size(), 1);
    compareColumns(testColumns2, result[0].second);
}

TEST_F(QueryConditionTest, TestNullField) {
    int value = 1;

    QueryBuilder builder = getBaseBuilder();
    builder.condition(columnMetas[0].name, EQ, (const char *)(&value));
    builder.condition(columnMetas[3].name, EQ, (const char *)(&value));

    QueryBuilder::Result result;
    ASSERT_NO_THROW(result = builder.execute());
    EXPECT_EQ(result.size(), 0);
}

TEST_F(QueryConditionTest, TestNullOp) {
    Columns testColumns0 = {Column(1), Column(1.1F), Column(testVarChar, 100),
                            Column::nullIntColumn()};
    Columns testColumns1 = {Column(1), Column(1.1F), Column(testVarChar, 100),
                            Column(1)};

    ASSERT_NO_THROW(table.insert(testColumns0));
    ASSERT_NO_THROW(table.insert(testColumns1));

    QueryBuilder::Result result;

    QueryBuilder builder = getBaseBuilder();
    builder.nullCondition(columnMetas[3].name, true);
    ASSERT_NO_THROW(result = builder.execute());
    EXPECT_EQ(result.size(), 1);
    compareColumns(testColumns0, result[0].second);

    builder = getBaseBuilder();
    builder.nullCondition(columnMetas[3].name, false);
    ASSERT_NO_THROW(result = builder.execute());
    EXPECT_EQ(result.size(), 1);
    compareColumns(testColumns1, result[0].second);
}

TEST_F(QueryConditionTest, TestNullValue) {
    Columns testColumns0 = {Column(1), Column(1.1F),
                            Column::nullVarcharColumn(100), Column(2)};

    ASSERT_NO_THROW(table.insert(testColumns0));

    std::vector<std::pair<CompareOp, int>> testCases = {
        {EQ, 0}, {NE, 0}, {GT, 0}, {GE, 0}, {LT, 0}, {LE, 0}};

    QueryBuilder::Result result;

    for (auto &testCase : testCases) {
        QueryBuilder builder = getBaseBuilder();
        builder.condition(columnMetas[2].name, testCase.first, "");

        ASSERT_NO_THROW(result = builder.execute());
        EXPECT_EQ(result.size(), testCase.second);
    }
}

// TEST_F(QueryConditionTest, TestLikeOp) {
//     Columns testColumns0 = {Column(1), Column(1.1F), Column("123451", 100),
//                             Column::nullIntColumn()};
//     Columns testColumns1 = {Column(1), Column(1.1F), Column("012314", 100),
//                             Column(1)};

//     ASSERT_NO_THROW(table.insert(testColumns0));
//     ASSERT_NO_THROW(table.insert(testColumns1));

//     // Matching the first but not second.
//     const char *regex = "[1-9]+";

//     QueryBuilder::Result result;

//     QueryBuilder builder = getBaseBuilder();
//     builder.condition(columnMetas[2].name, LIKE, regex);

//     ASSERT_NO_THROW(result = builder.execute());
//     EXPECT_EQ(result.size(), 1);
//     compareColumns(testColumns0, result[0].second);

//     // Test invalid regex.
//     regex = "[1-9";
//     builder = getBaseBuilder();
//     builder.condition(columnMetas[2].name, LIKE, regex);
//     EXPECT_THROW(result = builder.execute(), Internal::InvalidRegexError);
// }

// TEST_F(QueryConditionTest, TestInvalidLikeOperator) {
//     Columns testColumns0 = {Column(1), Column(1.1F), Column(testVarChar,
//     100),
//                             Column::nullIntColumn()};
//     ASSERT_NO_THROW(table.insert(testColumns0));

//     QueryBuilder builder = getBaseBuilder();
//     builder.condition(columnMetas[0].name, LIKE, "[0-9]+");

//     EXPECT_THROW(auto result = builder.execute(),
//                  Internal::InvalidOperatorError);
// }
