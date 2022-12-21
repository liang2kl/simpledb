#include <SimpleDB/SimpleDB.h>
#include <SimpleDB/internal/Column.h>
#include <SimpleDB/internal/IndexedTable.h>
#include <SimpleDB/internal/Table.h>
#include <gtest/gtest.h>

#include <climits>
#include <memory>
#include <tuple>
#include <vector>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class IndexedTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory("tmp");
        table.create("test", "test", schema);
        index = std::make_shared<Index>();
        index->create("index");
        srand(0);
    }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    const std::string colName = "col";
    const std::vector<ColumnMeta> schema = {
        {.type = INT, .nullable = false, .name = "col"},
    };

    Table table;
    std::shared_ptr<Index> index;

    CompareValueCondition cond(CompareOp op, int i) {
        ColumnValue v;
        v.intValue = i;
        return CompareValueCondition({.columnName = colName}, op, v);
    }
};

TEST_F(IndexedTableTest, TestRangeCollapse) {
    using TestCase = std::tuple<std::vector<CompareValueCondition>,
                                std::vector<Index::Range>>;

    std::vector<TestCase> testCases = {
        {{cond(NE, 1), cond(GE, 0), cond(LE, 3)}, {{0, 0}, {2, 3}}},
        {{cond(NE, 1), cond(EQ, 1)}, {}},
        {{cond(GE, 0), cond(LE, 1), cond(NE, 0), cond(NE, 1)}, {}},
        {{cond(GE, 0), cond(LE, 4), cond(NE, 1), cond(NE, 3)},
         {{0, 0}, {2, 2}, {4, 4}}},
        {{cond(NE, 0)}, {{INT_MIN, -1}, {1, INT_MAX}}},
        {{cond(GE, 0), cond(NE, INT_MAX)}, {{0, INT_MAX - 1}}},
    };

    Table t;

    for (const auto &testCase : testCases) {
        auto &[conditions, expected] = testCase;
        IndexedTable t = IndexedTable(
            &table,
            [&](const std::string &, const std::string &) { return index; });
        for (const auto &condition : conditions) {
            bool accepted = t.acceptCondition(condition);
            ASSERT_TRUE(accepted);
        }

        t.collapseRanges();

        ASSERT_EQ(t.ranges.size(), expected.size());
        for (size_t i = 0; i < t.ranges.size(); i++) {
            EXPECT_EQ(t.ranges[i].first, expected[i].first);
            EXPECT_EQ(t.ranges[i].second, expected[i].second);
        }
    }
}