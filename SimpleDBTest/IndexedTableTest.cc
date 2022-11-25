#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class IndexedTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory("tmp");
        srand(0);
    }
    void TearDown() override { std::filesystem::remove_all("tmp"); }
};

TEST_F(IndexedTableTest, TestRangeCollapse) {
    // TODO
}
