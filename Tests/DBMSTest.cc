#define TESTING
#define _DEBUG
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <vector>

using namespace SimpleDB;

class DBMSTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    DBMS dbms;
    std::istringstream getStream(const std::string &sql) {
        return std::istringstream(sql);
    }
};

TEST_F(DBMSTest, TestInvalidSQL) {
    std::vector<std::string> testCases = {
        "CREATE DATABASE;",
    };

    for (auto testCase : testCases) {
        auto stream = getStream(testCase);
        EXPECT_THROW(dbms.executeSQL(stream), Error::SyntaxError);
    }
}

// TEST_F(DBMSTest, TestCreateDatabase) {
//     std::vector<std::string> testCases = {
//         "CREATE DATABASE db1;",
//         "CREATE DATABASE db2;",
//         "CREATE DATABASE db3;",
//     };

//     for (auto testCase : testCases) {
//         auto stream = getStream(testCase);
//         ASSERT_NO_THROW(dbms.executeSQL(stream));
//     }
// }

TEST_F(DBMSTest, TestCreateTable) {
    std::vector<std::string> successCases = {
        "CREATE TABLE t2 (c1 INT, c2 VARCHAR(10));",
        "CREATE TABLE t3 (c1 INT, c2 VARCHAR(10), c3 FLOAT);",
    };

    for (auto testCase : successCases) {
        auto stream = getStream(testCase);
        EXPECT_NO_THROW(dbms.executeSQL(stream))
            << "Test case: " << stream.str();
    }
}