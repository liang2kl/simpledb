#include <SimpleDB/Error.h>
#ifndef TESTING
#define TESTING 1
#endif
#ifndef DEBUG
#define DEBUG 1
#endif
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

    DBMS dbms = DBMS("tmp");
    std::istringstream getStream(const std::string &sql) {
        return std::istringstream(sql);
    }

    void initDBMS() { dbms.init(); }
    void executeSQL(const std::string &sql) {
        auto stream = getStream(sql);
        dbms.executeSQL(stream);
    }
};

TEST_F(DBMSTest, TestInitDBMS) { ASSERT_NO_THROW(initDBMS()); }

TEST_F(DBMSTest, TestInvalidSQL) {
    initDBMS();
    std::vector<std::string> testCases = {
        "CREATE DATABASE;",
    };

    for (auto testCase : testCases) {
        EXPECT_THROW(executeSQL(testCase), Error::SyntaxError);
    }
}

TEST_F(DBMSTest, TestCreateDatabase) {
    initDBMS();
    std::vector<std::string> testCases = {
        "CREATE DATABASE db1;",
        "CREATE DATABASE db2;",
        "CREATE DATABASE db3;",
    };

    for (auto testCase : testCases) {
        ASSERT_NO_THROW(executeSQL(testCase));
    }

    for (auto testCase : testCases) {
        ASSERT_THROW(executeSQL(testCase), Error::DatabaseExistsError);
    }
}

// TEST_F(DBMSTest, TestCreateTable) {
//     std::vector<std::string> successCases = {
//         "CREATE TABLE t2 (c1 INT, c2 VARCHAR(10));",
//         "CREATE TABLE t3 (c1 INT, c2 VARCHAR(10), c3 FLOAT);",
//     };

//     std::vector<Service::ExecutionResult> results;

//     for (auto testCase : successCases) {
//         auto stream = getStream(testCase);
//         EXPECT_NO_THROW(results = dbms.executeSQL(stream))
//             << "Test case: " << stream.str();
//         EXPECT_EQ(results.size(), 1);
//         if (results.size() == 1) {
//             EXPECT_EQ(results[0].has_plain(), true);
//         }
//     }
// }