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

    const std::string testDbName = "db";

    void initDBMS() { dbms.init(); }
    void createAndUseDatabase() {
        ASSERT_NO_THROW(dbms.createDatabase(testDbName));
        ASSERT_NO_THROW(dbms.useDatabase(testDbName));
    }

    std::vector<Service::ExecutionResult> executeSQL(const std::string &sql) {
        auto stream = getStream(sql);
        return dbms.executeSQL(stream);
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

TEST_F(DBMSTest, TestCreateDropDatabase) {
    initDBMS();
    std::vector<std::string> testCases = {
        "DATABASE db1;",
        "DATABASE db2;",
        "DATABASE db3;",
    };

    for (auto testCase : testCases) {
        ASSERT_NO_THROW(executeSQL("CREATE " + testCase));
    }

    for (auto testCase : testCases) {
        ASSERT_THROW(executeSQL("CREATE " + testCase),
                     Error::DatabaseExistsError);
        ASSERT_NO_THROW(executeSQL("DROP " + testCase));
        ASSERT_THROW(executeSQL("DROP " + testCase),
                     Error::DatabaseNotExistError);
    }
}

TEST_F(DBMSTest, TestShowDatabases) {
    initDBMS();

    std::vector<std::string> testCases = {
        "db1",
        "db2",
        "db3",
    };

    for (auto testCase : testCases) {
        ASSERT_NO_THROW(executeSQL("CREATE DATABASE " + testCase + ";"));
    }

    std::vector<Service::ExecutionResult> results;

    ASSERT_NO_THROW(results = executeSQL("SHOW DATABASES;"));
    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results[0].has_show_databases());
    ASSERT_EQ(results[0].show_databases().databases_size(), testCases.size());

    for (int i = 0; i < testCases.size(); i++) {
        // The order is not necessarily the same, but it is actually the same in
        // our implementation.
        ASSERT_EQ(results[0].show_databases().databases(i), testCases[i]);
    }
}

TEST_F(DBMSTest, TestCreateDropTable) {
    initDBMS();
    createAndUseDatabase();

    // dbms.useDatabase(const std::string &dbName)
    std::vector<std::pair<std::string, std::string>> successCases = {
        {"t2", "CREATE TABLE t2 (c1 INT, c2 VARCHAR(10));"},
        {"t3", "CREATE TABLE t3 (c1 INT, c2 VARCHAR(10), c3 FLOAT);"},
    };

    std::vector<Service::ExecutionResult> results;

    for (auto testCase : successCases) {
        auto [tableName, sql] = testCase;
        ASSERT_NO_THROW(results = executeSQL(sql)) << "Test case: " << sql;
        ASSERT_EQ(results.size(), 1);
        EXPECT_TRUE(results[0].has_plain());

        auto dropStatement = std::string("DROP TABLE ") + tableName + ";";
        ASSERT_NO_THROW(results = executeSQL(dropStatement));
        ASSERT_THROW(results = executeSQL(dropStatement),
                     Error::TableNotExistsError);
    }
}

TEST_F(DBMSTest, TestShowTables) {
    initDBMS();
    createAndUseDatabase();

    ASSERT_NO_THROW(executeSQL("CREATE TABLE t1 (c1 INT, c2 VARCHAR(10));"));
    ASSERT_NO_THROW(executeSQL("CREATE TABLE t2 (c1 INT, c2 VARCHAR(10));"));

    std::vector<Service::ExecutionResult> results;

    ASSERT_NO_THROW(results = executeSQL("SHOW TABLES;"));

    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results[0].has_show_table());
    ASSERT_TRUE(results[0].show_table().tables().size() == 2);
    ASSERT_TRUE(results[0].show_table().tables(0) == "t1");
    ASSERT_TRUE(results[0].show_table().tables(1) == "t2");
}
