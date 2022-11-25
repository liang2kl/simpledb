#include <SimpleDB/Error.h>

#include <tuple>
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
        {"t1", "CREATE TABLE t1 (c1 INT, c2 VARCHAR(10));"},
        {"t2", "CREATE TABLE t2 (c1 INT, c2 VARCHAR(10), c3 FLOAT);"},
        {"t3",
         "CREATE TABLE t3 (c1 INT NOT NULL, c2 VARCHAR(10), c3 FLOAT, PRIMARY "
         "KEY (c1));"},
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

    std::vector<std::string> failedCases = {
        /* Primary key failures */
        "CREATE TABLE t1 (c1 INT NOT NULL, PRIMARY KEY (c2));",
        "CREATE TABLE t1 (c1 FLOAT NOT NULL, PRIMARY KEY (c1));",
        "CREATE TABLE t1 (c1 INT, PRIMARY KEY (c1));",
    };

    for (auto testCase : failedCases) {
        ASSERT_THROW(results = executeSQL(testCase), BaseError)
            << "Test case: " << testCase;
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

TEST_F(DBMSTest, TestDescTable) {
    initDBMS();
    createAndUseDatabase();

    std::string tableName = "t";

    std::vector<std::tuple</*NAME=*/std::string, /*TYPE=*/std::string,
                           /*NOTNULL=*/bool, /*HASDEFAULT=*/bool,
                           /*DEFAULT=*/std::string>>
        testCases = {
            {"c1", "INT", true, true, "12"},
            {"c2", "VARCHAR(20)", false, true, "'test_string'"},
            {"c3", "FLOAT", true, true, "1.23"},
            {"c4", "INT", false, false, ""},
        };

    std::string sql = "CREATE TABLE " + tableName + " (";

    for (auto testCase : testCases) {
        auto [name, type, notNull, hasDefault, defaultValue] = testCase;
        sql += name + " " + type;
        if (notNull) {
            sql += " NOT NULL";
        }
        if (hasDefault) {
            sql += " DEFAULT " + defaultValue;
        }
        sql += ", ";
    }

    sql.pop_back();
    sql.pop_back();
    sql += ");";

    ASSERT_NO_THROW(executeSQL(sql));

    std::vector<Service::ExecutionResult> results;

    ASSERT_NO_THROW(results = executeSQL("DESC " + tableName + ";"));
    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results[0].has_describe_table());
    ASSERT_TRUE(results[0].describe_table().columns_size() == testCases.size());

    for (int i = 0; i < testCases.size(); i++) {
        auto [name, type, notNull, hasDefault, defaultValue] = testCases[i];
        auto column = results[0].describe_table().columns(i);
        EXPECT_EQ(column.field(), name);
        EXPECT_EQ(column.type(), type);
        EXPECT_EQ(column.nullable(), !notNull);
        EXPECT_EQ(column.has_default_value(), hasDefault);
        if (hasDefault) {
            if (type == "FLOAT") {
                EXPECT_EQ(std::stof(column.default_value()),
                          std::stof(defaultValue));
            } else if (type == "INT") {
                EXPECT_EQ(column.default_value(), defaultValue);
            } else {
                EXPECT_EQ(column.default_value(),
                          defaultValue.substr(1, defaultValue.size() - 2));
            }
        }
    }
}

TEST_F(DBMSTest, TestAddDropPrimaryKey) {
    initDBMS();
    createAndUseDatabase();

    std::string sql =
        "CREATE TABLE t1 (c1 INT NOT NULL, c2 INT NOT NULL, c3 INT, PRIMARY "
        "KEY (c1));";

    ASSERT_NO_THROW(executeSQL(sql));

    std::string dropSql = "ALTER TABLE t1 DROP PRIMARY KEY;";
    std::string addSql = "ALTER TABLE t1 ADD CONSTRAINT PRIMARY KEY (c2);";
    std::string addSql2 = "ALTER TABLE t1 ADD CONSTRAINT PRIMARY KEY (c3);";

    ASSERT_NO_THROW(executeSQL(dropSql));
    ASSERT_NO_THROW(executeSQL(addSql));
    ASSERT_THROW(executeSQL(addSql), Error::AlterPrimaryKeyError);
    ASSERT_NO_THROW(executeSQL(dropSql));
    ASSERT_THROW(executeSQL(addSql2), Error::AlterPrimaryKeyError);
}

TEST_F(DBMSTest, TestAddDropIndex) {
    initDBMS();
    createAndUseDatabase();

    std::string sql1 = "CREATE TABLE t1 (c1 INT NOT NULL);";
    std::string addIndexSql = "ALTER TABLE t1 ADD INDEX (c1);";
    std::string dropIndexSql = "ALTER TABLE t1 DROP INDEX (c1);";

    ASSERT_NO_THROW(executeSQL(sql1));
    ASSERT_NO_THROW(executeSQL(addIndexSql));
    ASSERT_THROW(executeSQL(addIndexSql), Error::AlterIndexError);
    ASSERT_NO_THROW(executeSQL(dropIndexSql));
    ASSERT_THROW(executeSQL(dropIndexSql), Error::AlterIndexError);
}

TEST_F(DBMSTest, TestPrimaryKeyIndex) {
    initDBMS();
    createAndUseDatabase();

    std::string sql1 = "CREATE TABLE t1 (c1 INT NOT NULL, PRIMARY KEY (c1));";
    std::string addIndexSql = "ALTER TABLE t1 ADD INDEX (c1);";
    std::string dropIndexSql = "ALTER TABLE t1 DROP INDEX (c1);";

    ASSERT_NO_THROW(executeSQL(sql1));
    auto resultRow = dbms.findIndex(testDbName, "t1", "c1").second;
    ASSERT_EQ(resultRow[3].data.intValue, 0 /*primary*/);
    ASSERT_THROW(executeSQL(addIndexSql), Error::AlterIndexError);
    ASSERT_THROW(executeSQL(dropIndexSql), Error::AlterIndexError);

    std::string sql2 = "CREATE TABLE t2 (c1 INT NOT NULL);";
    std::string addIndexSql2 = "ALTER TABLE t2 ADD INDEX (c1);";
    std::string addPkSql2 = "ALTER TABLE t2 ADD CONSTRAINT PRIMARY KEY (c1);";
    std::string dropIndexSql2 = "ALTER TABLE t2 DROP INDEX (c1);";

    ASSERT_NO_THROW(executeSQL(sql2));

    ASSERT_NO_THROW(executeSQL(addIndexSql2));
    resultRow = dbms.findIndex(testDbName, "t2", "c1").second;
    ASSERT_EQ(resultRow[3].data.intValue, 1 /*ordinary*/);

    ASSERT_NO_THROW(executeSQL(addPkSql2));
    resultRow = dbms.findIndex(testDbName, "t2", "c1").second;
    ASSERT_EQ(resultRow[3].data.intValue, 2 /*ord -> pri*/);

    ASSERT_NO_THROW(executeSQL(dropIndexSql2));
    resultRow = dbms.findIndex(testDbName, "t2", "c1").second;
    ASSERT_EQ(resultRow[3].data.intValue, 0 /*pri*/);
}

TEST_F(DBMSTest, TestAddIndexOnDuplicateColumn) {
    // TODO
}

TEST_F(DBMSTest, TestInsertRecord) {
    initDBMS();
    createAndUseDatabase();

    std::string createTableSql = "CREATE TABLE t1 (c1 INT NOT NULL);";
    std::string insertSql = "INSERT INTO t1 VALUES (1);";
    std::string addIndexSql = "ALTER TABLE t1 ADD INDEX (c1);";

    ASSERT_NO_THROW(executeSQL(createTableSql));
    ASSERT_NO_THROW(executeSQL(insertSql));
    ASSERT_NO_THROW(executeSQL(addIndexSql));

    // Test default value.
    std::string createTableSql2 =
        "CREATE TABLE t2 (c1 INT NOT NULL DEFAULT 1);";
    std::string insertSql2 = "INSERT INTO t2 VALUES (DEFAULT);";
    std::string addIndexSql2 = "ALTER TABLE t2 ADD INDEX (c1);";

    ASSERT_NO_THROW(executeSQL(createTableSql2));
    ASSERT_NO_THROW(executeSQL(addIndexSql2));
    ASSERT_NO_THROW(executeSQL(insertSql2));

    // Test null value.
    std::string createTableSql3 = "CREATE TABLE t3 (c1 INT);";
    std::string insertSql3 = "INSERT INTO t3 VALUES (NULL);";

    ASSERT_NO_THROW(executeSQL(createTableSql3));
    ASSERT_NO_THROW(executeSQL(insertSql3));

    // Test insert into table with primary key.
    std::string createTableSql4 =
        "CREATE TABLE t4 (c1 INT NOT NULL, PRIMARY KEY (c1));";
    std::string insertSql4 = "INSERT INTO t4 VALUES (1);";

    ASSERT_NO_THROW(executeSQL(createTableSql4));
    ASSERT_NO_THROW(executeSQL(insertSql4));
    ASSERT_THROW(executeSQL(insertSql4), Error::InsertError);
}