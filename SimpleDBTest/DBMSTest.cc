#include <SimpleDB/Error.h>

#include <string>
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
    };

    for (auto testCase : failedCases) {
        ASSERT_THROW(results = executeSQL(testCase), BaseError)
            << "Test case: " << testCase;
    }

    // Test dropping a referenced table.
    ASSERT_NO_THROW(executeSQL("CREATE TABLE t1 (c1 INT, PRIMARY KEY (c1));"));
    ASSERT_NO_THROW(
        executeSQL("CREATE TABLE t2 (c1 INT, FOREIGN KEY (c1) "
                   "REFERENCES t1(c1));"));
    ASSERT_THROW(executeSQL("DROP TABLE t1;"), Error::DropTableError);
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

    // Test foreign key constraint.
    ASSERT_NO_THROW(executeSQL(addSql));
    ASSERT_NO_THROW(
        executeSQL("CREATE TABLE t2 (c1 INT, FOREIGN KEY (c1) "
                   "REFERENCES t1(c2));"));
    ASSERT_THROW(executeSQL(dropSql), Error::AlterPrimaryKeyError);
}

TEST_F(DBMSTest, TestAddDropForeignKey) {
    initDBMS();
    createAndUseDatabase();

    std::string createSql1 = "CREATE TABLE t1 (c1 INT, PRIMARY KEY(c1));";
    std::string createSql2 = "CREATE TABLE t2 (c1 INT);";

    ASSERT_NO_THROW(executeSQL(createSql1));
    ASSERT_NO_THROW(executeSQL(createSql2));

    std::string addSql =
        "ALTER TABLE t2 ADD CONSTRAINT FOREIGN KEY (c1) "
        "REFERENCES t1(c1);";

    ASSERT_NO_THROW(executeSQL(addSql));
    ASSERT_THROW(executeSQL(addSql), Error::AlterForeignKeyError);

    std::string dropSql = "ALTER TABLE t2 DROP FOREIGN KEY (c1);";

    ASSERT_NO_THROW(executeSQL(dropSql));
    ASSERT_THROW(executeSQL(dropSql), Error::AlterForeignKeyError);
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

    // Test insert non-exist foreign key.
    std::string createTableSql5 =
        "CREATE TABLE t5 (c1 INT NOT NULL, FOREIGN KEY (c1) "
        "REFERENCES t4(c1));";
    std::string insertSql5 = "INSERT INTO t5 VALUES (-1);";

    ASSERT_NO_THROW(executeSQL(createTableSql5));
    ASSERT_THROW(executeSQL(insertSql5), Error::InsertError);
}

TEST_F(DBMSTest, TestSelect) {
    initDBMS();
    createAndUseDatabase();

    std::string createTableSql = "CREATE TABLE t1 (c1 INT NOT NULL, c2 FLOAT);";

    ASSERT_NO_THROW(executeSQL(createTableSql));

    int sum = 0, min = 0, max = 0, count = 0;
    float sumf = 0, minf = 0, maxf = 0;
    float avg = 0, avgf = 0;

    const int INSERT_NUM = 1000;

    // Insert 1000 records into the table.
    for (int i = 0; i < INSERT_NUM; i++) {
        std::string intVal = std::to_string(i);
        std::string floatVal = std::to_string(i * 1.0);
        std::string insertSql =
            "INSERT INTO t1 VALUES (" + intVal + "," + floatVal + ");";
        ASSERT_NO_THROW(executeSQL(insertSql));
        sum += i;
        sumf += i;
        min = std::min(i, min);
        max = std::max(i, max);
        minf = std::min((float)i, minf);
        maxf = std::max((float)i, maxf);
        count++;
    }

    avg = float(sum) / count;
    avgf = sumf / count;

    // Test select all.
    std::string selectSql = "SELECT * FROM t1;";
    auto result = executeSQL(selectSql);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0].query().columns_size(), 2);
    ASSERT_EQ(result[0].query().rows_size(), INSERT_NUM);

    // Test select column.
    std::string selectSql2 = "SELECT c1 FROM t1;";
    auto result2 = executeSQL(selectSql2);
    ASSERT_EQ(result2.size(), 1);
    ASSERT_EQ(result2[0].query().columns_size(), 1);
    ASSERT_EQ(result2[0].query().rows_size(), INSERT_NUM);

    // Test select column with table name.
    std::string selectSql21 = "SELECT t1.c1 FROM t1;";
    auto result21 = executeSQL(selectSql2);
    ASSERT_EQ(result21.size(), 1);
    ASSERT_EQ(result21[0].query().columns_size(), 1);
    ASSERT_EQ(result21[0].query().rows_size(), INSERT_NUM);

    // Test select with condition.
    std::string selectSql3 = "SELECT * FROM t1 WHERE c1 > 500 AND c1 < 700;";
    auto result3 = executeSQL(selectSql3);
    ASSERT_EQ(result3.size(), 1);
    ASSERT_EQ(result3[0].query().columns_size(), 2);
    ASSERT_EQ(result3[0].query().rows_size(), 199);

    // Test aggregators with non-null values.
    std::string selectSql4 =
        "SELECT SUM(c1), MIN(c1), MAX(c1), COUNT(c1), AVG(c1), SUM(c2), "
        "MIN(c2), MAX(c2), COUNT(c2), AVG(c2) FROM t1;";
    auto result4 = executeSQL(selectSql4);
    ASSERT_EQ(result4.size(), 1);
    ASSERT_EQ(result4[0].query().columns_size(), 10);
    auto &row = result4[0].query().rows(0);
    ASSERT_TRUE(row.values(0).has_int_value());
    ASSERT_TRUE(row.values(1).has_int_value());
    ASSERT_TRUE(row.values(2).has_int_value());
    ASSERT_TRUE(row.values(3).has_int_value());
    ASSERT_TRUE(row.values(4).has_float_value());
    ASSERT_TRUE(row.values(5).has_float_value());
    ASSERT_TRUE(row.values(6).has_float_value());
    ASSERT_TRUE(row.values(7).has_float_value());
    ASSERT_TRUE(row.values(8).has_int_value());
    ASSERT_TRUE(row.values(9).has_float_value());

    EXPECT_EQ(row.values(0).int_value(), sum);
    EXPECT_EQ(row.values(1).int_value(), min);
    EXPECT_EQ(row.values(2).int_value(), max);
    EXPECT_EQ(row.values(3).int_value(), count);
    EXPECT_EQ(row.values(4).float_value(), avg);
    EXPECT_EQ(row.values(5).float_value(), sumf);
    EXPECT_EQ(row.values(6).float_value(), minf);
    EXPECT_EQ(row.values(7).float_value(), maxf);
    EXPECT_EQ(row.values(8).int_value(), count);
    EXPECT_EQ(row.values(9).float_value(), avgf);

    // Insert several null values.
    for (int i = INSERT_NUM; i < INSERT_NUM + 10; i++) {
        std::string intVal = std::to_string(i);
        std::string insertSql = "INSERT INTO t1 VALUES (" + intVal + ", NULL);";
        ASSERT_NO_THROW(executeSQL(insertSql));
    }

    // Now count(f), minf, maxf, sumf and avgf should remain unchanged.
    auto result5 = executeSQL(selectSql4);
    ASSERT_EQ(result5.size(), 1);
    ASSERT_EQ(result5[0].query().columns_size(), 10);
    auto &row2 = result5[0].query().rows(0);

    ASSERT_TRUE(row2.values(5).has_float_value());
    ASSERT_TRUE(row2.values(6).has_float_value());
    ASSERT_TRUE(row2.values(7).has_float_value());
    ASSERT_TRUE(row2.values(8).has_int_value());
    ASSERT_TRUE(row2.values(9).has_float_value());

    EXPECT_EQ(row2.values(5).float_value(), sumf);
    EXPECT_EQ(row2.values(6).float_value(), minf);
    EXPECT_EQ(row2.values(7).float_value(), maxf);
    EXPECT_EQ(row2.values(8).int_value(), count);
    EXPECT_EQ(row2.values(9).float_value(), avgf);

    // Test aggregate on empty set.
    std::string selectSql6 =
        "SELECT SUM(c1), MIN(c1), MAX(c1), COUNT(c1), AVG(c1), COUNT(*) FROM "
        "t1 WHERE c1 < 0;";

    auto result6 = executeSQL(selectSql6);
    ASSERT_EQ(result6.size(), 1);
    ASSERT_EQ(result6[0].query().columns_size(), 6);
    auto &row3 = result6[0].query().rows(0);

    ASSERT_TRUE(row3.values(0).has_null_value());
    ASSERT_TRUE(row3.values(1).has_null_value());
    ASSERT_TRUE(row3.values(2).has_null_value());
    ASSERT_TRUE(row3.values(3).has_int_value());
    ASSERT_TRUE(row3.values(4).has_null_value());
    ASSERT_TRUE(row3.values(5).has_int_value());

    EXPECT_EQ(row3.values(3).int_value(), 0);
    EXPECT_EQ(row3.values(5).int_value(), 0);

    // TODO: Test aggregators with conditions.
}

TEST_F(DBMSTest, TestSelectMultipleTable) {
    initDBMS();
    createAndUseDatabase();

    // Create table t1.
    std::string createSql1 = "CREATE TABLE t1 (c1 INT, c2 FLOAT);";
    ASSERT_NO_THROW(executeSQL(createSql1));

    // Create table t2.
    std::string createSql2 = "CREATE TABLE t2 (c1 INT, c2 FLOAT);";
    ASSERT_NO_THROW(executeSQL(createSql2));

    // Insert 100 rows into t1.
    for (int i = 0; i < 100; i++) {
        std::string intVal = std::to_string(i);
        std::string floatVal = std::to_string(i * 1.0);
        std::string insertSql =
            "INSERT INTO t1 VALUES (" + intVal + ", " + floatVal + ");";
        ASSERT_NO_THROW(executeSQL(insertSql));
    }

    // Insert 100 rows into t2.
    for (int i = 0; i < 100; i++) {
        std::string intVal = std::to_string(i);
        std::string floatVal = std::to_string(i * 1.0);
        std::string insertSql =
            "INSERT INTO t2 VALUES (" + intVal + ", " + floatVal + ");";
        ASSERT_NO_THROW(executeSQL(insertSql));
    }

    // Select from multiple tables.
    std::string selectSql = "SELECT * FROM t1, t2;";
    auto result = executeSQL(selectSql);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0].query().columns_size(), 4);
    ASSERT_EQ(result[0].query().rows_size(), 100 * 100);

    std::string selectSql2 =
        "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c1;";
    auto result2 = executeSQL(selectSql2);
    ASSERT_EQ(result2.size(), 1);
    ASSERT_EQ(result2[0].query().columns_size(), 2);
    ASSERT_EQ(result2[0].query().rows_size(), 100);
}

TEST_F(DBMSTest, TestUpdate) {
    initDBMS();
    createAndUseDatabase();

    // Create table.
    std::string createSql1 = "CREATE TABLE t1 (c1 INT, PRIMARY KEY (c1));";
    ASSERT_NO_THROW(executeSQL(createSql1));

    // Create a table that reference t1.
    std::string createSql2 =
        "CREATE TABLE t2 (c1 INT, FOREIGN KEY (c1) REFERENCES t1(c1));";
    ASSERT_NO_THROW(executeSQL(createSql2));

    const int NUM_RECORDS = 200;
    // Insert NUM_RECORDS records, NUM_RECORDS-1 of which has reference.
    for (int i = 0; i < NUM_RECORDS; i++) {
        std::string intVal = std::to_string(i);
        std::string insertSql = "INSERT INTO t1 VALUES (" + intVal + ");";
        ASSERT_NO_THROW(executeSQL(insertSql));
        if (i > 0) {
            std::string insertSql2 = "INSERT INTO t2 VALUES (" + intVal + ");";
            ASSERT_NO_THROW(executeSQL(insertSql2));
        }
    }

    // Update t1 (the unreferenced).
    std::string updateSql =
        "UPDATE t1 SET c1 = " + std::to_string(NUM_RECORDS) + "WHERE c1 = 0;";
    // ASSERT_NO_THROW(executeSQL(updateSql));
    // FIXME: We do not allow this update.
    ASSERT_THROW(executeSQL(updateSql), Error::UpdateError);

    // Update t1 (the referenced).
    for (int i = 1; i < NUM_RECORDS; i++) {
        std::string updateSql =
            "UPDATE t1 SET c1 = " + std::to_string(NUM_RECORDS + i) +
            "WHERE c1 = " + std::to_string(i) + ";";
        ASSERT_THROW(executeSQL(updateSql), Error::UpdateError);
    }

    // Update t2.
    for (int i = 1; i < NUM_RECORDS; i++) {
        std::string updateSql =
            "UPDATE t2 SET c1 = " + std::to_string(NUM_RECORDS - i) +
            "WHERE c1 = " + std::to_string(i) + ";";
        ASSERT_NO_THROW(executeSQL(updateSql));
    }
    std::string invalidUpdateSql = "UPDATE t2 SET c1 = -1 WHERE c1 = 0;";
    ASSERT_THROW(executeSQL(invalidUpdateSql), Error::UpdateError);
}

TEST_F(DBMSTest, TestDelete) {
    initDBMS();
    createAndUseDatabase();

    // Create table.
    std::string createSql1 = "CREATE TABLE t1 (c1 INT, PRIMARY KEY (c1));";
    ASSERT_NO_THROW(executeSQL(createSql1));

    const int NUM_RECORDS = 200;
    auto insertRecords = [=]() {
        for (int i = 0; i < NUM_RECORDS; i++) {
            std::string intVal = std::to_string(i);
            std::string insertSql = "INSERT INTO t1 VALUES (" + intVal + ");";
            ASSERT_NO_THROW(executeSQL(insertSql));
        }
    };

    // Insert records.
    insertRecords();

    // Delete all records.
    std::string deleteSql = "DELETE FROM t1 WHERE c1 >= 0;";
    ASSERT_NO_THROW(executeSQL(deleteSql));

    // Insert records again.
    insertRecords();

    // Create a table that reference t1.
    std::string createSql2 =
        "CREATE TABLE t2 (c1 INT, FOREIGN KEY (c1) REFERENCES t1(c1));";
    ASSERT_NO_THROW(executeSQL(createSql2));

    // Try to delete a record -- simply FAIL.
    std::string deleteSql2 = "DELETE FROM t1 WHERE c1 = 0;";
    ASSERT_THROW(executeSQL(deleteSql2), Error::DeleteError);
}