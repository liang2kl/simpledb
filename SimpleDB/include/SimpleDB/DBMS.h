#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <SQLParser/SqlParser.h>

#include <filesystem>
#include <map>
#include <string>

#include "internal/ParseTreeVisitor.h"
#include "internal/QueryBuilder.h"
#include "internal/Service.h"
#include "internal/Table.h"

/** File structures
 *  - /system: System tables
 *      - /databases: Database information
 *      - /tables: Table information
 *      - /indexes: Index information
 *  - /<db-name>: Database
 *      - /<table-name>: Table
 *  - /index: Index files
 *      - /<db-name>/<table-name>/<column>: index for column
 */

namespace SimpleDB {
class DBMS {
    friend class Internal::ParseTreeVisitor;

public:
    DBMS(const std::string &rootPath);
    DBMS() = default;
    ~DBMS();

    void init();
    void close();

    // Execute one or more SQL statement(s). No results will be returned if
    // one of the statements has failed even if the effects have taken
    // place, for the sheer simplicity.
    // @param stream The input stream to read the SQL statement from.
    // @throw Error::ExecutionErrorBase
    std::vector<Service::ExecutionResult> executeSQL(std::istream &stream);
#if !TESTING
private:
#endif
    std::filesystem::path rootPath;
    std::string currentDatabase;
    bool initialized = false;

    // === System Tables ===
    Internal::Table systemDatabaseTable;
    Internal::Table systemTablesTable;
    Internal::Table systemIndexesTable;

    static std::vector<Internal::ColumnMeta> systemDatabaseTableColumns;
    static std::vector<Internal::ColumnMeta> systemTablesTableColumns;
    static std::vector<Internal::ColumnMeta> systemIndexesTableColumns;

    Internal::ParseTreeVisitor visitor;
    std::map<std::string, Internal::Table *> openedTables;

    // === Database management methods ===
    Service::PlainResult createDatabase(const std::string &dbName);
    Service::PlainResult dropDatabase(const std::string &dbName);
    Service::PlainResult useDatabase(const std::string &dbName);
    Service::ShowDatabasesResult showDatabases();

    Service::ShowTableResult showTables();
    Service::PlainResult createTable(
        const std::string &tableName,
        const std::vector<Internal::ColumnMeta> &columns,
        const std::string &primaryKey,
        const std::vector<Internal::ForeignKey> &foreignKeys);
    Service::PlainResult dropTable(const std::string &tableName);
    Service::DescribeTableResult describeTable(const std::string &tableName);

    Service::PlainResult alterPrimaryKey(const std::string &tableName,
                                         const std::string &primaryKey,
                                         bool drop);
    Service::PlainResult createIndex(const std::string &tableName,
                                     const std::string &columnName);
    Service::PlainResult dropIndex(const std::string &tableName,
                                   const std::string &columnName);
    Service::ShowIndexesResult showIndexes(const std::string &tableName);

    // === System tables ===
    void initSystemTable(Internal::Table *table, const std::string &name,
                         const std::vector<Internal::ColumnMeta> columns);

    // === Helper methods ===
    void checkUseDatabase();
    void clearCurrentDatabase();

    std::filesystem::path getSystemTablePath(const std::string &name);
    std::filesystem::path getUserTablePath(const std::string database,
                                           const std::string &name);
    std::filesystem::path getIndexPath(const std::string &database,
                                       const std::string &table,
                                       const std::string &column);

    Service::PlainResult makePlainResult(const std::string &msg);

    std::pair<Internal::RecordID, Internal::Columns> findDatabase(
        const std::string &dbName);
    std::pair<Internal::RecordID, Internal::Columns> findTable(
        const std::string &database, const std::string &tableName);
    Internal::QueryBuilder::Result findAllTables(const std::string &database);
    std::pair<Internal::RecordID, Internal::Columns> findIndex(
        const std::string &database, const std::string &table,
        const std::string &columnName);
    Internal::QueryBuilder::Result findIndexes(const std::string &database,
                                               const std::string &table);
    std::pair<Internal::RecordID, Internal::Table *> getTable(
        const std::string &tableName);
};

};  // namespace SimpleDB

#endif