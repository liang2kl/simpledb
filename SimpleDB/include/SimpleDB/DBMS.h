#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <SQLParser/SqlParser.h>

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "internal/IndexedTable.h"
#include "internal/ParseTreeVisitor.h"
#include "internal/QueryBuilder.h"
#include "internal/Table.h"
// Keep last...
#include "internal/Service.h"

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
    std::string getCurrentDatabase() const { return currentDatabase; }
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
    Internal::Table systemForeignKeyTable;

    static std::vector<Internal::ColumnMeta> systemDatabaseTableColumns;
    static std::vector<Internal::ColumnMeta> systemTablesTableColumns;
    static std::vector<Internal::ColumnMeta> systemIndexesTableColumns;
    static std::vector<Internal::ColumnMeta> systemForeignKeyTableColumns;

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
        const std::string &primaryKey = std::string(),
        const std::vector<Internal::ForeignKey> &foreignKeys = {});
    Service::PlainResult dropTable(const std::string &tableName);
    Service::DescribeTableResult describeTable(const std::string &tableName);

    Service::PlainResult alterPrimaryKey(const std::string &tableName,
                                         const std::string &primaryKey,
                                         bool drop);
    Service::PlainResult addForeignKey(const std::string &tableName,
                                       const std::string &column,
                                       const std::string &refTable,
                                       const std::string &refColumn);
    Service::PlainResult dropForeignKey(const std::string &tableName,
                                        const std::string &column);
    Service::PlainResult createIndex(const std::string &tableName,
                                     const std::string &columnName,
                                     bool isPrimaryKey = false);
    Service::PlainResult dropIndex(const std::string &tableName,
                                   const std::string &columnName,
                                   bool isPrimaryKey = false);
    Service::ShowIndexesResult showIndexes(const std::string &tableName);

    // === CURD methods ===
    Service::PlainResult insert(const std::string &tableName,
                                const std::vector<Internal::Column> &values,
                                Internal::ColumnBitmap emptyBits);
    Service::PlainResult update(Internal::QueryBuilder &builder,
                                const std::vector<std::string> &columnNames,
                                const Internal::Columns &columns);
    Service::PlainResult delete_(Internal::QueryBuilder &builder);
    Service::QueryResult select(Internal::QueryBuilder &builder);

    Internal::QueryBuilder buildQuery(
        const std::vector<std::string> &tables,
        const std::vector<Internal::QuerySelector> &selectors,
        const std::vector<Internal::CompareValueCondition> &valueConditions,
        const std::vector<Internal::CompareColumnCondition> &columnConditions,
        const std::vector<Internal::CompareNullCondition> &nullConditions,
        int limit, int offset, bool updateOrDelete = false);

    Internal::QueryBuilder buildQueryForUpdateOrDelete(
        const std::string &table,
        const std::vector<Internal::CompareValueCondition> &valueConditions,
        const std::vector<Internal::CompareColumnCondition> &columnConditions,
        const std::vector<Internal::CompareNullCondition> &nullConditions);

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

    Service::PlainResult makePlainResult(const std::string &msg,
                                         int affectedRows = -1);

    struct ForeignKeyInfo {
        Internal::RecordID rid;
        std::string database;
        std::string table;
        std::string column;
        std::string refTable;
        std::string refColumn;
    };
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
    std::vector<ForeignKeyInfo> findForeignKeys(const std::string &database,
                                                const std::string &table,
                                                const std::string &column,
                                                const std::string &refTable,
                                                const std::string &refColumn);
    bool hasReferencingRecord(
        const Internal::Table *oriTable,
        const std::vector<ForeignKeyInfo> &referencingColumns,
        const Internal::Columns &allNewColumns);
    std::pair<Internal::RecordID, Internal::Table *> getTable(
        const std::string &tableName);
    std::pair<Internal::RecordID, std::shared_ptr<Internal::Index>> getIndex(
        const std::string &database, const std::string &table,
        const std::string &column);
    std::shared_ptr<Internal::IndexedTable> newIndexedTable(
        Internal::Table *table);
};

};  // namespace SimpleDB

#endif