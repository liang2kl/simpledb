#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <filesystem>
#include <map>
#include <string>

#include "SQLParser/SqlParser.h"
#include "internal/ParseTreeVisitor.h"
#include "internal/Service.h"
#include "internal/Table.h"

/** File structures
 *  - /system: System tables
 *      - /system/databases: Database information
 *  - /<db-name>: Database
 *      - /<db-name>/<table-name>: Table
 */

namespace SimpleDB {

class DBMS {
    friend class Internal::ParseTreeVisitor;

public:
    DBMS(const std::string &rootPath);
    ~DBMS();

    void init();

    // Execute one or more SQL statement(s). No results will be returned if one
    // of the statements has failed even if the effects have taken place, for
    // the sheer simplicity.
    // @param stream The input stream to read the SQL statement from.
    // @throw Error::ExecutionErrorBase
    std::vector<Service::ExecutionResult> executeSQL(std::istream &stream);
#if !TESTING
private:
#endif
    std::filesystem::path rootPath;
    bool initialized = false;

    // === System Tables ===
    Internal::Table databaseSystemTable;

    Internal::ColumnMeta databaseSystemTableColumns[1] = {
        Internal::ColumnMeta{.type = Internal::DataType::VARCHAR,
                             .size = Internal::MAX_DATABASE_NAME_LEN,
                             .nullable = false,
                             .name = "name",
                             .hasDefault = false}};

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
        const std::vector<Internal::ColumnMeta> &columns);
    Service::PlainResult dropTable(const std::string &tableName);
    // void describeTable(const std::string &tableName);

    // === System tables ===
    void initDatabaseSystemTable();

    // === Helper methods ===
    std::string getTablePath(const std::string &tableName);
    std::string getDatabasePath(const std::string &tableName);
};

};  // namespace SimpleDB

#endif