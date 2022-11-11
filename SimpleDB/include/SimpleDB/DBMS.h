#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <string>

#include "SQLParser/SqlParser.h"
#include "internal/ParseTreeVisitor.h"
#include "internal/Table.h"

namespace SimpleDB {

class DBMS {
    friend class Internal::ParseTreeVisitor;

public:
    DBMS();

    // Execute a SQL statement.
    // @param stream The input stream to read the SQL statement from.
    // @throw Error::ExecutionErrorBase
    void executeSQL(std::istream &stream);

private:
    Internal::ParseTreeVisitor visitor;

    void createDatabase(const std::string &dbName);
    void dropDatabase(const std::string &dbName);
    void useDatabase(const std::string &dbName);
    void showDatabases();

    void showTables();
    void createTable(const std::string &tableName,
                     const std::vector<Internal::ColumnMeta> &columns);
    void dropTable(const std::string &tableName);
    void describeTable(const std::string &tableName);
};

};  // namespace SimpleDB

#endif