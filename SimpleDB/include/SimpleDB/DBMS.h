#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <SQLParser/SqlParser.h>

#include <string>

#include "Table.h"
#include "internal/ParseTreeVisitor.h"

namespace SimpleDB {

class DBMS {
    friend class ParseTreeVisitor;

public:
    DBMS();

    // Execute a SQL statement.
    // @param stream The input stream to read the SQL statement from.
    // @throw Error::ExecutionErrorBase
    void executeSQL(std::istream &stream);

private:
    ParseTreeVisitor visitor;
    SQLParser::SqlParser::ProgramContext *parse(std::istream &stream);

    void createDatabase(const std::string &dbName);
    void dropDatabase(const std::string &dbName);
    void useDatabase(const std::string &dbName);
    void showDatabases();

    void showTables();
    void createTable(const std::string &tableName,
                     const std::vector<ColumnMeta> &columns);
    void dropTable(const std::string &tableName);
    void describeTable(const std::string &tableName);
};

};  // namespace SimpleDB

#endif