#ifndef _SIMPLEDB_DBMS_H
#define _SIMPLEDB_DBMS_H

#include <string>

#include "SQLParser/SqlParser.h"
#include "SimpleDBService/query.pb.h"
// Undef the evil PAGE_SIZE macro somewhere deep inside this header.
#ifdef PAGE_SIZE
#undef PAGE_SIZE
#endif

#include "internal/ParseTreeVisitor.h"
#include "internal/Table.h"

namespace SimpleDB {

class DBMS {
    friend class Internal::ParseTreeVisitor;

public:
    DBMS();

    // Execute one or more SQL statement(s). No results will be returned if one
    // of the statements has failed even if the effects have taken place, for
    // the sheer simplicity.
    // @param stream The input stream to read the SQL statement from.
    // @throw Error::ExecutionErrorBase
    std::vector<Service::ExecutionResult> executeSQL(std::istream &stream);

private:
    Internal::ParseTreeVisitor visitor;

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
};

};  // namespace SimpleDB

#endif