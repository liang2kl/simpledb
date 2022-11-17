#ifndef _SIMPLEDB_PARSE_TREE_VISITOR_H
#define _SIMPLEDB_PARSE_TREE_VISITOR_H

#include <any>

#include "SQLParser/SqlBaseVisitor.h"
#include "SimpleDBService/query.pb.h"

namespace SimpleDB {

class DBMS;

namespace Internal {

class ParseTreeVisitor : public SQLParser::SqlBaseVisitor {
public:
    ParseTreeVisitor() = default;
    ParseTreeVisitor(::SimpleDB::DBMS *dbms);

    virtual std::any visitProgram(
        SQLParser::SqlParser::ProgramContext *ctx) override;
    virtual std::any visitStatement(
        SQLParser::SqlParser::StatementContext *ctx) override;
    virtual std::any visitCreate_db(
        SQLParser::SqlParser::Create_dbContext *ctx) override;
    virtual std::any visitDrop_db(
        SQLParser::SqlParser::Drop_dbContext *ctx) override;
    virtual std::any visitShow_dbs(
        SQLParser::SqlParser::Show_dbsContext *ctx) override;
    virtual std::any visitUse_db(
        SQLParser::SqlParser::Use_dbContext *ctx) override;
    virtual std::any visitShow_tables(
        SQLParser::SqlParser::Show_tablesContext *ctx) override;
    virtual std::any visitCreate_table(
        SQLParser::SqlParser::Create_tableContext *ctx) override;
    virtual std::any visitDrop_table(
        SQLParser::SqlParser::Drop_tableContext *ctx) override;
    virtual std::any visitDescribe_table(
        SQLParser::SqlParser::Describe_tableContext *ctx) override;

    // @returns: std::vector<ColumnMeta>
    virtual std::any visitField_list(
        SQLParser::SqlParser::Field_listContext *ctx) override;
    virtual std::any visitNormal_field(
        SQLParser::SqlParser::Normal_fieldContext *ctx) override;

private:
    DBMS *dbms;

#define DECLARE_WRAPPER(upperName, lowerName)                                 \
    Service::ExecutionResult wrap(const Service::upperName##Result &result) { \
        Service::ExecutionResult execResult;                                  \
        execResult.mutable_##lowerName()->CopyFrom(result);                   \
        return execResult;                                                    \
    }

    DECLARE_WRAPPER(Plain, plain);
    DECLARE_WRAPPER(ShowDatabases, show_databases);
    DECLARE_WRAPPER(ShowTable, show_table);

#undef DECLARE_WRAPPER
};

}  // namespace Internal
}  // namespace SimpleDB

#endif