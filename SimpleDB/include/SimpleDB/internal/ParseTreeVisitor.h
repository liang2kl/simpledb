#ifndef _SIMPLEDB_PARSE_TREE_VISITOR_H
#define _SIMPLEDB_PARSE_TREE_VISITOR_H

#include <SQLParser/SqlBaseVisitor.h>

namespace SimpleDB {

class DBMS;

namespace Internal {

class ParseTreeVisitor : public SQLParser::SqlBaseVisitor {
public:
    ParseTreeVisitor() = default;
    ParseTreeVisitor(::SimpleDB::DBMS *dbms);

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

private:
    DBMS *dbms;
};

}  // namespace Internal
}  // namespace SimpleDB

#endif