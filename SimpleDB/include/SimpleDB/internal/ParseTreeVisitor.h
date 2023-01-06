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

    virtual antlrcpp::Any visitProgram(
        SQLParser::SqlParser::ProgramContext *ctx) override;
    virtual antlrcpp::Any visitStatement(
        SQLParser::SqlParser::StatementContext *ctx) override;
    virtual antlrcpp::Any visitCreate_db(
        SQLParser::SqlParser::Create_dbContext *ctx) override;
    virtual antlrcpp::Any visitDrop_db(
        SQLParser::SqlParser::Drop_dbContext *ctx) override;
    virtual antlrcpp::Any visitShow_dbs(
        SQLParser::SqlParser::Show_dbsContext *ctx) override;
    virtual antlrcpp::Any visitUse_db(
        SQLParser::SqlParser::Use_dbContext *ctx) override;
    virtual antlrcpp::Any visitShow_tables(
        SQLParser::SqlParser::Show_tablesContext *ctx) override;
    virtual antlrcpp::Any visitCreate_table(
        SQLParser::SqlParser::Create_tableContext *ctx) override;
    virtual antlrcpp::Any visitDrop_table(
        SQLParser::SqlParser::Drop_tableContext *ctx) override;
    virtual antlrcpp::Any visitDescribe_table(
        SQLParser::SqlParser::Describe_tableContext *ctx) override;

    virtual antlrcpp::Any visitField_list(
        SQLParser::SqlParser::Field_listContext *ctx) override;
    virtual antlrcpp::Any visitNormal_field(
        SQLParser::SqlParser::Normal_fieldContext *ctx) override;

    virtual antlrcpp::Any visitAlter_table_drop_pk(
        SQLParser::SqlParser::Alter_table_drop_pkContext *ctx) override;
    virtual antlrcpp::Any visitAlter_table_add_pk(
        SQLParser::SqlParser::Alter_table_add_pkContext *ctx) override;
    virtual antlrcpp::Any visitAlter_table_drop_foreign_key(
        SQLParser::SqlParser::Alter_table_drop_foreign_keyContext *ctx)
        override;
    virtual antlrcpp::Any visitAlter_table_add_foreign_key(
        SQLParser::SqlParser::Alter_table_add_foreign_keyContext *ctx) override;
    virtual antlrcpp::Any visitAlter_add_index(
        SQLParser::SqlParser::Alter_add_indexContext *ctx) override;
    virtual antlrcpp::Any visitAlter_drop_index(
        SQLParser::SqlParser::Alter_drop_indexContext *ctx) override;
    virtual antlrcpp::Any visitShow_indexes(
        SQLParser::SqlParser::Show_indexesContext *ctx) override;

    virtual antlrcpp::Any visitWhere_and_clause(
        SQLParser::SqlParser::Where_and_clauseContext *ctx) override;
    virtual antlrcpp::Any visitWhere_operator_expression(
        SQLParser::SqlParser::Where_operator_expressionContext *ctx) override;
    virtual antlrcpp::Any visitWhere_null(
        SQLParser::SqlParser::Where_nullContext *ctx) override;
    virtual antlrcpp::Any visitColumn(
        SQLParser::SqlParser::ColumnContext *ctx) override;

    virtual antlrcpp::Any visitInsert_into_table(
        SQLParser::SqlParser::Insert_into_tableContext *ctx) override;
    virtual antlrcpp::Any visitSelect_table_(
        SQLParser::SqlParser::Select_table_Context *ctx) override;
    virtual antlrcpp::Any visitUpdate_table(
        SQLParser::SqlParser::Update_tableContext *ctx) override;
    virtual antlrcpp::Any visitDelete_from_table(
        SQLParser::SqlParser::Delete_from_tableContext *ctx) override;

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
    DECLARE_WRAPPER(DescribeTable, describe_table);
    DECLARE_WRAPPER(ShowIndexes, show_indexes);
    DECLARE_WRAPPER(Query, query);

#undef DECLARE_WRAPPER
};

}  // namespace Internal
}  // namespace SimpleDB

#endif