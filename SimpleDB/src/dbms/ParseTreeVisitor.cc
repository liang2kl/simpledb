#include "internal/ParseTreeVisitor.h"

#include "DBMS.h"
#include "internal/ParseHelper.h"

using namespace SQLParser;
using namespace SimpleDB::Internal;

namespace SimpleDB {
namespace Internal {

ParseTreeVisitor::ParseTreeVisitor(::SimpleDB::DBMS *dbms) : dbms(dbms) {}

std::any ParseTreeVisitor::visitCreate_db(SqlParser::Create_dbContext *ctx) {
    visitChildren(ctx);

    dbms->createDatabase(ctx->Identifier()->getText());
    return std::any();
}

std::any ParseTreeVisitor::visitDrop_db(SqlParser::Drop_dbContext *ctx) {
    visitChildren(ctx);

    dbms->dropDatabase(ctx->Identifier()->getText());
    return std::any();
}

std::any ParseTreeVisitor::visitShow_dbs(SqlParser::Show_dbsContext *ctx) {
    visitChildren(ctx);

    dbms->showDatabases();
    return std::any();
}

std::any ParseTreeVisitor::visitUse_db(SqlParser::Use_dbContext *ctx) {
    visitChildren(ctx);

    dbms->useDatabase(ctx->Identifier()->getText());
    return std::any();
}

std::any ParseTreeVisitor::visitShow_tables(
    SqlParser::Show_tablesContext *ctx) {
    visitChildren(ctx);

    dbms->showTables();
    return std::any();
}

std::any ParseTreeVisitor::visitCreate_table(
    SqlParser::Create_tableContext *ctx) {
    visitChildren(ctx);

    auto *fieldList = ctx->field_list();

    SqlParser::Normal_fieldContext *normalField;
    SqlParser::Primary_key_fieldContext *primaryKeyField;
    SqlParser::Foreign_key_fieldContext *foreignKeyField;

    std::vector<ColumnMeta> columns;

    for (auto *field : fieldList->field()) {
        ColumnMeta column;

        if ((normalField =
                 dynamic_cast<SqlParser::Normal_fieldContext *>(field))) {
            ParseHelper::parseName(normalField->Identifier()->getText(),
                                   column.name);
            column.type =
                ParseHelper::parseDataType(normalField->type_()->getText());

            if (column.type == VARCHAR) {
                assert(normalField->type_()->Integer() != nullptr);
                column.size = ParseHelper::parseInt(
                    normalField->type_()->Integer()->getText());
            } else {
                column.size = 4;
            }

            column.nullable = normalField->Null() != nullptr;

            if (normalField->value() != nullptr) {
                ParseHelper::parseDefaultValue(normalField->value()->getText(),
                                               column.type, column.defaultValue,
                                               column.size);
            }
        } else if ((primaryKeyField =
                        dynamic_cast<SqlParser::Primary_key_fieldContext *>(
                            field))) {
            assert(false);
        } else if ((foreignKeyField =
                        dynamic_cast<SqlParser::Foreign_key_fieldContext *>(
                            field))) {
            assert(false);
        } else {
            assert(false);
        }

        columns.push_back(column);
    }

    dbms->createTable(ctx->Identifier()->getText(), columns);
    return std::any();
}

std::any ParseTreeVisitor::visitDrop_table(SqlParser::Drop_tableContext *ctx) {
    visitChildren(ctx);

    dbms->dropTable(ctx->Identifier()->getText());
    return std::any();
}

std::any ParseTreeVisitor::visitDescribe_table(
    SqlParser::Describe_tableContext *ctx) {
    visitChildren(ctx);

    // dbms->describeTable(ctx->Identifier()->getText());
    return std::any();
}

}  // namespace Internal
}