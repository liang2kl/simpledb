#include "internal/ParseTreeVisitor.h"

#include <cstdio>
#include <tuple>
#include <utility>
#include <vector>

#include "DBMS.h"
#include "Error.h"
#include "internal/Column.h"
#include "internal/Logger.h"
#include "internal/ParseHelper.h"
#include "internal/Table.h"

using namespace SQLParser;
using namespace SimpleDB::Internal;
using namespace SimpleDB::Service;

namespace SimpleDB {
namespace Internal {

ParseTreeVisitor::ParseTreeVisitor(::SimpleDB::DBMS *dbms) : dbms(dbms) {}

antlrcpp::Any ParseTreeVisitor::visitProgram(SqlParser::ProgramContext *ctx) {
    std::vector<ExecutionResult> results;

    try {
        for (auto *stmt : ctx->statement()) {
            antlrcpp::Any result = stmt->accept(this);
            results.push_back(result.as<ExecutionResult>());
        }
    } catch (Error::ExecutionErrorBase &e) {
        // Normal exception.
        Logger::log(DEBUG_,
                    "ParseTreeVisitor: exception caught during execution: %s\n",
                    e.what());
        throw;

    } catch (Internal::InternalErrorBase &e) {
        // Uncaught internal exception.
        Logger::log(ERROR,
                    "ParseTreeVisitor: uncaught internal exception during "
                    "execution: %s\n",
                    e.what());
        throw Error::InternalError(e.what());

    } catch (std::exception &e) {
        // Unknown error.
        Logger::log(
            DEBUG_,
            "ParseTreeVisitor: unknown exception caught during execution: %s\n",
            e.what());
        throw Error::UnknownError(e.what());

    } catch (...) {
        // Unknown error.
        Logger::log(
            DEBUG_,
            "ParseTreeVisitor: unknown exception caught during execution");
        throw Error::UnknownError();
    }

    return results;
}

antlrcpp::Any ParseTreeVisitor::visitStatement(
    SqlParser::StatementContext *ctx) {
    if (ctx->alter_statement()) {
        return ctx->alter_statement()->accept(this);
    }
    if (ctx->db_statement()) {
        return ctx->db_statement()->accept(this);
    }
    if (ctx->io_statement()) {
        return ctx->io_statement()->accept(this);
    }
    if (ctx->table_statement()) {
        return ctx->table_statement()->accept(this);
    }

    PlainResult result;
    result.set_msg("No-op done.");
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitCreate_db(
    SqlParser::Create_dbContext *ctx) {
    visitChildren(ctx);

    PlainResult result = dbms->createDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitDrop_db(SqlParser::Drop_dbContext *ctx) {
    visitChildren(ctx);

    PlainResult result = dbms->dropDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitShow_dbs(SqlParser::Show_dbsContext *ctx) {
    ShowDatabasesResult result = dbms->showDatabases();
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitUse_db(SqlParser::Use_dbContext *ctx) {
    PlainResult result = dbms->useDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitShow_tables(
    SqlParser::Show_tablesContext *ctx) {
    ShowTableResult result = dbms->showTables();
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitCreate_table(
    SqlParser::Create_tableContext *ctx) {
    auto [columns, primaryKey, foreignKeys] =
        ctx->field_list()
            ->accept(this)
            .as<std::tuple<std::vector<ColumnMeta>, std::string,
                           std::vector<ForeignKey>>>();

    PlainResult result = dbms->createTable(ctx->Identifier()->getText(),
                                           columns, primaryKey, foreignKeys);

    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitDrop_table(
    SqlParser::Drop_tableContext *ctx) {
    PlainResult result = dbms->dropTable(ctx->Identifier()->getText());
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitDescribe_table(
    SqlParser::Describe_tableContext *ctx) {
    DescribeTableResult result =
        dbms->describeTable(ctx->Identifier()->getText());
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitField_list(
    SqlParser::Field_listContext *ctx) {
    std::vector<ColumnMeta> columns;
    std::string primaryKey;
    std::vector<ForeignKey> foreignKeys;

    for (auto field : ctx->field()) {
        if (dynamic_cast<SqlParser::Normal_fieldContext *>(field)) {
            columns.push_back(field->accept(this).as<ColumnMeta>());
        } else if (dynamic_cast<SqlParser::Primary_key_fieldContext *>(field)) {
            if (!primaryKey.empty()) {
                throw Error::MultiplePrimaryKeyError();
            }
            primaryKey =
                static_cast<SqlParser::Primary_key_fieldContext *>(field)
                    ->Identifier()
                    ->getText();
        } else if (dynamic_cast<SqlParser::Foreign_key_fieldContext *>(field)) {
            foreignKeys.push_back(field->accept(this).as<ForeignKey>());
        } else {
            assert(false);
        }
    }

    return std::make_tuple(columns, primaryKey, foreignKeys);
}

antlrcpp::Any ParseTreeVisitor::visitNormal_field(
    SqlParser::Normal_fieldContext *ctx) {
    ColumnMeta column;
    ParseHelper::parseName(ctx->Identifier()->getText(), column.name);
    column.type = ParseHelper::parseDataType(ctx->type_()->getText());

    if (column.type == VARCHAR) {
        assert(ctx->type_()->Integer() != nullptr);
        column.size = ParseHelper::parseInt(ctx->type_()->Integer()->getText());
    } else {
        column.size = 4;
    }

    column.nullable = ctx->Null() == nullptr;

    if (ctx->value() != nullptr) {
        column.hasDefault = true;
        ParseHelper::parseDefaultValue(ctx->value()->getText(), column.type,
                                       column.defaultValue, column.size);
    } else {
        column.hasDefault = false;
    }

    return column;
}

antlrcpp::Any ParseTreeVisitor::visitAlter_table_add_pk(
    SqlParser::Alter_table_add_pkContext *ctx) {
    PlainResult result =
        dbms->alterPrimaryKey(ctx->Identifier(0)->getText(),
                              ctx->Identifier(1)->getText(), /*drop=*/false);
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitAlter_table_drop_pk(
    SqlParser::Alter_table_drop_pkContext *ctx) {
    PlainResult result = dbms->alterPrimaryKey(
        ctx->Identifier(0)->getText(),
        ctx->Identifier(1) == nullptr ? "" : ctx->Identifier(1)->getText(),
        /*drop=*/true);
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitAlter_add_index(
    SqlParser::Alter_add_indexContext *ctx) {
    PlainResult result;

    // We are not dealing with something like "... ADD INDEX (a, a)", which will
    // raise an exception for "index exists".

    const auto &tableName = ctx->Identifier()->getText();

    for (int i = 0; i < ctx->identifiers()->children.size(); i++) {
        const auto &columnName = ctx->identifiers()->Identifier(i)->getText();
        result = dbms->createIndex(tableName, columnName);
    }

    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitAlter_drop_index(
    SqlParser::Alter_drop_indexContext *ctx) {
    PlainResult result;

    const auto &tableName = ctx->Identifier()->getText();

    for (int i = 0; i < ctx->identifiers()->children.size(); i++) {
        const auto &columnName = ctx->identifiers()->Identifier(i)->getText();
        result = dbms->dropIndex(tableName, columnName);
    }

    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitShow_indexes(
    SQLParser::SqlParser::Show_indexesContext *ctx) {
    const auto &tableName = ctx->Identifier()->getText();

    ShowIndexesResult result = dbms->showIndexes(tableName);

    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitInsert_into_table(
    SQLParser::SqlParser::Insert_into_tableContext *ctx) {
    const std::string &tableName = ctx->Identifier()->getText();
    std::vector<Column> columns;
    ColumnBitmap emptyBits = 0;

    auto insertValues = ctx->insert_value_list()->insert_value();

    for (int i = 0; i < insertValues.size(); i++) {
        auto &value = insertValues[i];
        if (value->value() != nullptr) {
            columns.push_back(ParseHelper::parseColumnValue(value->value()));
        } else {
            emptyBits |= (1L << i);
        }
    }

    PlainResult result = dbms->insert(tableName, columns, emptyBits);
    return wrap(result);
}

}  // namespace Internal
}