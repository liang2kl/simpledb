#include "internal/ParseTreeVisitor.h"

#include <SimpleDB/Error.h>
#include <SimpleDB/internal/Table.h>

#include <cstdio>
#include <tuple>
#include <utility>
#include <vector>

#include "DBMS.h"
#include "internal/Logger.h"
#include "internal/ParseHelper.h"

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
    // dbms->describeTable(ctx->Identifier()->getText());
    return antlrcpp::Any();
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
            // FIXME: We just ignore the identifiers behind...
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

    column.nullable = ctx->Null() != nullptr;

    if (ctx->value() != nullptr) {
        ParseHelper::parseDefaultValue(ctx->value()->getText(), column.type,
                                       column.defaultValue, column.size);
    }

    return column;
}

}  // namespace Internal
}