#include "internal/ParseTreeVisitor.h"

#include <SimpleDB/Error.h>
#include <SimpleDB/internal/Table.h>

#include <cstdio>
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

std::any ParseTreeVisitor::visitProgram(SqlParser::ProgramContext *ctx) {
    std::vector<ExecutionResult> results;

    try {
        for (auto *stmt : ctx->statement()) {
            std::any result = stmt->accept(this);
            results.push_back(std::any_cast<ExecutionResult>(result));
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

std::any ParseTreeVisitor::visitStatement(SqlParser::StatementContext *ctx) {
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

std::any ParseTreeVisitor::visitCreate_db(SqlParser::Create_dbContext *ctx) {
    visitChildren(ctx);

    PlainResult result = dbms->createDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

std::any ParseTreeVisitor::visitDrop_db(SqlParser::Drop_dbContext *ctx) {
    visitChildren(ctx);

    PlainResult result = dbms->dropDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

std::any ParseTreeVisitor::visitShow_dbs(SqlParser::Show_dbsContext *ctx) {
    ShowDatabasesResult result = dbms->showDatabases();
    return wrap(result);
}

std::any ParseTreeVisitor::visitUse_db(SqlParser::Use_dbContext *ctx) {
    PlainResult result = dbms->useDatabase(ctx->Identifier()->getText());
    return wrap(result);
}

std::any ParseTreeVisitor::visitShow_tables(
    SqlParser::Show_tablesContext *ctx) {
    ShowTableResult result = dbms->showTables();
    return wrap(result);
}

std::any ParseTreeVisitor::visitCreate_table(
    SqlParser::Create_tableContext *ctx) {
    std::any results = ctx->field_list()->accept(this);

    std::vector<ColumnMeta> columns =
        std::any_cast<std::vector<ColumnMeta>>(results);
    PlainResult result =
        dbms->createTable(ctx->Identifier()->getText(), columns);

    return wrap(result);
}

std::any ParseTreeVisitor::visitDrop_table(SqlParser::Drop_tableContext *ctx) {
    PlainResult result = dbms->dropTable(ctx->Identifier()->getText());
    return wrap(result);
}

std::any ParseTreeVisitor::visitDescribe_table(
    SqlParser::Describe_tableContext *ctx) {
    // dbms->describeTable(ctx->Identifier()->getText());
    return std::any();
}

std::any ParseTreeVisitor::visitField_list(SqlParser::Field_listContext *ctx) {
    std::vector<ColumnMeta> columns;

    for (auto field : ctx->field()) {
        std::any result = field->accept(this);
        columns.push_back(std::any_cast<ColumnMeta>(result));
    }

    return columns;
}

std::any ParseTreeVisitor::visitNormal_field(
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