#include "internal/ParseTreeVisitor.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
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
            foreignKeys.push_back(ForeignKey{});
            ForeignKey &fk = foreignKeys.back();

            auto *fkField =
                static_cast<SqlParser::Foreign_key_fieldContext *>(field);
            fk.name = fkField->Identifier(0)->getText();
            fk.table = fkField->Identifier(1)->getText();
            fk.ref = fkField->Identifier(2)->getText();
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

antlrcpp::Any ParseTreeVisitor::visitAlter_table_drop_foreign_key(
    SqlParser::Alter_table_drop_foreign_keyContext *ctx) {
    std::string tableName = ctx->Identifier(0)->getText();
    std::string columnName = ctx->Identifier(1)->getText();

    PlainResult result = dbms->dropForeignKey(tableName, columnName);
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitAlter_table_add_foreign_key(
    SqlParser::Alter_table_add_foreign_keyContext *ctx) {
    std::string tableName = ctx->Identifier(0)->getText();
    std::string columnName = ctx->Identifier(1)->getText();
    std::string refTableName = ctx->Identifier(2)->getText();
    std::string refColumnName = ctx->Identifier(3)->getText();

    PlainResult result =
        dbms->addForeignKey(tableName, columnName, refTableName, refColumnName);
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

struct _WhereExpressionResult {
    ColumnId lhs;
    ColumnId rhs;
    ColumnValue value;
    CompareOp op;
    bool isValueCondition;

    _WhereExpressionResult(){};
};

antlrcpp::Any ParseTreeVisitor::visitWhere_and_clause(
    SqlParser::Where_and_clauseContext *ctx) {
    std::vector<CompareValueCondition> conditions;
    std::vector<CompareNullCondition> nullConditions;
    std::vector<CompareColumnCondition> columnConditions;

    // TODO: Other clauses.

    for (auto &condition : ctx->where_clause()) {
        if (dynamic_cast<SQLParser::SqlParser::Where_operator_expressionContext
                             *>(condition)) {
            auto cond = condition->accept(this).as<_WhereExpressionResult>();
            if (cond.isValueCondition) {
                conditions.push_back(
                    CompareValueCondition(cond.lhs, cond.op, cond.value));
            } else {
                columnConditions.push_back(
                    CompareColumnCondition(cond.lhs, cond.op, cond.rhs));
            }
        } else if (dynamic_cast<SQLParser::SqlParser::Where_nullContext *>(
                       condition)) {
            auto nullCondition =
                condition->accept(this).as<CompareNullCondition>();
            nullConditions.push_back(nullCondition);
        } else {
            assert(false);
        }
    }

    return std::make_tuple(conditions, columnConditions, nullConditions);
}

antlrcpp::Any ParseTreeVisitor::visitWhere_operator_expression(
    SqlParser::Where_operator_expressionContext *ctx) {
    _WhereExpressionResult result;
    result.lhs = ctx->column()->accept(this).as<ColumnId>();
    result.op = ParseHelper::parseCompareOp(ctx->operator_()->getText());

    // TODO: Implement other comparisions.
    auto expressionNode = ctx->expression();
    if (expressionNode->value() != nullptr) {
        result.value =
            ParseHelper::parseColumnValue(ctx->expression()->value()).data;
        result.isValueCondition = true;
    } else if (expressionNode->column() != nullptr) {
        result.rhs = expressionNode->column()->accept(this).as<ColumnId>();
        result.isValueCondition = false;
    }

    return result;
}

antlrcpp::Any ParseTreeVisitor::visitWhere_null(
    SqlParser::Where_nullContext *ctx) {
    CompareNullCondition condition;
    condition.columnId = ctx->column()->accept(this).as<ColumnId>();
    condition.isNull = ctx->WhereNot() == nullptr;

    return condition;
}

antlrcpp::Any ParseTreeVisitor::visitColumn(
    SQLParser::SqlParser::ColumnContext *ctx) {
    ColumnId id;
    if (ctx->Identifier().size() == 2) {
        id.tableName = ctx->Identifier(0)->getText();
        id.columnName = ctx->Identifier(1)->getText();
    } else {
        id.columnName = ctx->Identifier(0)->getText();
    }
    return id;
}

using ConditionTuple = std::tuple<std::vector<CompareValueCondition>,
                                  std::vector<CompareColumnCondition>,
                                  std::vector<CompareNullCondition>>;

antlrcpp::Any ParseTreeVisitor::visitSelect_table_(
    SqlParser::Select_table_Context *ctx) {
    auto selectTable = ctx->select_table();
    ConditionTuple tuple;

    if (selectTable->where_and_clause() != nullptr) {
        tuple =
            selectTable->where_and_clause()->accept(this).as<ConditionTuple>();
    }

    auto &[valConds, colConds, nullConds] = tuple;

    std::vector<std::string> tableNames;
    for (const auto id : selectTable->identifiers()->Identifier()) {
        tableNames.push_back(id->getText());
    }

    // Get columns.
    std::vector<QuerySelector> selectors;
    bool hasAggregator = false;
    bool hasNonAggregator = false;

    for (const auto &selector : selectTable->selectors()->selector()) {
        if (selector->column() != nullptr &&
            selector->aggregator() == nullptr) {
            if (hasAggregator) {
                throw Error::SelectError(
                    "aggregated query cannot have non-aggregated column");
            }
            hasNonAggregator = true;

            selectors.push_back(
                {QuerySelector::COLUMN,
                 selector->column()->accept(this).as<ColumnId>()});
        } else {
            if (hasNonAggregator) {
                throw Error::SelectError(
                    "aggregated query cannot have non-aggregated column");
            }
            hasAggregator = true;

            if (selector->aggregator() != nullptr) {
                // COUNT, AVERAGE, MAX, MIN, SUM
                auto *aggregator = selector->aggregator();
                ColumnId columnId =
                    selector->column()->accept(this).as<ColumnId>();
                if (aggregator->Count() != nullptr) {
                    selectors.push_back({QuerySelector::COUNT_COL, columnId});
                } else if (aggregator->Sum() != nullptr) {
                    selectors.push_back({QuerySelector::SUM, columnId});
                } else if (aggregator->Average() != nullptr) {
                    selectors.push_back({QuerySelector::AVG, columnId});
                } else if (aggregator->Max() != nullptr) {
                    selectors.push_back({QuerySelector::MAX, columnId});
                } else if (aggregator->Min() != nullptr) {
                    selectors.push_back({QuerySelector::MIN, columnId});
                } else {
                    assert(false);
                }
            } else if (selector->Count() != nullptr) {
                // COUNT(*)
                selectors.push_back({QuerySelector::COUNT_STAR});
            } else {
                assert(false);
            }
        }
    }

    auto integers = selectTable->Integer();
    int limit = -1;

    if (integers.size() >= 1) {
        // Limit.
        limit = ParseHelper::parseInt(integers[0]->getText());
        limit = limit < 0 ? -1 : limit;
    }

    int offset = 0;
    if (integers.size() >= 2) {
        // Offset.
        offset = ParseHelper::parseInt(integers[1]->getText());
        if (offset < 0) {
            throw Error::IncompatableValueError("offset must be non-negative");
        }
    }

    QueryBuilder builder = dbms->buildQuery(tableNames, selectors, valConds,
                                            colConds, nullConds, limit, offset);

    QueryResult result = dbms->select(builder);
    return wrap(result);
}

antlrcpp::Any ParseTreeVisitor::visitUpdate_table(
    SqlParser::Update_tableContext *ctx) {
    ConditionTuple tuple;

    if (ctx->where_and_clause() != nullptr) {
        tuple = ctx->where_and_clause()->accept(this).as<ConditionTuple>();
    }

    auto &[valConds, colConds, nullConds] = tuple;

    std::string tableName = ctx->Identifier()->getText();

    QueryBuilder builder = dbms->buildQueryForUpdateOrDelete(
        tableName, valConds, colConds, nullConds);

    auto setClause = ctx->set_clause();
    int numColumn = setClause->Identifier().size();
    assert(numColumn == setClause->EqualOrAssign().size());

    std::vector<std::string> columnNames;
    Columns columns;
    for (int i = 0; i < numColumn; i++) {
        columnNames.push_back(setClause->Identifier(i)->getText());
        columns.push_back(ParseHelper::parseColumnValue(setClause->value(i)));
    }

    PlainResult res = dbms->update(builder, columnNames, columns);
    return wrap(res);
}

antlrcpp::Any ParseTreeVisitor::visitDelete_from_table(
    SqlParser::Delete_from_tableContext *ctx) {
    ConditionTuple tuple =
        ctx->where_and_clause()->accept(this).as<ConditionTuple>();

    auto &[valConds, colConds, nullConds] = tuple;

    std::string tableName = ctx->Identifier()->getText();

    QueryBuilder builder = dbms->buildQueryForUpdateOrDelete(
        tableName, valConds, colConds, nullConds);

    PlainResult res = dbms->delete_(builder);
    return wrap(res);
}

}  // namespace Internal
}