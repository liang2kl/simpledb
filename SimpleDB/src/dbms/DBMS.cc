#include <SQLParser/SqlLexer.h>

#include <any>
#include <bitset>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "Error.h"
#include "internal/Column.h"
#include "internal/Index.h"
#include "internal/JoinedTable.h"
#include "internal/Logger.h"
#include "internal/Macros.h"
#include "internal/ParseTreeVisitor.h"
#include "internal/QueryBuilder.h"
#include "internal/Table.h"

// Keep last...
#include "DBMS.h"

using namespace SQLParser;
using namespace SimpleDB::Internal;
using namespace SimpleDB::Service;

namespace SimpleDB {

class : public antlr4::BaseErrorListener {
    virtual void syntaxError(antlr4::Recognizer *recognizer,
                             antlr4::Token *offendingSymbol, size_t line,
                             size_t charPositionInLine, const std::string &msg,
                             std::exception_ptr e) override {
        throw Error::SyntaxError(msg);
    }
} static errorListener;

DBMS::DBMS(const std::string &rootPath) : rootPath(rootPath) {
    visitor = ParseTreeVisitor(this);
}

DBMS::~DBMS() { close(); }

void DBMS::init() {
    if (initialized) {
        return;
    }
    const std::filesystem::path systemPath = rootPath / "system";

    if (std::filesystem::exists(systemPath)) {
        if (!std::filesystem::is_directory(systemPath)) {
            throw Error::InitializationError(
                "The root path exists and is not a directory");
        }
    } else {
        try {
            std::filesystem::create_directories(systemPath);
        } catch (std::filesystem::filesystem_error &e) {
            throw Error::InitializationError(e.what());
        }
    }

    // Create or load system tables.
    initSystemTable(&systemDatabaseTable, "databases",
                    systemDatabaseTableColumns);
    initSystemTable(&systemTablesTable, "tables", systemTablesTableColumns);
    initSystemTable(&systemIndexesTable, "indexes", systemIndexesTableColumns);

    initialized = true;
}

void DBMS::close() {
    systemDatabaseTable.close();
    systemTablesTable.close();

    clearCurrentDatabase();
    initialized = false;
}

std::vector<Service::ExecutionResult> DBMS::executeSQL(std::istream &stream) {
    if (!initialized) {
        throw Error::UninitializedError();
    }

    antlr4::ANTLRInputStream input(stream);
    SqlLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    SqlParser parser(&tokens);

    parser.removeErrorListeners();
    parser.addErrorListener(&errorListener);

    auto program = parser.program();

    // The errors are handled in the visitor.
    return visitor.visitProgram(program)
        .as<std::vector<Service::ExecutionResult>>();
}

PlainResult DBMS::createDatabase(const std::string &dbName) {
    if (dbName.size() > MAX_DATABASE_NAME_LEN) {
        throw Error::InvalidDatabaseNameError("database name too long");
    }

    bool found = findDatabase(dbName).first != RecordID::NULL_RECORD;

    if (found) {
        throw Error::DatabaseExistsError(dbName);
    }

    // Create database directory.
    const std::filesystem::path dbPath = rootPath / dbName;
    if (!std::filesystem::is_directory(dbPath)) {
        try {
            std::filesystem::create_directories(dbPath);
        } catch (std::filesystem::filesystem_error &e) {
            throw Error::CreateDatabaseError(e.what());
        }
    }

    // Append to the system table.
    Columns columns = {Column(dbName.c_str(), MAX_VARCHAR_LEN)};
    systemDatabaseTable.insert(columns);

    return makePlainResult("OK");
}

PlainResult DBMS::dropDatabase(const std::string &dbName) {
    auto [id, columns] = findDatabase(dbName);
    if (id == RecordID::NULL_RECORD) {
        throw Error::DatabaseNotExistError(dbName);
    }

    QueryBuilder::Result allTables = findAllTables(dbName);
    for (const auto &result : allTables) {
        auto [recordId, _] = result;
        systemTablesTable.remove(recordId);
    }

    // If the database is currently opened, close it first.
    if (currentDatabase == dbName) {
        clearCurrentDatabase();
    }

    systemDatabaseTable.remove(id);
    std::filesystem::remove_all(rootPath / dbName);

    return makePlainResult("OK");
}

PlainResult DBMS::useDatabase(const std::string &dbName) {
    if (dbName == currentDatabase) {
        return makePlainResult("Already in database " + dbName);
    }
    bool found = findDatabase(dbName).first != RecordID::NULL_RECORD;
    if (!found) {
        throw Error::DatabaseNotExistError(dbName);
    }

    // Switch database.
    clearCurrentDatabase();
    currentDatabase = dbName;

    return makePlainResult("Switch to database " + dbName);
}

ShowDatabasesResult DBMS::showDatabases() {
    QueryBuilder builder(&systemDatabaseTable);

    QueryBuilder::Result queryResult = builder.execute();

    ShowDatabasesResult result;

    for (const auto &res : queryResult) {
        const auto &column = res.second[0];
        result.mutable_databases()->Add(column.data.stringValue);
    }

    return result;
}

PlainResult DBMS::createTable(const std::string &tableName,
                              const std::vector<ColumnMeta> &columns,
                              const std::string &primaryKey,
                              const std::vector<ForeignKey> &foreignKeys) {
    checkUseDatabase();

    if (tableName.size() > MAX_TABLE_NAME_LEN) {
        throw Error::InvalidTableNameError("table name is too long");
    }

    // Check if the table exists.
    bool exists =
        findTable(currentDatabase, tableName).first != RecordID::NULL_RECORD;

    if (exists) {
        throw Error::TableExistsError(tableName);
    }

    std::filesystem::path path = getUserTablePath(currentDatabase, tableName);
    Table *table = new Table();

    try {
        table->create(path, tableName, columns, primaryKey);
    } catch (Internal::TooManyColumnsError &e) {
        throw CreateTableError(e.what());
    } catch (Internal::InvalidPrimaryKeyError &e) {
        throw CreateTableError(e.what());
    } catch (Internal::CreateTableError &e) {
        throw CreateTableError(e.what());
    } catch (Internal::InvalidColumnSizeError &e) {
        throw CreateTableError(e.what());
    } catch (Internal::DuplicateColumnNameError &e) {
        throw CreateTableError(e.what());
    }

    openedTables[tableName] = table;

    // Append to the system table.
    systemTablesTable.insert(
        {Column(tableName.c_str(), MAX_TABLE_NAME_LEN),
         Column(currentDatabase.c_str(), MAX_DATABASE_NAME_LEN),
         table->meta.primaryKeyIndex >= 0 ? Column(table->meta.primaryKeyIndex)
                                          : Column::nullIntColumn()});

    // Create index on primary key.
    if (!primaryKey.empty()) {
        createIndex(tableName, primaryKey, /*isPrimaryKey=*/true);
    }

    return makePlainResult("OK");
}

PlainResult DBMS::dropTable(const std::string &tableName) {
    checkUseDatabase();

    RecordID id = findTable(currentDatabase, tableName).first;
    if (id == RecordID::NULL_RECORD) {
        throw Error::TableNotExistsError(tableName);
    }

    std::filesystem::path path = getUserTablePath(currentDatabase, tableName);
    // Close the table, if it's opened.
    auto it = openedTables.find(tableName);
    if (it != openedTables.end()) {
        it->second->close();
        delete it->second;
        openedTables.erase(it);
    }

    // Remove the table and other related stuff from the system tables.
    systemTablesTable.remove(id);

    // TODO: Remove index.

    // Remove the table file.
    std::filesystem::remove(getUserTablePath(currentDatabase, tableName));

    return makePlainResult("OK");
}

ShowTableResult DBMS::showTables() {
    checkUseDatabase();

    QueryBuilder::Result queryResult = findAllTables(currentDatabase);

    ShowTableResult result;
    for (const auto &res : queryResult) {
        result.mutable_tables()->Add(res.second[0].data.stringValue);
    }

    return result;
}

DescribeTableResult DBMS::describeTable(const std::string &tableName) {
    checkUseDatabase();

    auto [recordId, table] = getTable(tableName);

    if (table == nullptr) {
        throw Error::TableNotExistsError(tableName);
    }

    DescribeTableResult result;

    for (int i = 0; i < table->meta.numColumn; i++) {
        const ColumnMeta &column = table->meta.columns[i];
        auto *meta = result.add_columns();
        meta->set_field(column.name);
        meta->set_type(column.typeDesc());
        meta->set_nullable(column.nullable);
        meta->set_primary_key(table->meta.primaryKeyIndex == i);
        if (column.hasDefault) {
            meta->set_default_value(column.defaultValDesc());
        }
    }

    return result;
}

PlainResult DBMS::alterPrimaryKey(const std::string &tableName,
                                  const std::string &primaryKey, bool drop) {
    checkUseDatabase();

    auto [recordId, table] = getTable(tableName);

    if (table == nullptr) {
        throw Error::TableNotExistsError(tableName);
    }

    try {
        if (drop) {
            std::string actualPk = primaryKey;
            if (actualPk.empty() && table->meta.primaryKeyIndex >= 0) {
                actualPk =
                    table->meta.columns[table->meta.primaryKeyIndex].name;
            }
            table->dropPrimaryKey(primaryKey);
            dropIndex(tableName, actualPk, /*isPrimaryKey=*/true);
        } else {
            table->setPrimaryKey(primaryKey);
            createIndex(tableName, primaryKey, /*isPrimaryKey=*/true);
        }
    } catch (Internal::TableErrorBase &e) {
        throw Error::AlterPrimaryKeyError(e.what());
    }

    // Update the system table.
    systemTablesTable.update(
        recordId,
        {drop ? Column::nullIntColumn() : Column(table->meta.primaryKeyIndex)},
        0b100);

    return makePlainResult("OK");
}

PlainResult DBMS::createIndex(const std::string &tableName,
                              const std::string &columnName,
                              bool isPrimaryKey) {
    checkUseDatabase();
    auto [id, record] = findIndex(currentDatabase, tableName, columnName);

    if (id != RecordID::NULL_RECORD) {
        if (isPrimaryKey) {
            // Just alter the index type.
            systemIndexesTable.update(id, {Column(2 /*ordinary -> primary*/)},
                                      0b1000);
            return makePlainResult("OK");
        }
        throw Error::AlterIndexError("index exists for " + columnName);
    }

    auto [recordId, table] = getTable(tableName);
    int columnIndex = table->getColumnIndex(columnName.c_str());

    if (columnIndex < 0) {
        throw Error::AlterIndexError("column not exists: " + columnName);
    }

    const ColumnMeta &columnMeta = table->meta.columns[columnIndex];

    if (columnMeta.type != INT) {
        throw Error::AlterIndexError(
            "creating index on VARCHAR or FLOAT is not supported");
    }

    if (columnMeta.nullable) {
        throw Error::AlterIndexError(
            "creating index on nullable column is not "
            "supported");
    }

    // Now create the index. We are not caching this as it is relatively
    // lightweight.
    Index newIndex;
    auto path = getIndexPath(currentDatabase, tableName, columnName);
    std::filesystem::create_directories(path.parent_path());
    newIndex.create(path);

    // Inser existing records into the index.
    table->iterate([&](RecordID id, Columns &columns) {
        newIndex.insert(columns[columnIndex].data.intValue, id);
        return true;
    });

    newIndex.close();

    // Finally, insert the index into the system table.
    systemIndexesTable.insert(
        {Column(currentDatabase.c_str(), MAX_DATABASE_NAME_LEN),
         Column(tableName.c_str(), MAX_TABLE_NAME_LEN),
         Column(columnName.c_str(), MAX_COLUMN_NAME_LEN),
         Column(isPrimaryKey ? 0 : 1)});

    return makePlainResult("OK");
}

PlainResult DBMS::dropIndex(const std::string &tableName,
                            const std::string &columnName, bool isPrimaryKey) {
    checkUseDatabase();
    auto [id, record] = findIndex(currentDatabase, tableName, columnName);

    if (id == RecordID::NULL_RECORD) {
        throw Error::AlterIndexError("index does not exist: " + columnName);
    }

    int indexType = record[3].data.intValue;

    if (isPrimaryKey && indexType == 2 /*primary -> ordinary*/) {
        // Just alter the index type.
        systemIndexesTable.update(id, {Column(1 /*ordinary*/)}, 0b1000);
        return makePlainResult("OK");
    }

    if (!isPrimaryKey) {
        if (indexType == 0) {
            // Drop the primary key's index is not allowed.
            throw Error::AlterIndexError("cannot drop primary key's index");
        }

        if (indexType == 2) {
            // Dropping ordinary index from a primary key results only in type
            // change.
            systemIndexesTable.update(id, {Column(0 /*primary*/)}, 0b1000);
            return makePlainResult("OK");
        }
    }

    // Remove record from system table.
    systemIndexesTable.remove(id);

    // Remove file.
    auto path = getIndexPath(currentDatabase, tableName, columnName);
    std::filesystem::remove(path);

    return makePlainResult("OK");
}

ShowIndexesResult DBMS::showIndexes(const std::string &tableName) {
    checkUseDatabase();

    auto result = findIndexes(currentDatabase, tableName);

    ShowIndexesResult showIndexesResult;

    for (const auto &[_, row] : result) {
        auto *index = showIndexesResult.add_indexes();
        index->set_column(row[2].data.stringValue);
        index->set_table(row[1].data.stringValue);
        index->set_is_pk(row[3].data.intValue == 0);
    }

    return showIndexesResult;
}

PlainResult DBMS::update(QueryBuilder &builder,
                         const std::vector<std::string> &columnNames,
                         const Columns &columns) {
    checkUseDatabase();
    assert(builder.validForUpdateOrDelete());
    assert(columnNames.size() == columns.size());

    IndexedTable *indexedTable =
        dynamic_cast<IndexedTable *>(builder.getDataSource());
    assert(indexedTable != nullptr);

    Table *table = indexedTable->getTable();

    // Check if the input columns are valid.
    std::vector<ColumnInfo> columnInfos = builder.getColumnInfo();
    ColumnBitmap updateBitmap = 0;
    std::vector<std::shared_ptr<Index>> indexes;
    std::vector<int> indexMapping;                 // col i -> index i
    std::vector<int> columnUpdateIndexRevMapping;  // col i -> update i
    std::vector<int> columnUpdateIndexMapping;     // update i -> col i
    int primaryKeyIndex = -1;

    indexMapping.assign(columnInfos.size(), -1);
    columnUpdateIndexRevMapping.assign(columnInfos.size(), -1);

    for (int i = 0; i < columns.size(); i++) {
        int colIndex = table->getColumnIndex(columnNames[i].c_str());
        if (colIndex < 0) {
            throw Error::UpdateError(
                ColumnNotFoundError(columnNames[i]).what());
        }
        updateBitmap |= (1L << colIndex);
        columnUpdateIndexMapping.push_back(colIndex);
        columnUpdateIndexRevMapping[colIndex] = i;

        auto index =
            getIndex(currentDatabase, table->meta.name, columnNames[i]).second;

        if (index != nullptr) {
            indexes.push_back(index);
            indexMapping[colIndex] = indexes.size() - 1;
        }

        if (colIndex == table->meta.primaryKeyIndex) {
            primaryKeyIndex = i;
        }
    }

    // First get id of all records.
    std::vector<RecordID> rids;
    std::vector<Columns> oldColumns;

    builder.iterate([&](RecordID id, Columns &record) {
        rids.push_back(id);
        oldColumns.emplace_back();
        // Store the old values of the index.
        for (int i = 0; i < indexMapping.size(); i++) {
            if (indexMapping[i] >= 0) {
                oldColumns[oldColumns.size() - 1].push_back(record[i]);
            }
        }
        return true;
    });

    // Check constraints.
    if (primaryKeyIndex >= 0) {
        if (rids.size() >= 2) {
            // This update is doomed to cause a duplicate pk.
            throw Error::UpdateError("duplicate primary key");
        }
        assert(indexMapping[primaryKeyIndex] != -1);
        // The the index of this column.
        auto index = indexes[indexMapping[primaryKeyIndex]];
        // Check if the new pk already exists.
        if (index->has(columns[primaryKeyIndex].data.intValue)) {
            throw Error::UpdateError("duplicate primary key");
        }
    }

    // Perform updates (update index by the way).
    for (int i = 0; i < rids.size(); i++) {
        RecordID rid = rids[i];
        table->update(rid, columns, updateBitmap);

        // Update index.
        for (int columnIndex : columnUpdateIndexMapping) {
            if (indexMapping[columnIndex] >= 0) {
                auto index = indexes[indexMapping[columnIndex]];
                int updateColIndex = columnUpdateIndexRevMapping[columnIndex];
                index->remove(oldColumns[i][updateColIndex].data.intValue, rid);
                index->insert(columns[updateColIndex].data.intValue, rid);
            }
        }
    }

    // TODO: FK...

    // Close all indexes.
    for (auto &index : indexes) {
        index->close();
    }

    return makePlainResult("OK", rids.size());
}

PlainResult DBMS::delete_(Internal::QueryBuilder &builder) {
    // Happily, we only need to check foreign key references here, no annoying
    // primary key.
    // TODO: Check PK.

    checkUseDatabase();
    assert(builder.validForUpdateOrDelete());

    // Get all indexes.
    IndexedTable *indexedTable =
        dynamic_cast<IndexedTable *>(builder.getDataSource());
    assert(indexedTable != nullptr);
    Table *table = indexedTable->getTable();

    std::vector<std::shared_ptr<Index>> indexes;
    std::vector<int> indexMapping;  // indexes.i -> col i

    for (int i = 0; i < table->meta.numColumn; i++) {
        auto index = getIndex(currentDatabase, table->meta.name,
                              table->meta.columns[i].name)
                         .second;
        if (index == nullptr) {
            continue;
        }
        indexes.push_back(index);
        indexMapping.push_back(i);
    }

    // Get all records and index keys.
    std::vector<RecordID> rids;
    std::vector<Columns> oldColumns;
    builder.iterate([&](RecordID id, Columns &record) {
        rids.push_back(id);
        oldColumns.emplace_back();
        for (int i = 0; i < indexMapping.size(); i++) {
            oldColumns[oldColumns.size() - 1].push_back(
                record[indexMapping[i]]);
        }
        return true;
    });

    // TODO: Constraints.
    // Remove records (and update index).
    for (int i = 0; i < rids.size(); i++) {
        RecordID rid = rids[i];
        table->remove(rid);

        // Update index.
        for (int j = 0; j < indexMapping.size(); j++) {
            auto index = indexes[j];
            index->remove(oldColumns[i][j].data.intValue, rid);
        }
    }

    // Close all indexes.
    for (auto &index : indexes) {
        index->close();
    }

    return makePlainResult("OK", rids.size());
}

PlainResult DBMS::insert(const std::string &tableName,
                         const std::vector<Column> &_columns,
                         ColumnBitmap emptyBits) {
    checkUseDatabase();

    // Create a mutable copy.
    std::vector<Column> columns = _columns;

    auto [_, table] = getTable(tableName);

    if (table == nullptr) {
        throw Error::TableNotExistsError(tableName);
    }

    // Check if the number of columns is correct.
    constexpr int bitwidth = std::numeric_limits<ColumnBitmap>::digits +
                             std::numeric_limits<ColumnBitmap>::is_signed;
    int numDefault = std::bitset<bitwidth>(emptyBits).count();

    if (numDefault + columns.size() != table->meta.numColumn) {
        throw Error::InsertError("number of columns does not match");
    }

    // Create a mapping from index in source columns to index in table.
    std::vector<int> columnMapping(table->meta.numColumn, -1);
    int indexOfSource = 0;
    for (int i = 0; i < table->meta.numColumn; i++) {
        if ((emptyBits & (1L << i)) == 0) {
            // Check if the data type is matched.
            if (!columns[indexOfSource].isNull &&
                table->meta.columns[i].type != columns[indexOfSource].type) {
                // A fix for casting from int to float.
                if (table->meta.columns[i].type == DataType::FLOAT &&
                    columns[indexOfSource].type == DataType::INT) {
                    // Allow int to float.
                    columns[indexOfSource].type = DataType::FLOAT;
                    columns[indexOfSource].data.floatValue =
                        columns[indexOfSource].data.intValue;
                } else {
                    throw Error::InsertError("data type does not match");
                }
            }
            columnMapping[i] = indexOfSource;
            // Also, check null value and set corresponding data type.
            columns[indexOfSource].type = table->meta.columns[i].type;
            indexOfSource++;
        }
    }

    // Check primary key.
    if (table->meta.primaryKeyIndex >= 0) {
        int primaryKeyIndex = table->meta.primaryKeyIndex;
        std::string primaryKeyName = table->meta.columns[primaryKeyIndex].name;
        int key =
            columnMapping[primaryKeyIndex] == -1
                ? table->meta.columns[primaryKeyIndex].defaultValue.intValue
                : columns[columnMapping[primaryKeyIndex]].data.intValue;
        // Index-based search.
        auto indexedTable = newIndexedTable(table);
        QueryBuilder builder(indexedTable);
        builder.condition(primaryKeyName, EQ, key).limit(1);

        builder.iterate([&](RecordID id, Columns &record) {
            throw Error::InsertError("duplicate primary key" +
                                     std::to_string(record[0].data.intValue));
            return false;
        });
    }

    RecordID id = table->insert(columns, ~emptyBits);

    // Insert into indexes.
    std::map<int, std::shared_ptr<Index>> indexes;

    QueryBuilder::Result result = findIndexes(currentDatabase, tableName);

    for (const auto &pair : result) {
        auto [_, indexColumns] = pair;
        const std::string &columnName = indexColumns[2].data.stringValue;
        int columnIndex = table->getColumnIndex(columnName.c_str());

        auto path = getIndexPath(currentDatabase, tableName, columnName);

        // TODO: Cache index.
        Index index;
        index.open(path);

        int key = columnMapping[columnIndex] == -1
                      ? table->meta.columns[columnIndex].defaultValue.intValue
                      : columns[columnMapping[columnIndex]].data.intValue;

        index.insert(key, id);
        index.close();
    }

    return makePlainResult("OK", 1);
}

QueryBuilder DBMS::buildQuery(
    const std::vector<std::string> &tableNames,
    const std::vector<QuerySelector> &selectors,
    const std::vector<Internal::CompareValueCondition> &valueConditions,
    const std::vector<Internal::CompareColumnCondition> &columnConditions,
    const std::vector<Internal::CompareNullCondition> &nullConditions,
    int limit, int offset, bool updateOrDelete) {
    checkUseDatabase();

    QueryBuilder builder;

    if (updateOrDelete) {
        assert(tableNames.size() == 1);

        Table *table = getTable(tableNames[0]).second;
        if (table == nullptr) {
            throw Error::TableNotExistsError(tableNames[0]);
        }
        std::shared_ptr<IndexedTable> indexedTable = newIndexedTable(table);
        builder = QueryBuilder(indexedTable);
    } else {
        if (tableNames.size() > 2) {
            throw Error::SelectError("only support table joins with 2 tables");
        }

        std::shared_ptr<JoinedTable> joinedTable =
            std::make_shared<JoinedTable>();

        for (const auto &name : tableNames) {
            Table *table = getTable(name).second;
            if (table == nullptr) {
                throw Error::TableNotExistsError(name);
            }
            std::shared_ptr<IndexedTable> indexedTable = newIndexedTable(table);
            joinedTable->append(indexedTable);
        }
        builder = QueryBuilder(joinedTable);
    }

    for (const auto &cond : valueConditions) {
        builder.condition(cond);
    }

    for (const auto &cond : columnConditions) {
        builder.condition(cond);
    }

    for (const auto &cond : nullConditions) {
        builder.nullCondition(cond);
    }

    if (limit >= 0) {
        builder.limit(limit);
    }

    if (offset >= 1) {
        builder.offset(offset);
    }

    for (const auto &selector : selectors) {
        builder.select(selector);
    }

    return builder;
}

QueryBuilder DBMS::buildQueryForUpdateOrDelete(
    const std::string &table,
    const std::vector<CompareValueCondition> &valueConditions,
    const std::vector<CompareColumnCondition> &columnConditions,
    const std::vector<CompareNullCondition> &nullConditions) {
    return buildQuery({table}, {}, valueConditions, columnConditions,
                      nullConditions, -1, 0, true);
}

QueryResult DBMS::select(Internal::QueryBuilder &builder) {
    QueryBuilder::Result result;
    std::vector<ColumnInfo> columns;

    try {
        result = builder.execute();
        columns = builder.getColumnInfo();
    } catch (BaseError &e) {
        throw Error::SelectError(e.what());
    }

    QueryResult queryResult;

    // Insert columns.
    for (const ColumnInfo &info : columns) {
        auto *column = queryResult.add_columns();
        column->set_name(info.columnName);
        switch (info.type) {
            case INT:
                column->set_type(QueryColumn_Type_TYPE_INT);
                break;
            case FLOAT:
                column->set_type(QueryColumn_Type_TYPE_FLOAT);
                break;
            case VARCHAR:
                column->set_type(QueryColumn_Type_TYPE_VARCHAR);
                break;
        }
    }

    // Add rows.
    for (const auto &pair : result) {
        auto [_, columns] = pair;
        auto *row = queryResult.add_rows();
        for (const auto &column : columns) {
            QueryValue *val = row->add_values();
            if (column.isNull) {
                val->set_null_value(true);
                continue;
            }
            switch (column.type) {
                case INT:
                    val->set_int_value(column.data.intValue);
                    break;
                case FLOAT:
                    val->set_float_value(column.data.floatValue);
                    break;
                case VARCHAR:
                    val->set_varchar_value(column.data.stringValue);
                    break;
            }
        }
    }

    return queryResult;
}

void DBMS::initSystemTable(Internal::Table *table, const std::string &name,
                           const std::vector<Internal::ColumnMeta> columns) {
    std::filesystem::path path = getSystemTablePath(name);

    try {
        if (std::filesystem::exists(path)) {
            table->open(path);
        } else {
            table->create(path, "system_" + name, columns);
        }
    } catch (BaseError &e) {
        throw Error::InitializationError(e.what());
    }
}

PlainResult DBMS::makePlainResult(const std::string &msg, int affectedRows) {
    PlainResult result;
    result.set_msg(msg);
    result.set_affected_rows(affectedRows);
    return result;
}

std::pair<RecordID, Columns> DBMS::findDatabase(const std::string &dbName) {
    QueryBuilder builder(&systemDatabaseTable);
    builder.condition("name", EQ, dbName.c_str()).limit(1);

    auto result = builder.execute();

    if (result.size() == 0) {
        return {RecordID::NULL_RECORD, {}};
    } else {
        return result[0];
    }
}

QueryBuilder::Result DBMS::findAllTables(const std::string &database) {
    QueryBuilder builder(&systemTablesTable);
    builder.condition("database", EQ, database.c_str()).select("name");

    return builder.execute();
}

std::pair<RecordID, Columns> DBMS::findTable(const std::string &database,
                                             const std::string &tableName) {
    QueryBuilder builder(&systemTablesTable);

    builder.condition("name", EQ, tableName.c_str())
        .condition("database", EQ, database.c_str())
        .limit(1);

    auto result = builder.execute();

    if (result.size() == 0) {
        return {RecordID::NULL_RECORD, {}};
    } else {
        return result[0];
    }
}

std::pair<RecordID, Columns> DBMS::findIndex(const std::string &database,
                                             const std::string &table,
                                             const std::string &columnName) {
    QueryBuilder builder(&systemIndexesTable);

    builder.condition("database", EQ, database.c_str())
        .condition("table", EQ, table.c_str())
        .condition("field", EQ, columnName.c_str())
        .limit(1);

    auto result = builder.execute();

    if (result.size() == 0) {
        return {RecordID::NULL_RECORD, {}};
    } else {
        return result[0];
    }
}

QueryBuilder::Result DBMS::findIndexes(const std::string &database,
                                       const std::string &table) {
    QueryBuilder builder(&systemIndexesTable);

    builder.condition("database", EQ, database.c_str())
        .condition("table", EQ, table.c_str());

    return builder.execute();
}

std::pair<RecordID, Table *> DBMS::getTable(const std::string &tableName) {
    RecordID id = findTable(currentDatabase, tableName).first;
    if (id == RecordID::NULL_RECORD) {
        return {id, nullptr};
    }

    std::filesystem::path path = getUserTablePath(currentDatabase, tableName);
    Table *table;

    auto it = openedTables.find(tableName);
    if (it != openedTables.end()) {
        table = it->second;
    } else {
        table = new Table();
        table->open(path);
        openedTables[tableName] = table;
    }

    return {id, table};
}

std::pair<RecordID, std::shared_ptr<Index>> DBMS::getIndex(
    const std::string &database, const std::string &table,
    const std::string &column) {
    auto [id, columns] = findIndex(currentDatabase, table, column);
    if (id == RecordID::NULL_RECORD) {
        return {id, nullptr};
    }
    auto path = getIndexPath(currentDatabase, table, column);
    std::shared_ptr<Index> index = std::make_shared<Index>();
    index->open(path);

    return {id, index};
}

std::shared_ptr<IndexedTable> DBMS::newIndexedTable(Table *table) {
    std::shared_ptr<IndexedTable> indexedTable = std::make_shared<IndexedTable>(
        table,
        [this](const std::string &table,
               const std::string &column) -> std::shared_ptr<Index> {
            return this->getIndex(currentDatabase, table, column).second;
        });

    return indexedTable;
}

void DBMS::checkUseDatabase() {
    if (currentDatabase.empty()) {
        throw Error::DatabaseNotSelectedError();
    }
}

void DBMS::clearCurrentDatabase() {
    if (!currentDatabase.empty()) {
        // Close related stuff
        for (auto &pair : openedTables) {
            pair.second->close();
            delete pair.second;
        }
        openedTables.clear();
    }
    currentDatabase.clear();
}

std::filesystem::path DBMS::getSystemTablePath(const std::string &name) {
    return rootPath / "system" / name;
}

std::filesystem::path DBMS::getUserTablePath(const std::string database,
                                             const std::string &name) {
    return rootPath / database / name;
}

std::filesystem::path DBMS::getIndexPath(const std::string &database,
                                         const std::string &table,
                                         const std::string &column) {
    return rootPath / "index" / database / table / column;
}

}  // namespace SimpleDB