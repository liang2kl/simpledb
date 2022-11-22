#include "DBMS.h"

#include <SimpleDB/internal/ParseTreeVisitor.h>

#include <any>
#include <filesystem>
#include <system_error>
#include <tuple>
#include <vector>

#include "Error.h"
#include "SQLParser/SqlLexer.h"
#include "SimpleDB/internal/QueryBuilder.h"
#include "internal/Logger.h"
#include "internal/Macros.h"
#include "internal/QueryBuilder.h"
#include "internal/Table.h"

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
    initSystemTable(&systemIndicesTable, "indices", systemIndicesTableColumns);

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
    // TODO: Add index to system table.
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

    QueryBuilder::Result allTables = findAllTables();
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

    // Remove the table from the system table.
    systemTablesTable.remove(id);

    // Remove the table file.
    std::filesystem::remove(getUserTablePath(currentDatabase, tableName));

    return makePlainResult("OK");
}

ShowTableResult DBMS::showTables() {
    checkUseDatabase();

    QueryBuilder::Result queryResult = findAllTables();

    ShowTableResult result;
    for (const auto &res : queryResult) {
        result.mutable_tables()->Add(res.second[0].data.stringValue);
    }

    return result;
}

DescribeTableResult DBMS::describeTable(const std::string &tableName) {
    checkUseDatabase();

    Table *table = getTable(tableName);

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

    Table *table = getTable(tableName);

    if (table == nullptr) {
        throw Error::TableNotExistsError(tableName);
    }

    try {
        if (drop) {
            table->dropPrimaryKey(primaryKey);
        } else {
            table->setPrimaryKey(primaryKey);
        }
    } catch (Internal::TableErrorBase &e) {
        throw Error::AlterTableError(e.what());
    }

    // Update the system table.
    RecordID id = findTable(currentDatabase, tableName).first;
    systemTablesTable.update(
        id,
        {drop ? Column::nullIntColumn() : Column(table->meta.primaryKeyIndex)},
        0b100);

    return makePlainResult("OK");
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

PlainResult DBMS::makePlainResult(const std::string &msg) {
    PlainResult result;
    result.set_msg(msg);
    return result;
}

std::pair<RecordID, Columns> DBMS::findDatabase(const std::string &dbName) {
    CompareConditions conditions = {{CompareCondition::eq(
        systemDatabaseTableColumns[0].name, dbName.c_str())}};

    QueryBuilder builder(&systemDatabaseTable);
    builder.condition(systemDatabaseTableColumns[0].name, EQ, dbName.c_str())
        .limit(1);

    auto result = builder.execute();

    if (result.size() == 0) {
        return {RecordID::NULL_RECORD, {}};
    } else {
        return result[0];
    }
}

QueryBuilder::Result DBMS::findAllTables() {
    QueryBuilder builder(&systemTablesTable);
    builder
        .condition(systemTablesTableColumns[1].name, EQ,
                   currentDatabase.c_str())
        .select(systemTablesTableColumns[0].name);

    return builder.execute();
}

std::pair<RecordID, Columns> DBMS::findTable(const std::string &database,
                                             const std::string &tableName) {
    QueryBuilder builder(&systemTablesTable);

    builder.condition(systemTablesTableColumns[0].name, EQ, tableName.c_str())
        .condition(systemTablesTableColumns[1].name, EQ, database.c_str())
        .limit(1);

    auto result = builder.execute();

    if (result.size() == 0) {
        return {RecordID::NULL_RECORD, {}};
    } else {
        return result[0];
    }
}

Table *DBMS::getTable(const std::string &tableName) {
    RecordID id = findTable(currentDatabase, tableName).first;
    if (id == RecordID::NULL_RECORD) {
        return nullptr;
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

    return table;
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

}  // namespace SimpleDB