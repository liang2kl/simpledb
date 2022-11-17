#include "DBMS.h"

#include <SimpleDB/internal/RecordScanner.h>

#include <any>
#include <filesystem>
#include <system_error>
#include <tuple>

#include "Error.h"
#include "SQLParser/SqlLexer.h"
#include "internal/Logger.h"
#include "internal/Macros.h"
#include "internal/Table.h"

using namespace SQLParser;
using namespace SimpleDB::Internal;
using namespace SimpleDB::Service;

#define WRAP_INTERNAL_THROW(stmt)            \
    try {                                    \
        stmt;                                \
    } catch (BaseError & e) {                \
        throw Error::UnknownError(e.what()); \
    }

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

DBMS::~DBMS() {
    databaseSystemTable.close();
    for (auto &pair : openedTables) {
        pair.second->close();
    }
}

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
            std::filesystem::create_directory(systemPath);
        } catch (std::filesystem::filesystem_error &e) {
            throw Error::InitializationError(e.what());
        }
    }

    // Create or load system tables.
    initDatabaseSystemTable();

    initialized = true;
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
    return std::any_cast<std::vector<Service::ExecutionResult>>(
        visitor.visitProgram(program));
}

PlainResult DBMS::createDatabase(const std::string &dbName) {
    // TODO: Add index to system table.
    CompareConditions conditions(1);
    conditions[0] = CompareCondition(databaseSystemTableColumns[0].name,
                                     CompareOp::EQ, dbName.c_str());

    bool found = databaseSystemTable.getScanner().findFirst(conditions).first !=
                 RecordID::NULL_RECORD;

    if (found) {
        throw Error::DatabaseExistsError(dbName);
    }

    // Create database directory
    const std::filesystem::path dbPath = rootPath / dbName;
    if (!std::filesystem::is_directory(dbPath)) {
        try {
            std::filesystem::create_directory(dbPath);
        } catch (std::filesystem::filesystem_error &e) {
            throw Error::CreateDatabaseError(e.what());
        }
    }

    // Append to the system table
    Columns columns = {Column(dbName.c_str(), MAX_VARCHAR_LEN)};
    databaseSystemTable.insert(columns);

    return makePlainResult("OK");
}

PlainResult DBMS::dropDatabase(const std::string &dbName) {
    CompareConditions conditions(1);
    conditions[0] = CompareCondition(databaseSystemTableColumns[0].name,
                                     CompareOp::EQ, dbName.c_str());

    auto [id, columns] = databaseSystemTable.getScanner().findFirst(conditions);
    if (id == RecordID::NULL_RECORD) {
        throw Error::DatabaseNotExistError(dbName);
    }

    WRAP_INTERNAL_THROW(databaseSystemTable.remove(id));

    return makePlainResult("OK");
}

PlainResult DBMS::useDatabase(const std::string &dbName) {}
ShowTableResult DBMS::showTables() {}
ShowDatabasesResult DBMS::showDatabases() {}
PlainResult DBMS::createTable(const std::string &tableName,
                              const std::vector<ColumnMeta> &columns) {}
PlainResult DBMS::dropTable(const std::string &tableName) {}
// void DBMS::describeTable(const std::string &tableName) {}

void DBMS::initDatabaseSystemTable() {
    std::filesystem::path path = rootPath / "system" / "databases";

    // TODO: Add index to system table.
    try {
        if (std::filesystem::exists(path)) {
            databaseSystemTable.open(path);
        } else {
            databaseSystemTable.create(
                path, "system_databases",
                sizeof(databaseSystemTableColumns) / sizeof(ColumnMeta),
                databaseSystemTableColumns);
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

}  // namespace SimpleDB