#include "DBMS.h"

#include <any>

#include "SQLParser/SqlLexer.h"
#include "internal/Logger.h"

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

DBMS::DBMS() { visitor = ParseTreeVisitor(this); }

std::vector<Service::ExecutionResult> DBMS::executeSQL(std::istream &stream) {
    try {
        antlr4::ANTLRInputStream input(stream);
        SqlLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        SqlParser parser(&tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(&errorListener);

        auto program = parser.program();

        return std::any_cast<std::vector<Service::ExecutionResult>>(
            visitor.visitProgram(program));

    } catch (Error::ExecutionErrorBase &e) {
        // Normal exception.
        Logger::log(DEBUG_, "DBMS: exception caught during execution: %s\n",
                    e.what());
        throw;

    } catch (Internal::InternalErrorBase &e) {
        // Uncaught internal exception.
        Logger::log(DEBUG_,
                    "DBMS: uncaught internal exception during execution: %s\n",
                    e.what());
        throw Error::InternalError(e.what());

    } catch (std::exception &e) {
        // Unknown error.
        Logger::log(DEBUG_,
                    "DBMS: unknown exception caught during execution: %s\n",
                    e.what());
        throw Error::UnknownError(e.what());

    } catch (...) {
        // Unknown error.
        Logger::log(DEBUG_, "DBMS: unknown exception caught during execution");
        throw Error::UnknownError();
    }
}

PlainResult DBMS::createDatabase(const std::string &dbName) {}
PlainResult DBMS::dropDatabase(const std::string &dbName) {}
PlainResult DBMS::useDatabase(const std::string &dbName) {}
ShowTableResult DBMS::showTables() {}
ShowDatabasesResult DBMS::showDatabases() {}
PlainResult DBMS::createTable(const std::string &tableName,
                              const std::vector<ColumnMeta> &columns) {}
PlainResult DBMS::dropTable(const std::string &tableName) {}
// void DBMS::describeTable(const std::string &tableName) {}

}  // namespace SimpleDB