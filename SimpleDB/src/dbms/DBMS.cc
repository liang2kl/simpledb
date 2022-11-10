#include "DBMS.h"

#include <SQLParser/SqlLexer.h>

#include "Logger.h"

using namespace SQLParser;

namespace SimpleDB {

class ParseErrorListener : public antlr4::BaseErrorListener {
    virtual void syntaxError(antlr4::Recognizer *recognizer,
                             antlr4::Token *offendingSymbol, size_t line,
                             size_t charPositionInLine, const std::string &msg,
                             std::exception_ptr e) override {
        throw Error::SyntaxError(msg);
    }
};

static ParseErrorListener errorListener;

DBMS::DBMS() { visitor = ParseTreeVisitor(this); }

void DBMS::executeSQL(std::istream &stream) {
    try {
        antlr4::ANTLRInputStream input(stream);
        SqlLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        SqlParser parser(&tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(&errorListener);

        auto program = parser.program();

        visitor.visitProgram(program);
    } catch (Error::ExecutionErrorBase &e) {
        // Normal exception.
        Logger::log(DEBUG, "DBMS: exception caught during execution: %s\n",
                    e.what());
        throw;
    } catch (Internal::InternalErrorBase &e) {
        // Uncaught internal exception.
        Logger::log(DEBUG,
                    "DBMS: uncaught internal exception during execution: %s\n",
                    e.what());
        throw Error::InternalError(e.what());
    } catch (std::exception &e) {
        // Unknown error.
        Logger::log(DEBUG,
                    "DBMS: unknown exception caught during execution: %s\n",
                    e.what());
        throw Error::UnknownError(e.what());
    } catch (...) {
        // Unknown error.
        Logger::log(DEBUG, "DBMS: unknown exception caught during execution");
        throw Error::UnknownError();
    }
}

void DBMS::createDatabase(const std::string &dbName) {}
void DBMS::dropDatabase(const std::string &dbName) {}
void DBMS::useDatabase(const std::string &dbName) {}
void DBMS::showTables() {}
void DBMS::showDatabases() {}
void DBMS::createTable(const std::string &tableName,
                       const std::vector<ColumnMeta> &columns) {}
void DBMS::dropTable(const std::string &tableName) {}
void DBMS::describeTable(const std::string &tableName) {}

}  // namespace SimpleDB