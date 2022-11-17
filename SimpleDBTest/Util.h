#ifndef _SIMPLEDBTEST_UTIL_H
#define _SIMPLEDBTEST_UTIL_H

#include <SimpleDB/SimpleDB.h>

struct DisableLogGuard {
    DisableLogGuard() {
        level = SimpleDB::Internal::Logger::getLogLevel();
        SimpleDB::Internal::Logger::setLogLevel(SimpleDB::Internal::SILENT);
    }

    ~DisableLogGuard() { SimpleDB::Internal::Logger::setLogLevel(level); }

private:
    SimpleDB::Internal::LogLevel level;
};

void compareColumns(const SimpleDB::Internal::Columns &columns,
                    const SimpleDB::Internal::Columns &readColumns);

#endif