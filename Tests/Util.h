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

void compareColumns(SimpleDB::Internal::Column *columns, SimpleDB::Internal::Column *readColumns,
                    int num);

#endif