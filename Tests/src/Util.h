#ifndef _SIMPLEDBTEST_UTIL_H
#define _SIMPLEDBTEST_UTIL_H

#include <SimpleDB/SimpleDB.h>

struct DisableLogGuard {
    DisableLogGuard() {
        level = SimpleDB::Logger::getLogLevel();
        SimpleDB::Logger::setLogLevel(SimpleDB::SILENT);
    }

    ~DisableLogGuard() { SimpleDB::Logger::setLogLevel(level); }

private:
    SimpleDB::LogLevel level;
};

void compareColumns(SimpleDB::Column *columns, SimpleDB::Column *readColumns,
                    int num);

#endif