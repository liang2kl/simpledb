#ifndef _SIMPLEDB_LOGGER_H
#define _SIMPLEDB_LOGGER_H

#include <stdio.h>

namespace SimpleDB {

enum LogLevel { VERBOSE, DEBUG, NOTICE, WARNING, ERROR, SILENT };

static const char* logLevelNames[] = {"VERBOSE", "DEBUG", "NOTICE",
                                      "WARNING", "ERROR", "SILENT"};

class Logger {
public:
    static void setErrorStream(FILE* stream) { errorStream = stream; }
    static void setLogLevel(LogLevel level) { displayMinLevel = level; }
    static void log(LogLevel level, const char* fmt, ...)
#ifdef __GNUC__
        __attribute__((format(printf, 2, 3)));
#else
        ;
#endif

private:
    static LogLevel displayMinLevel;
    static FILE* errorStream;
};

}  // namespace SimpleDB
#endif