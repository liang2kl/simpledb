#ifndef _SIMPLEDB_LOGGER_H
#define _SIMPLEDB_LOGGER_H

#include <stdio.h>

namespace SimpleDB {
namespace Internal {

enum LogLevel { VERBOSE, DEBUG_, NOTICE, WARNING, ERROR, SILENT };

static const char* logLevelNames[] = {"VERBOSE", "DEBUG", "NOTICE",
                                      "WARNING", "ERROR", "SILENT"};

class Logger {
public:
    static void setErrorStream(FILE* stream) { errorStream = stream; }
    static void setLogLevel(LogLevel level) { displayMinLevel = level; }
    static LogLevel getLogLevel() { return displayMinLevel; }
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

}  // namespace Internal
}  // namespace SimpleDB
#endif