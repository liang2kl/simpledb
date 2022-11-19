#include "internal/Logger.h"

#include <stdarg.h>
#include <stdio.h>

namespace SimpleDB {
namespace Internal {

LogLevel Logger::displayMinLevel = NOTICE;
FILE *Logger::errorStream = stderr;

void Logger::log(LogLevel level, const char *fmt, ...) {
    if (level < displayMinLevel) return;
    fprintf(errorStream, "[%-7s] ", logLevelNames[level]);

    va_list argList;

    va_start(argList, fmt);
    vfprintf(errorStream, fmt, argList);
    va_end(argList);
}

}  // namespace Internal
}  // namespace SimpleDB