#include "internal/Logger.h"

#include <stdio.h>

namespace SimpleDB {
LogLevel Logger::displayMinLevel = NOTICE;
FILE *Logger::errorStream = stderr;

void Logger::log(LogLevel level, const char *fmt, ...) {
    if (level < displayMinLevel) return;
    fprintf(errorStream, "[%s] ", logLevelNames[level]);
    fprintf(errorStream, fmt, logLevelNames[level]);
}
}  // namespace SimpleDB