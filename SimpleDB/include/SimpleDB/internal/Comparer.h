#ifndef _SIMPLEDB_COMPARER_H
#define _SIMPLEDB_COMPARER_H

#include <string.h>

#include <cmath>

#include "internal/Macros.h"

namespace SimpleDB {
namespace Internal {

// === Internal classes only for comparision ===
class _String {
public:
    _String(const char *data) : str(data) {}
    bool operator==(const _String &rhs) const {
        return strcmp(str, rhs.str) == 0;
    }
    bool operator!=(const _String &rhs) const {
        return strcmp(str, rhs.str) != 0;
    }
    bool operator<(const _String &rhs) const {
        return strcmp(str, rhs.str) < 0;
    }
    bool operator<=(const _String &rhs) const {
        return strcmp(str, rhs.str) <= 0;
    }
    bool operator>(const _String &rhs) const {
        return strcmp(str, rhs.str) > 0;
    }
    bool operator>=(const _String &rhs) const {
        return strcmp(str, rhs.str) >= 0;
    }

private:
    const char *str;
};

class _Int {
public:
    _Int(const char *data) : value(*(int *)data) {}
    bool operator==(const _Int &rhs) const { return value == rhs.value; }
    bool operator!=(const _Int &rhs) const { return value != rhs.value; }
    bool operator<(const _Int &rhs) const { return value < rhs.value; }
    bool operator<=(const _Int &rhs) const { return value <= rhs.value; }
    bool operator>(const _Int &rhs) const { return value > rhs.value; }
    bool operator>=(const _Int &rhs) const { return value >= rhs.value; }

private:
    int value;
};

class _Float {
public:
    _Float(const char *data) : value(*(float *)data) {}
    bool operator==(const _Float &rhs) const {
        return std::fabs(value - rhs.value) <= EQUAL_PRECISION;
    }
    bool operator!=(const _Float &rhs) const {
        return std::fabs(value - rhs.value) <= EQUAL_PRECISION;
    }
    bool operator<(const _Float &rhs) const { return value < rhs.value; }
    bool operator<=(const _Float &rhs) const {
        return *this < rhs || *this == rhs;
    }
    bool operator>(const _Float &rhs) const { return value > rhs.value; }
    bool operator>=(const _Float &rhs) const {
        return *this > rhs || *this == rhs;
    }

private:
    float value;
};

}  // namespace Internal

}  // namespace SimpleDB

#endif