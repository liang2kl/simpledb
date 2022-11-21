#ifndef _SIMPLEDB_PARSE_HELPER_H
#define _SIMPLEDB_PARSE_HELPER_H

#include <SimpleDB/internal/Column.h>

#include <cstring>
#include <string>

#include "Error.h"
#include "internal/Macros.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

class ParseHelper {
public:
    static void parseName(const std::string &name, char *dest) {
        if (name.size() >= MAX_COLUMN_NAME_LEN) {
            throw Error::IncompatableValueError("Column name too long");
        }
        std::strcpy(dest, name.c_str());
    }

    static DataType parseDataType(const std::string &type) {
        if (type == "INT") {
            return INT;
        } else if (type == "FLOAT") {
            return FLOAT;
        } else if (type.compare(0, sizeof("VARCHAR") - 1, "VARCHAR") == 0) {
            return VARCHAR;
        }
        assert(false);
        return INT;
    }

    static void parseDefaultValue(const std::string &value, DataType type,
                                  ColumnValue &dest, int size) {
        switch (type) {
            case INT:
                dest.intValue = parseInt(value);
                break;
            case FLOAT:
                dest.floatValue = parseFloat(value);
                break;
            case VARCHAR:
                if (value.size() >= MAX_VARCHAR_LEN || value.size() > size) {
                    throw Error::IncompatableValueError("VARCHAR too long");
                }
                std::strcpy(dest.stringValue,
                            value.substr(1, value.size() - 2).c_str());
                break;
        }
    }

    static int parseInt(const std::string &value) {
        try {
            return std::stoi(value);
        } catch (std::exception &e) {
            throw Error::IncompatableValueError("Invalid integer value");
        }
    }

    static float parseFloat(const std::string &value) {
        try {
            return std::stof(value);
        } catch (std::exception &e) {
            throw Error::IncompatableValueError("Invalid float value");
        }
    }
};

}  // namespace Internal
}  // namespace SimpleDB

#endif