#ifndef _SIMPLEDB_PARSE_HELPER_H
#define _SIMPLEDB_PARSE_HELPER_H

#include <SQLParser/SqlParser.h>

#include <cassert>
#include <cstring>
#include <string>

#include "Error.h"
#include "internal/Column.h"
#include "internal/Macros.h"
#include "internal/QueryFilter.h"
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
                parseString(value, std::min(MAX_VARCHAR_LEN, size),
                            dest.stringValue);
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

    static void parseString(const std::string &value, int maxSize, char *dest) {
        // Here we deal with the raw string from antlr, which starts and ends
        // with a single quote (').
        if (value.size() - 2 >= maxSize) {
            throw Error::IncompatableValueError("VARCHAR too long");
        }
        std::strcpy(dest, value.substr(1, value.size() - 2).c_str());
    }

    static CompareOp parseCompareOp(const std::string &op) {
        if (op == "=") {
            return EQ;
        } else if (op == "<>") {
            return NE;
        } else if (op == "<") {
            return LT;
        } else if (op == "<=") {
            return LE;
        } else if (op == ">") {
            return GT;
        } else if (op == ">=") {
            return GE;
        }
        assert(false);
    }

    static Column parseColumnValue(SQLParser::SqlParser::ValueContext *ctx) {
        Column column;
        if (ctx->Integer() != nullptr) {
            column.data.intValue = parseInt(ctx->Integer()->getText());
            column.type = INT;
        } else if (ctx->Float() != nullptr) {
            column.data.floatValue = parseFloat(ctx->Float()->getText());
            column.type = FLOAT;
        } else if (ctx->String() != nullptr) {
            parseString(ctx->String()->getText(), MAX_VARCHAR_LEN,
                        column.data.stringValue);
            column.type = VARCHAR;
        } else if (ctx->Null() != nullptr) {
            column.isNull = true;
            // The data type is unknown here, must be set outside.
        }
        return column;
    }

    // static CompareValueCondition parseCompareValueCondition(
    //     const std::string &columnName, const std::string operator_,
    //     SQLParser::SqlParser::ValueContext *ctx) {
    //     return CompareValueCondition(columnName, parseCompareOp(operator_),
    //                                  parseColumnValue(ctx).data);
    // }
};

}  // namespace Internal
}  // namespace SimpleDB

#endif