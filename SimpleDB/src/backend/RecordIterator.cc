#include "RecordIterator.h"

#include <cmath>
#include <regex>

#include "Logger.h"
#include "Table.h"

namespace SimpleDB {

RecordIterator::RecordIterator(Table *table) : table(table) {}

int RecordIterator::iterate(Columns bufColumns, CompareConditions conditions,
                            IteratorFunc callback) {
    Logger::log(VERBOSE,
                "RecordIterator: iterate through all records of the table\n");
    int numReturns = 0;
    for (int page = 1; page < table->meta.numUsedPages; page++) {
        PageHandle *handle = table->getHandle(page);
        for (int slot = 1; slot < NUM_SLOT_PER_PAGE; slot++) {
            if (table->occupied(*handle, slot)) {
                table->get({page, slot}, bufColumns);
                if (validate(bufColumns, conditions)) {
                    callback(numReturns);
                    numReturns++;
                }
            }
        }
    }

    return numReturns;
}

int RecordIterator::iterate(Columns bufColumns, CompareConditions conditions,
                            GetNextRecordFunc getNext, IteratorFunc callback) {
    int numReturns = 0;
    RecordID id;

    while (id = getNext(), id.page != -1) {
        table->get(id, bufColumns);
        if (validate(bufColumns, conditions)) {
            callback(numReturns);
            numReturns++;
        }
    }

    return numReturns;
}

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

using _Comparer = bool (*)(CompareOp, const char *, const char *);

template <typename T>
static bool _comparer(CompareOp op, const char *lhs, const char *rhs) {
    T l = T(lhs);
    T r = T(rhs);
    switch (op) {
        case EQ:
            return l == r;
        case NE:
            return l != r;
        case LT:
            return l < r;
        case LE:
            return l <= r;
        case GT:
            return l > r;
        case GE:
            return l >= r;
        default:
            Logger::log(ERROR,
                        "RecordIterator: internal error: invalid compare op %d "
                        "for _comparer<T>\n",
                        op);
            throw Error::UnexpedtedOperatorError();
    }
}

static bool _nullComparer(CompareOp op, const Column &column) {
    switch (op) {
        case IS_NULL:
            return column.isNull;
        case NOT_NULL:
            return !column.isNull;
        default:
#ifdef _DEBUG
            Logger::log(ERROR,
                        "RecordIterator: internal error: invalid compare op %d "
                        "for IS_NULL or NOT_NULL comparision\n",
                        op);
            throw Error::UnexpedtedOperatorError();
#endif
    }
}

static bool _regexComparer(CompareOp op, const Column &column,
                           const char *regexStr) {
#ifdef _DEBUG
    if (op != LIKE) {
        Logger::log(ERROR,
                    "RecordIterator: internal error: invalid compare op %d "
                    "for LIKE comparision\n",
                    op);
        throw Error::UnexpedtedOperatorError();
    }
#endif
    try {
        std::regex regex(regexStr);
        return std::regex_match(column.data, regex);
    } catch (std::regex_error &e) {
        Logger::log(ERROR, "RecordIterator: invalid input regex \"%s\"\n",
                    regexStr);
        throw Error::InvalidRegexError();
    }
}

// static bool

bool RecordIterator::validate(const Columns columns,
                              const CompareConditions &conditions) {
    for (auto &condition : conditions) {
        int columnIndex = table->getColumnIndex(condition.columnName);
        if (columnIndex < 0) {
            throw Error::InvalidColumnNameError();
        }

        ColumnMeta &meta = table->meta.columns[columnIndex];

        Column &column = columns[columnIndex];

        // FIXME: What's the standard to deal with this?
        if (condition.op == IS_NULL || condition.op == NOT_NULL) {
            return _nullComparer(condition.op, column);
        }

        if (condition.op == LIKE) {
            if (meta.type != VARCHAR) {
                Logger::log(ERROR,
                            "RecordIterator: LIKE can only be used on "
                            "VARCHAR column, but column %s is of type "
                            "%d\n",
                            condition.columnName, meta.type);
                throw Error::InvalidOperatorError();
            }
            return _regexComparer(condition.op, column, condition.value);
        }

        _Comparer comparer;

        switch (column.type) {
            case DataType::INT:
                comparer = _comparer<_Int>;
                break;
            case DataType::FLOAT:
                comparer = _comparer<_Float>;
                break;
            case DataType::VARCHAR:
                comparer = _comparer<_String>;
                break;
        }

        if (!comparer(condition.op, column.data, condition.value)) {
            return false;
        }
    }

    return true;
}

}  // namespace SimpleDB
