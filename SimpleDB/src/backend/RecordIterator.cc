#include "internal/RecordIterator.h"

#include <cmath>
#include <regex>

#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

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
                    bool continueIteration = callback(numReturns);
                    numReturns++;
                    if (!continueIteration) {
                        break;
                    }
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
            throw Internal::UnexpedtedOperatorError();
    }
}

static bool _nullComparer(CompareOp op, const Column &column) {
    switch (op) {
        case IS_NULL:
            return column.isNull;
        case NOT_NULL:
            return !column.isNull;
        default:
            Logger::log(ERROR,
                        "RecordIterator: internal error: invalid compare op %d "
                        "for IS_NULL or NOT_NULL comparision\n",
                        op);
            throw Internal::UnexpedtedOperatorError();
    }
}

static bool _regexComparer(CompareOp op, const Column &column,
                           const char *regexStr) {
#ifdef DEBUG
    if (op != LIKE) {
        Logger::log(ERROR,
                    "RecordIterator: internal error: invalid compare op %d "
                    "for LIKE comparision\n",
                    op);
        throw Internal::UnexpedtedOperatorError();
    }
#endif
    try {
        std::regex regex(regexStr);
        return std::regex_match(column.data, regex);
    } catch (std::regex_error &e) {
        Logger::log(ERROR, "RecordIterator: invalid input regex \"%s\"\n",
                    regexStr);
        throw Internal::InvalidRegexError();
    }
}

// static bool

bool RecordIterator::validate(const Columns columns,
                              const CompareConditions &conditions) {
    for (auto &condition : conditions) {
        int columnIndex = table->getColumnIndex(condition.columnName);
        if (columnIndex < 0) {
            throw Internal::InvalidColumnNameError();
        }

        Column &column = columns[columnIndex];

        if (condition.op == IS_NULL || condition.op == NOT_NULL) {
            return _nullComparer(condition.op, column);
        }

        // FIXME: What's the specification to deal with this?
        if (column.isNull) {
            return false;
        }

        if (condition.op == LIKE) {
            if (column.type != VARCHAR) {
                Logger::log(ERROR,
                            "RecordIterator: LIKE can only be used on "
                            "VARCHAR column, but column %s is of type "
                            "%d\n",
                            condition.columnName, column.type);
                throw Internal::InvalidOperatorError();
            }
            return _regexComparer(condition.op, column, condition.value);
        }

        _Comparer comparer;

        switch (column.type) {
            case DataType::INT:
                comparer = _comparer<Internal::_Int>;
                break;
            case DataType::FLOAT:
                comparer = _comparer<Internal::_Float>;
                break;
            case DataType::VARCHAR:
                comparer = _comparer<Internal::_String>;
                break;
        }

        if (!comparer(condition.op, column.data, condition.value)) {
            return false;
        }
    }

    return true;
}

}  // namespace Internal
}  // namespace SimpleDB
