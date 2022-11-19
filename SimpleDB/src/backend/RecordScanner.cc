#include "internal/RecordScanner.h"

#include <cmath>
#include <regex>
#include <utility>

#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

RecordScanner::RecordScanner(Table *table) : table(table) {}

int RecordScanner::iterate(CompareConditions conditions,
                           IteratorFunc callback) {
    Logger::log(VERBOSE,
                "RecordScanner: scanning through all records of the table\n");
    int numReturns = 0;
    Columns bufColumns;
    for (int page = 1; page < table->meta.numUsedPages; page++) {
        PageHandle *handle = table->getHandle(page);
        for (int slot = 1; slot < NUM_SLOT_PER_PAGE; slot++) {
            if (table->occupied(*handle, slot)) {
                RecordID rid = {page, slot};
                table->get(rid, bufColumns);
                if (validate(bufColumns, conditions)) {
                    bool continueIteration =
                        callback(numReturns, rid, bufColumns);
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

int RecordScanner::iterate(CompareConditions conditions,
                           GetNextRecordFunc getNext, IteratorFunc callback) {
    int numReturns = 0;
    RecordID id;
    Columns bufColumns;

    while (id = getNext(), id.page != -1) {
        table->get(id, bufColumns);
        if (validate(bufColumns, conditions)) {
            callback(numReturns, id, bufColumns);
            numReturns++;
        }
    }

    return numReturns;
}

int RecordScanner::iterate(IteratorFunc callback) {
    return iterate(CompareConditions(), callback);
}

int RecordScanner::iterate(GetNextRecordFunc getNext, IteratorFunc callback) {
    return iterate(CompareConditions(), getNext, callback);
}

std::pair<RecordID, Columns> RecordScanner::findFirst(
    CompareConditions conditions) {
    RecordID rid = RecordID::NULL_RECORD;
    Columns bufColumns;
    iterate(conditions, [&](int index, RecordID id, const Columns &columns) {
        bufColumns = std::move(columns);
        rid = id;
        return false;
    });

    return {rid, bufColumns};
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
                        "RecordScanner: internal error: invalid compare op %d "
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
                        "RecordScanner: internal error: invalid compare op %d "
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
                    "RecordScanner: internal error: invalid compare op %d "
                    "for LIKE comparision\n",
                    op);
        throw Internal::UnexpedtedOperatorError();
    }
#endif
    try {
        std::regex regex(regexStr);
        return std::regex_match(column.data.stringValue, regex);
    } catch (std::regex_error &e) {
        Logger::log(ERROR, "RecordScanner: invalid input regex \"%s\"\n",
                    regexStr);
        throw Internal::InvalidRegexError();
    }
}

// static bool

bool RecordScanner::validate(const Columns &columns,
                             const CompareConditions &conditions) {
    for (auto &condition : conditions) {
        int columnIndex = table->getColumnIndex(condition.columnName);
        if (columnIndex < 0) {
            throw Internal::InvalidColumnNameError();
        }

        const Column &column = columns[columnIndex];

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
                            "RecordScanner: LIKE can only be used on "
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

        if (!comparer(condition.op, column.data.stringValue, condition.value)) {
            return false;
        }
    }

    return true;
}

}  // namespace Internal
}  // namespace SimpleDB
