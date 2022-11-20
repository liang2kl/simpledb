#include "internal/QueryFilter.h"

#include <regex>

#include "Error.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

// ===== Begin AggregatedFilter =====
bool AggregatedFilter::apply(Columns &columns) {
    for (auto filter : filters) {
        if (!filter->apply(columns)) {
            return false;
        }
    }
    return true;
}
// ====== End AggregatedFilter ======

// ===== Begin LimitFilter =====
bool LimitFilter::apply(Columns &columns) {
    if (count == limit) {
        return false;
    }

    count++;
    return true;
}
// ====== End LimitFilter ======

// ===== Begin SelectFilter =====
bool SelectFilter::apply(Columns &columns) {
    Columns newColumns;
    // TODO: Optimization.

    for (auto &name : columnNames) {
        int index = table->getColumnIndex(name);
        if (index != -1) {
            newColumns.push_back(columns[index]);
        } else {
            throw ColumnNotFoundError(name);
        }
    }

    columns = newColumns;
    return true;
}
// ====== End SelectFilger ======

// ===== Begin ConditionFilter =====
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

bool ConditionFilter::apply(Columns &columns) {
    int columnIndex = table->getColumnIndex(condition.columnName);
    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(condition.columnName);
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

    return comparer(condition.op, column.data.stringValue, condition.value);
}
// ====== End ConditionFilter ======

}  // namespace Internal
}  // namespace SimpleDB
