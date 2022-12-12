#include "internal/QueryFilter.h"

#include <regex>

#include "Error.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

// ===== Begin AggregatedFilter =====
std::pair<bool, bool> AggregatedFilter::apply(Columns &columns) {
    bool stop = false;
    for (auto filter : filters) {
        auto [accept, continue_] = filter->apply(columns);
        if (!continue_) {
            stop = true;
        }
        if (!accept) {
            return {false, !stop};
        }
    }
    return {true, !stop};
}
// ====== End AggregatedFilter ======

// ===== Begin LimitFilter =====
std::pair<bool, bool> LimitFilter::apply(Columns &columns) {
    count++;
    if (limit < 0) {
        return {true, true};
    }

    if (count > limit) {
        return {false, false};
    }

    if (count == limit) {
        return {true, false};
    }

    return {true, true};
}
// ====== End LimitFilter ======

// ===== Begin OffsetFilter =====
std::pair<bool, bool> OffsetFilter::apply(Columns &columns) {
    count++;
    if (count <= offset) {
        return {false, true};
    }
    return {true, true};
}
// ====== End OffsetFilter ======

// ===== Begin SelectFilter =====
std::pair<bool, bool> SelectFilter::apply(Columns &columns) {
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
    return {true, true};
}
// ====== End SelectFilter ======

// ===== Begin NullConditionFilter =====
std::pair<bool, bool> NullConditionFilter::apply(Columns &columns) {
    int columnIndex = table->getColumnIndex(condition.columnName);
    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(condition.columnName);
    }

    const Column &column = columns[columnIndex];

    bool accept = (condition.isNull && column.isNull) ||
                  (!condition.isNull && !column.isNull);
    return {accept, true};
}
// ====== End NullConditionFilter ======

// ===== Begin ValueConditionFilter =====
// static bool _regexComparer(CompareOp op, const Column &column,
//                            const char *regexStr) {
// #ifdef DEBUG
//     if (op != LIKE) {
//         Logger::log(ERROR,
//                     "RecordScanner: internal error: invalid compare op %d "
//                     "for LIKE comparision\n",
//                     op);
//         throw Internal::UnexpedtedOperatorError();
//     }
// #endif
//     try {
//         std::regex regex(regexStr);
//         return std::regex_match(column.data.stringValue, regex);
//     } catch (std::regex_error &e) {
//         Logger::log(ERROR, "RecordScanner: invalid input regex \"%s\"\n",
//                     regexStr);
//         throw Internal::InvalidRegexError();
//     }
// }

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

std::pair<bool, bool> ValueConditionFilter::apply(Columns &columns) {
    int columnIndex = table->getColumnIndex(condition.columnName);
    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(condition.columnName);
    }

    const Column &column = columns[columnIndex];

    // FIXME: What's the specification to deal with this?
    if (column.isNull) {
        return {false, true};
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

    return {comparer(condition.op, column.data.stringValue,
                     condition.value.stringValue),
            true};
}
// ====== End ValueConditionFilter ======

}  // namespace Internal
}  // namespace SimpleDB
