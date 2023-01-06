#include "internal/QueryFilter.h"

#include <cassert>
#include <cstdint>
#include <limits>
#include <regex>

#include "Error.h"
#include "internal/Comparer.h"
#include "internal/Logger.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

// ===== Begin AggregatedFilter =====
void AggregatedFilter::build() {
    for (auto &filter : filters) {
        filter->build();
    }
}

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
bool AggregatedFilter::finalize(Columns &columns) {
    bool ret = false;
    for (auto filter : filters) {
        if (filter->finalize(columns)) {
            ret = true;
        }
    }
    return ret;
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
void SelectFilter::build() {
    for (int i = 0; i < selectors.size(); i++) {
        const auto &selector = selectors[i];
        if (selector.type == QuerySelector::COLUMN) {
            isAggregated = false;
            auto index = table->getColumnIndex(selector.column);
            if (index < 0) {
                throw ColumnNotFoundError(selector.column.getDesc());
            }
            selectIndexes.push_back(index);
        } else {
            // Aggregated column.
            isAggregated = true;
            if (selector.type != QuerySelector::COUNT_STAR) {
                auto index = table->getColumnIndex(selector.column);
                if (index < 0) {
                    throw ColumnNotFoundError(selector.column.getDesc());
                }
                if (table->columns[index].type == VARCHAR) {
                    throw AggregatorError(
                        "Cannot aggregate on VARCHAR columns");
                }
                selectIndexes.push_back(index);
            } else {
                selectIndexes.push_back(-1);
            }
        }
    }

    if (isAggregated) {
        selectContexts.resize(selectors.size());
    }
}

std::pair<bool, bool> SelectFilter::apply(Columns &columns) {
    Columns newColumns;
    // TODO: Optimization.

    if (!isAggregated) {
        for (int index : selectIndexes) {
            newColumns.push_back(columns[index]);
        }
    } else {
        for (size_t i = 0; i < selectors.size(); i++) {
            auto &selector = selectors[i];
            auto &context = selectContexts[i];
            if (selector.type == QuerySelector::COUNT_STAR) {
                // COUNT(*)
                context.initializeInt(0);
                context.value.intValue++;
            } else {
                assert(columns.size() == table->columns.size());
                const auto &columnInfo = table->columns[selectIndexes[i]];
                bool isNull = columns[selectIndexes[i]].isNull;
                if (!isNull) {
                    context.count++;
                }

#define AGGREGATE(_type, _Type)                                             \
    auto value = columns[selectIndexes[i]].data._type;                      \
    switch (selector.type) {                                                \
        case QuerySelector::COUNT_COL:                                      \
            context.initializeInt(0);                                       \
            context.value.intValue += (isNull ? 0 : 1);                     \
            break;                                                          \
        case QuerySelector::MIN:                                            \
            if (!isNull) {                                                  \
                context.initialize##_Type(value);                           \
                context.value._type = std::min(context.value._type, value); \
            }                                                               \
            break;                                                          \
        case QuerySelector::MAX:                                            \
            if (!isNull) {                                                  \
                context.initialize##_Type(value);                           \
                context.value._type = std::max(context.value._type, value); \
            }                                                               \
            break;                                                          \
        case QuerySelector::SUM:                                            \
        case QuerySelector::AVG:                                            \
            if (!isNull) {                                                  \
                context.initialize##_Type(0);                               \
                context.value._type += value;                               \
            }                                                               \
            break;                                                          \
        default:                                                            \
            assert(false);                                                  \
    }
                if (columnInfo.type == INT) {
                    AGGREGATE(intValue, Int);
                } else {
                    AGGREGATE(floatValue, Float);
                }
#undef AGGREGATE
            }
        }
        // Not accepting aggregated columns before finalizing.
        return {false, true};
    }

    columns = newColumns;
    return {true, true};
}

bool SelectFilter::finalize(Columns &columns) {
    if (!isAggregated) {
        return false;
    }
    columns.resize(selectors.size());
    for (int i = 0; i < selectors.size(); i++) {
        const auto &context = selectContexts[i];
        const auto &selector = selectors[i];
        columns[i].isNull = context.isNull;

        if (!context.isNull) {
            DataType dataType = selectIndexes[i] == -1
                                    ? INT
                                    : table->columns[selectIndexes[i]].type;
#define CALC_AGGREGATION(_type, _Type)                         \
    switch (selector.type) {                                   \
        case QuerySelector::AVG:                               \
            columns[i].data.floatValue =                       \
                float(context.value._type) / context.count;    \
            columns[i].type = FLOAT;                           \
            break;                                             \
        case QuerySelector::MIN:                               \
        case QuerySelector::MAX:                               \
        case QuerySelector::SUM:                               \
            columns[i].data._type = context.value._type;       \
            columns[i].type = _Type;                           \
            break;                                             \
        case QuerySelector::COUNT_COL:                         \
        case QuerySelector::COUNT_STAR:                        \
            columns[i].data.intValue = context.value.intValue; \
            columns[i].type = INT;                             \
            break;                                             \
        default:                                               \
            assert(false);                                     \
    }
            if (dataType == INT) {
                CALC_AGGREGATION(intValue, INT);
            } else {
                CALC_AGGREGATION(floatValue, FLOAT);
            }
#undef CALC_AGGREGATION
        } else { /* context.isNull */
            if (selector.type == QuerySelector::COUNT_COL ||
                selector.type == QuerySelector::COUNT_STAR) {
                columns[i].isNull = false;
                columns[i].data.intValue = 0;
            }
        }
    }

    return true;
}
// ====== End SelectFilter ======

// ===== Begin NullConditionFilter =====
void NullConditionFilter::build() {
    columnIndex = table->getColumnIndex(condition.columnId);
    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(condition.columnId.getDesc());
    }
}

std::pair<bool, bool> NullConditionFilter::apply(Columns &columns) {
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
//                     "RecordScanner: internal error: invalid compare op %d
//                     " "for LIKE comparision\n", op);
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

void ValueConditionFilter::build() {
    columnIndex = table->getColumnIndex(condition.columnId);
    if (columnIndex < 0) {
        throw Internal::ColumnNotFoundError(condition.columnId.getDesc());
    }
}

static _Comparer _getComparer(DataType type) {
    switch (type) {
        case DataType::INT:
            return _comparer<Internal::_Int>;
        case DataType::FLOAT:
            return _comparer<Internal::_Float>;
        case DataType::VARCHAR:
            return _comparer<Internal::_String>;
        default:
            assert(false);
    }
}

std::pair<bool, bool> ValueConditionFilter::apply(Columns &columns) {
    const Column &column = columns[columnIndex];

    // FIXME: What's the specification to deal with this?
    if (column.isNull) {
        return {false, true};
    }

    _Comparer comparer = _getComparer(column.type);

    return {comparer(condition.op, column.data.stringValue,
                     condition.value.stringValue),
            true};
}
// ====== End ValueConditionFilter ======

// ===== Begin ColumnConditionFilter =====
void ColumnConditionFilter::build() {
    columnIndex1 = table->getColumnIndex(condition.lhs);
    columnIndex2 = table->getColumnIndex(condition.rhs);
    if (columnIndex1 < 0) {
        throw Internal::ColumnNotFoundError(condition.lhs.getDesc());
    }
    if (columnIndex2 < 0) {
        throw Internal::ColumnNotFoundError(condition.rhs.getDesc());
    }
}

std::pair<bool, bool> ColumnConditionFilter::apply(Columns &columns) {
    const Column &column1 = columns[columnIndex1];
    const Column &column2 = columns[columnIndex2];

    // FIXME: Is this necessary?
    assert(column1.type == column2.type);

    _Comparer comparer = _getComparer(column1.type);

    return {comparer(condition.op, column1.data.stringValue,
                     column2.data.stringValue),
            true};
}

std::string QuerySelector::getColumnName() const {
    std::string desc = column.getDesc();
    switch (type) {
        case COLUMN:
            return desc;
        case COUNT_STAR:
            return "COUNT(*)";
        case COUNT_COL:
            return "COUNT(" + desc + ")";
        case SUM:
            return "SUM(" + desc + ")";
        case AVG:
            return "AVG(" + desc + ")";
        case MIN:
            return "MIN(" + desc + ")";
        case MAX:
            return "MAX(" + desc + ")";
    }
}

}  // namespace Internal
}  // namespace SimpleDB
