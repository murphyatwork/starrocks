// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized//sorting//sort_permute.h"
#include "exec/vectorized//sorting//sorting.h"
#include "runtime/timestamp_value.h"

namespace starrocks::vectorized {

// Comparator for sort
template <class T>
struct SorterComparator {
    static int compare(const T& lhs, const T& rhs) {
        if (lhs == rhs) {
            return 0;
        } else if (lhs < rhs) {
            return -1;
        } else {
            return 1;
        }
    }
};

template <>
struct SorterComparator<Slice> {
    static int compare(const Slice& lhs, const Slice& rhs) { return lhs.compare(rhs); }
};

template <>
struct SorterComparator<DateValue> {
    static int compare(const DateValue& lhs, const DateValue& rhs) {
        auto x = lhs.julian() - rhs.julian();
        if (x == 0) {
            return x;
        } else {
            return x > 0 ? 1 : -1;
        }
    }
};

template <>
struct SorterComparator<TimestampValue> {
    static int compare(TimestampValue lhs, TimestampValue rhs) {
        auto x = lhs.timestamp() - rhs.timestamp();
        if (x == 0) {
            return x;
        } else {
            return x > 0 ? 1 : -1;
        }
    }
};

template <class PermutationType>
static std::string dubug_column(const Column* column, const PermutationType& permutation) {
    std::string res;
    for (auto p : permutation) {
        res += fmt::format("{:>5}, ", column->debug_item(p.index_in_chunk));
    }
    return res;
}



} // namespace starrocks::vectorized