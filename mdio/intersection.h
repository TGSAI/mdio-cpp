#ifndef MDIO_INTERSECTION_H
#define MDIO_INTERSECTION_H

#include "mdio/variable.h"
#include "mdio/dataset.h"
#include "mdio/impl.h"

#include "tensorstore/index_space/index_transform.h"
#include "tensorstore/index_space/index_domain_builder.h"
#include "tensorstore/index_space/index_domain.h"
#include "tensorstore/index_space/dim_expression.h"
#include "tensorstore/array.h"
#include "tensorstore/util/span.h"
#include "tensorstore/index_space/index_transform_builder.h"
#include "tensorstore/box.h"

namespace mdio {


/// \brief Collects valid index selections per dimension for a Dataset without
/// performing slicing immediately.
///
/// Only dimensions explicitly filtered via add_selection appear in the map;
/// any dimension not present should be treated as having its full index range.
class IndexSelection {
struct IndexedInterval;  // Forward declaration
public:
  /// Construct from an existing Dataset (captures its full domain).
  explicit IndexSelection(const Dataset& dataset)
      : dataset_(dataset), base_domain_(dataset.domain) {}

  /// Add or intersect selection of indices where the coordinate variable equals descriptor.value,
  /// preserving full N-D tuple of IndexedInterval metadata.
  template <typename T>
  mdio::Future<void> add_selection(const ValueDescriptor<T>& descriptor) {
    using Index = mdio::Index;
    using VarInterval = typename Variable<T>::Interval;

    // Lookup coordinate variable by label
    MDIO_ASSIGN_OR_RETURN(auto var,
        dataset_.variables.get<T>(std::string(descriptor.label.label())));

    // Get per-dimension intervals
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());

    // Read coordinate data
    auto fut = var.Read();
    if (!fut.status().ok()) return fut.status();
    auto data = fut.value();

    const T* data_ptr     = data.get_data_accessor().data();
    Index offset          = data.get_flattened_offset();
    Index n_samples       = data.num_samples();
    auto current_pos      = intervals;

    // Collect all matching N-D IndexedInterval tuples in this pass
    std::vector<std::vector<IndexedInterval>> local_tuples;
    local_tuples.reserve(n_samples);

    for (Index idx = offset; idx < offset + n_samples; ++idx) {
      if (data_ptr[idx] == descriptor.value) {
        std::vector<IndexedInterval> tuple;
        tuple.reserve(current_pos.size());
        for (auto const& pos : current_pos) {
          IndexedInterval iv;
          iv.label         = pos.label;
          iv.inclusive_min = pos.inclusive_min;
          tuple.push_back(iv);
        }
        local_tuples.push_back(std::move(tuple));
      }
      _current_position_increment<T>(current_pos, intervals);
    }

    if (local_tuples.empty()) {
      std::stringstream ss;
      ss << "No matches for coordinate '" << descriptor.label.label() << "'";
      return absl::NotFoundError(ss.str());
    }

    // Comparator for IndexedInterval tuples: lexicographic by (label, inclusive_min)
    auto tuple_cmp = [](auto const& a, auto const& b) {
      size_t na = a.size(), nb = b.size();
      for (size_t i = 0; i < na && i < nb; ++i) {
        const auto& ai = a[i];
        const auto& bi = b[i];
        auto al = ai.label.label();
        auto bl = bi.label.label();
        if (al != bl) return al < bl;
        if (ai.inclusive_min != bi.inclusive_min) return ai.inclusive_min < bi.inclusive_min;
      }
      return na < nb;
    };

    if (kept_tuples_.empty()) {
      // First descriptor seeds the full tuple set
      kept_tuples_ = std::move(local_tuples);
    } else {
      std::sort(kept_tuples_.begin(), kept_tuples_.end(), tuple_cmp);
      std::sort(local_tuples.begin(),    local_tuples.end(),    tuple_cmp);
      std::vector<std::vector<IndexedInterval>> new_kept;
      std::set_intersection(
        kept_tuples_.begin(), kept_tuples_.end(),
        local_tuples.begin(),  local_tuples.end(),
        std::back_inserter(new_kept),
        tuple_cmp);
      kept_tuples_.swap(new_kept);
    }

    return absl::OkStatus();
  }

  /// \brief Emit a RangeDescriptor per surviving tuple coordinate, without coalescing.
  std::vector<RangeDescriptor<Index>> range_descriptors() const {
    using Index = mdio::Index;
    std::vector<RangeDescriptor<Index>> result;
    if (kept_tuples_.empty()) return result;

    size_t rank = kept_tuples_[0].size();
    // For each dimension from fastest (last) to slowest (0)
    for (int d = static_cast<int>(rank) - 1; d >= 0; --d) {
      // Map context of higher dims -> list of positions in dim d
      std::map<std::vector<Index>, std::vector<Index>> context_map;
      for (auto const& tup : kept_tuples_) {
        std::vector<Index> context;
        context.reserve(rank - d - 1);
        for (size_t k = d + 1; k < rank; ++k)
          context.push_back(tup[k].inclusive_min);
        context_map[context].push_back(tup[d].inclusive_min);
      }

      // For each context, coalesce runs and emit RangeDescriptors
      for (auto& [ctx, coords] : context_map) {
        std::sort(coords.begin(), coords.end());
        coords.erase(std::unique(coords.begin(), coords.end()), coords.end());
        if (coords.empty()) continue;
        Index start = coords.front();
        Index prev = start;
        for (size_t i = 1; i < coords.size(); ++i) {
          if (coords[i] == prev + 1) {
            prev = coords[i];
          } else {
            result.emplace_back(RangeDescriptor<Index>{
              tup_label(d), start, prev + 1, 1});
            start = coords[i]; prev = start;
          }
        }
        result.emplace_back(RangeDescriptor<Index>{
          tup_label(d), start, prev + 1, 1});
      }
    }
    return result;
  }

  // Helper to get dimension label by position d
  std::string_view tup_label(size_t d) const {
    // All tuples have same order of labels
    return kept_tuples_[0][d].label.label();
  }

  /// \brief Get the current selection map: dimension name -> indices.
  /// Dimensions not present imply full range.
  // const std::map<std::string, std::vector<mdio::Index>>& selections() const {
  //   return selections_;
  // }

  /*
  Future<Dataset> SliceAndSort(std::vector<std::string> to_sort_by) {
    // Ensure that all the keys in to_sort_by are present in the Dataset.
    // If not, return an error.
    
    // isel the dataset by `range_descriptors`
    // Begin reading in the Variables and VariableData contained by to_sort_by.

    // Solicit the futures.
    
    // Perform the appropriate sorts. What I need to do for the sorting is map the actual values to the indices.
    // That means that I should be able to sort by value AND have an index mapping which Tensorstore should be able to take as an IndexTransform.
    // If it doesn't accept the full list for sorting, then we are in for sad times! 
    // I'm not sure what it means for this method if 
  }
  */

  /// \brief Slice the dataset by current selections and then stable-sort by one coordinate
  Future<Dataset> SliceAndSort(const std::string& sort_key) {
    // 1. Apply current selections
    auto non_const_ds = dataset_;
    const auto& descs = range_descriptors();
    std::cout << "Number of range descriptors: " << descs.size() << std::endl;
    MDIO_ASSIGN_OR_RETURN(auto ds, non_const_ds.isel(descs));
    
    // 2. Read the sort variable
    MDIO_ASSIGN_OR_RETURN(
      auto var, ds.variables.get<mdio::dtypes::int32_t>(sort_key));
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());
    auto fut = var.Read();
    if (!fut.status().ok()) return fut.status();
    auto dataVar = fut.value();

    // 3. Gather values and corresponding tuples
    std::vector<typename Variable<int32_t>::Interval> pos = intervals;
    const int32_t* acc = dataVar.get_data_accessor().data();
    mdio::Index offset = dataVar.get_flattened_offset();
    mdio::Index n = dataVar.num_samples();

    std::vector<std::pair<int32_t, std::vector<IndexedInterval>>> indexed;
    indexed.reserve(n);
    for (mdio::Index i = 0; i < n; ++i) {
      // build tuple of IndexedInterval
      std::vector<IndexedInterval> tup;
      tup.reserve(pos.size());
      for (auto const& iv : pos) {
        IndexedInterval tmp{iv.label, iv.inclusive_min};
        tup.push_back(tmp);
      }
      indexed.emplace_back(acc[i + offset], tup);
      _current_position_increment<int32_t>(pos, intervals);
    }

    // 4. Stable sort by value
    std::stable_sort(
      indexed.begin(), indexed.end(),
      [](auto const& a, auto const& b){ return a.first < b.first; });

    // 5. Extract new tuples into kept_tuples_
    kept_tuples_.clear();
    kept_tuples_.reserve(indexed.size());
    for (auto& pr : indexed) {
      kept_tuples_.push_back(std::move(pr.second));
    }

    return ds;
  }

  /// \brief Access the surviving N-D IndexedInterval tuples directly.
  const std::vector<std::vector<IndexedInterval>>& tuples() const {
    return kept_tuples_;
  }

private:
  const Dataset& dataset_;
  tensorstore::IndexDomain<> base_domain_;
  std::vector<std::vector<IndexedInterval>> kept_tuples_;

  /// A minimal struct carrying label and position
  struct IndexedInterval {
    mdio::DimensionIdentifier label;
    mdio::Index inclusive_min;
    IndexedInterval() = default;
    IndexedInterval(mdio::DimensionIdentifier l, mdio::Index i)
      : label(l), inclusive_min(i) {}

    bool operator<(IndexedInterval const& o) const {
      auto a = label.label();
      auto b = o.label.label();
      if (a != b) return a < b;
      return inclusive_min < o.inclusive_min;
    }
    bool operator==(IndexedInterval const& o) const {
      return label.label() == o.label.label() &&
             inclusive_min  == o.inclusive_min;
    }
  };

  /// Advance a multidimensional odometer position by one step.
  template <typename T>
  void _current_position_increment(
      std::vector<typename Variable<T>::Interval>& position,
      const std::vector<typename Variable<T>::Interval>& interval) const {
    for (std::size_t d = position.size(); d-- > 0; ) {
      if (position[d].inclusive_min + 1 < interval[d].exclusive_max) {
        ++position[d].inclusive_min;
        return;
      }
      position[d].inclusive_min = interval[d].inclusive_min;
    }
  }
};

} // namespace mdio

#endif // MDIO_INTERSECTION_H
