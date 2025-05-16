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
public:
  /// Construct from an existing Dataset (captures its full domain).
  explicit IndexSelection(const Dataset& dataset)
      : dataset_(dataset), base_domain_(dataset.domain) {}

  /// Add or intersect selection of indices where the coordinate variable equals
  /// descriptor.value. Returns a Future for async compatibility.
  ///
  /// For each dimension of the coordinate variable, records the index positions
  /// where the value matches, then intersects with any prior selections on that
  /// dimension.
  template <typename T>
  mdio::Future<void> add_selection(const ValueDescriptor<T>& descriptor) {
    using Index = mdio::Index;
    using Interval = typename Variable<T>::Interval;

    // Lookup coordinate variable by label
    MDIO_ASSIGN_OR_RETURN(auto var,
        dataset_.variables.get<T>(std::string(descriptor.label.label())));

    // Get per-dimension intervals
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());

    // Read coordinate data
    auto fut = var.Read();
    if (!fut.status().ok()) {
      return fut.status();
    }
    auto data = fut.value();

    // Access flattened buffer
    const T* data_ptr     = data.get_data_accessor().data();
    Index offset          = data.get_flattened_offset();
    Index n_samples       = data.num_samples();
    std::vector<Interval> current_pos = intervals;

    // Local map: dimension label -> matching indices
    std::map<std::string, std::vector<Index>> local_matched;

    // Scan all elements
    for (Index idx = offset; idx < offset + n_samples; ++idx) {
      if (data_ptr[idx] == descriptor.value) {
        // On match, capture each dimension's index
        for (const auto& pos : current_pos) {
          std::string dim(pos.label.label());
          local_matched[dim].push_back(pos.inclusive_min);
        }
      }
      _current_position_increment<T>(current_pos, intervals);
    }

    if (local_matched.empty()) {
      return absl::NotFoundError(
          "No matches for coordinate '" + std::string(descriptor.label.label()) + "'");
    }

    // Merge into global selections_
    for (auto& kv : local_matched) {
      const std::string& dim = kv.first;
      auto& vec = kv.second;

      // Deduplicate
      std::sort(vec.begin(), vec.end());
      vec.erase(std::unique(vec.begin(), vec.end()), vec.end());

      // Intersect or assign
      auto it = selections_.find(dim);
      if (it == selections_.end()) {
        selections_.emplace(dim, std::move(vec));
      } else {
        auto& existing = it->second;
        std::vector<Index> intersected;
        std::set_intersection(
            existing.begin(), existing.end(),
            vec.begin(), vec.end(),
            std::back_inserter(intersected));
        existing = std::move(intersected);
      }
    }

    return absl::OkStatus();
  }

  /// \brief Build contiguous RangeDescriptors (stride=1) from selections.
  ///
  /// For each dimension in selections_, coalesces consecutive indices into
  /// RangeDescriptor<Index> with step=1, emitting the minimal set covering all.
  std::vector<RangeDescriptor<Index>> range_descriptors() const {
    using Index = mdio::Index;
    std::vector<RangeDescriptor<Index>> result;

    for (const auto& kv : selections_) {
      const auto& dim = kv.first;
      const auto& idxs = kv.second;
      if (idxs.empty()) continue;

      Index start = idxs.front();
      Index prev = start;

      for (size_t i = 1; i < idxs.size(); ++i) {
        if (idxs[i] == prev + 1) {
          prev = idxs[i];
        } else {
          // close out the current range [start, prev+1)
          result.emplace_back(RangeDescriptor<Index>{dim, start, prev + 1, 1});
          // begin new range
          start = idxs[i];
          prev = idxs[i];
        }
      }
      // flush last range
      result.emplace_back(RangeDescriptor<Index>{dim, start, prev + 1, 1});
    }

    return result;
  }

  /// \brief Get the current selection map: dimension name -> indices.
  /// Dimensions not present imply full range.
  const std::map<std::string, std::vector<mdio::Index>>& selections() const {
    return selections_;
  }

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

  Future<Dataset> SliceAndSort(std::string to_sort_by) {
    auto non_const_ds = dataset_;
    MDIO_ASSIGN_OR_RETURN(auto ds, non_const_ds.isel(static_cast<std::vector<RangeDescriptor<mdio::Index>>&>(range_descriptors())));
    MDIO_ASSIGN_OR_RETURN(auto var, ds.variables.get<mdio::dtypes::int32_t>(to_sort_by));
    auto fut = var.Read();
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());
    auto currentPosition = intervals;
    if (!fut.status().ok()) {
      return fut.status();
    }
    auto dataVar = fut.value();
    auto accessor = dataVar.get_data_accessor().data();
    auto numSamples = dataVar.num_samples();
    auto flattenedOffset = dataVar.get_flattened_offset();

    std::vector<std::pair<int32_t, std::vector<mdio::Variable<int32_t>::Interval>>> indexed_values;
    indexed_values.reserve(numSamples);
    for (mdio::Index i = 0; i < numSamples; ++i) {
      indexed_values.emplace_back(accessor[i], currentPosition);
      _current_position_increment<int32_t>(currentPosition, intervals);
    }
    std::stable_sort(
      indexed_values.begin(),
      indexed_values.end(),
      [](const auto& a, const auto& b) {
        return a.first < b.first;
      }
    );

    std::vector<mdio::Index> trace_indices;
    MDIO_ASSIGN_OR_RETURN(auto transformVar, ds.variables.at("depth_data"));
    auto numTraceSamples = transformVar.num_samples();
    trace_indices.reserve(numTraceSamples);
    for (mdio::Index i = 0; i < numTraceSamples; ++i) {
      trace_indices.push_back(i);
    }

    std::vector<Index> idx0;
    std::vector<Index> idx1;
    std::vector<Index> idx2;
    for (const auto& [_coord, intervals] : indexed_values) {
      idx0.push_back(intervals[0].inclusive_min);
      idx1.push_back(intervals[1].inclusive_min);
      idx2.push_back(intervals[2].inclusive_min);
    }

    auto store = transformVar.get_mutable_store();
    auto input_domain = store.domain();
    auto rank = static_cast<tensorstore::DimensionIndex>(input_domain.rank());
    using ::tensorstore::MakeArrayView;

    // Build an index transform here using array mapping for sorted dimensions
    MDIO_ASSIGN_OR_RETURN(auto transform,
        tensorstore::IndexTransformBuilder<>(rank, rank)
            .input_domain(input_domain)
            .output_index_array(0, 0, 1, ::tensorstore::MakeCopy(::tensorstore::MakeArrayView(idx0)))
            .output_index_array(1, 0, 1, ::tensorstore::MakeCopy(::tensorstore::MakeArrayView(idx1)))
            .output_index_array(2, 0, 1, ::tensorstore::MakeCopy(::tensorstore::MakeArrayView(idx2)))
            .output_single_input_dimension(3, 3)
            .Finalize());

    MDIO_ASSIGN_OR_RETURN(auto new_store, store | transform);
    transformVar.set_store(new_store);
    return ds;
  }

private:
  const Dataset& dataset_;
  tensorstore::IndexDomain<> base_domain_;
  std::map<std::string, std::vector<mdio::Index>> selections_;

  /// Advance a multidimensional odometer position by one step.
  template <typename T>
  void _current_position_increment(
      std::vector<typename Variable<T>::Interval>& position,
      const std::vector<typename Variable<T>::Interval>& interval) const {
    // Walk dimensions from fastest (last) to slowest (first)
    for (std::size_t d = position.size(); d-- > 0; ) {
      if (position[d].inclusive_min + 1 < interval[d].exclusive_max) {
        ++position[d].inclusive_min;
        return;
      }
      position[d].inclusive_min = interval[d].inclusive_min;
    }
    // Should never reach here
  }
};

} // namespace mdio

#endif // MDIO_INTERSECTION_H
