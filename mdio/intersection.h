#ifndef MDIO_INTERSECTION_H
#define MDIO_INTERSECTION_H

#include "mdio/variable.h"
#include "mdio/dataset.h"
#include "mdio/impl.h"

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
