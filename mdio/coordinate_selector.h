// Copyright 2025 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MDIO_COORDINATE_SELECTOR_H_
#define MDIO_COORDINATE_SELECTOR_H_

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "mdio/dataset.h"
#include "mdio/impl.h"
#include "mdio/variable.h"
#include "tensorstore/array.h"
#include "tensorstore/box.h"
#include "tensorstore/index_space/dim_expression.h"
#include "tensorstore/index_space/index_domain.h"
#include "tensorstore/index_space/index_domain_builder.h"
#include "tensorstore/index_space/index_transform.h"
#include "tensorstore/index_space/index_transform_builder.h"
#include "tensorstore/util/span.h"

// #define MDIO_INTERNAL_PROFILING 0  // TODO(BrianMichell): Remove simple
// profiling code once we approach a more mature API access.

namespace mdio {

#ifdef MDIO_INTERNAL_PROFILING
void timer(std::chrono::high_resolution_clock::time_point start) {
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  std::cout << "Time taken: " << duration.count() << " microseconds"
            << std::endl;
}
#endif

// — helper to tag multi-key sorts ——
template <typename T>
struct SortKey {
  std::string key;
  using value_type = T;
};

// — trait to detect ValueDescriptor<T> ——
template <typename D>
struct is_value_descriptor : std::false_type {};
template <typename T>
struct is_value_descriptor<ValueDescriptor<T>> : std::true_type {};
template <typename D>
inline constexpr bool is_value_descriptor_v =
    is_value_descriptor<std::decay_t<D>>::value;

// — trait to detect SortKey<T> ——
template <typename D>
struct is_sort_key : std::false_type {};
template <typename T>
struct is_sort_key<SortKey<T>> : std::true_type {};
template <typename D>
inline constexpr bool is_sort_key_v = is_sort_key<std::decay_t<D>>::value;

/// \brief Collects valid index selections per dimension for a Dataset without
/// performing slicing immediately.
///
/// Only dimensions explicitly filtered via filterByCoordinate appear in the
/// map; any dimension not present should be treated as having its full index
/// range.
class CoordinateSelector {
 public:
  /// Construct from an existing Dataset (captures its full domain).
  explicit CoordinateSelector(Dataset& dataset)
      : dataset_(dataset), base_domain_(dataset.domain) {}

   template<typename... OutTs, typename... Ops>
  Future<std::tuple<std::vector<OutTs>...>> ReadDataVariables(
      std::vector<std::string> const& data_variables,
      Ops const&...                   ops)
  {
    if (data_variables.size() != sizeof...(OutTs)) {
      return absl::InvalidArgumentError(
        "ReadDataVariables: number of names must match number of OutTs");
    }
    // 1) apply all filters & sorts in order
    absl::Status st = absl::OkStatus();
    ((st = st.ok() ? _applyOp(ops).status() : st), ...);
    if (!st.ok()) return st;

    // 2) kick off and await all reads
    return _readMultiple<OutTs...>(data_variables);
  }

  void reset() {
    kept_runs_.clear();
  }

  /**
   * @brief Filter the Dataset by the given coordinate.
   * Limitations:
   * - Only a single filter is currently tested.
   * - A bug exists if the filter value does not make a perfect hyper-rectangle
   * within its dimensions.
   *
   */
  template <typename T>
  mdio::Future<void> filterByCoordinate(const ValueDescriptor<T>& descriptor) {
    if (kept_runs_.empty()) {
      return _init_runs(descriptor);
    } else {
      return _add_new_run(descriptor);
    }
  }

  template <typename T>
  Future<void> sortSelectionByKey(const std::string& sort_key) {
#ifdef MDIO_INTERNAL_PROFILING
    auto start = std::chrono::high_resolution_clock::now();
#endif
    const size_t n = kept_runs_.size();

    // 1) Fire off all reads in parallel and gather the key values
    std::vector<Future<VariableData<void>>> reads;
    reads.reserve(n);
    for (auto const& desc : kept_runs_) {
      MDIO_ASSIGN_OR_RETURN(auto ds, dataset_.isel(desc));
      MDIO_ASSIGN_OR_RETURN(auto var, ds.variables.at(sort_key));
      reads.push_back(var.Read());
    }

    std::vector<T> keys;
    keys.reserve(n);
#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Set up sorting of  " << sort_key << " ... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif
    for (auto& f : reads) {
      // if (!f.status().ok()) return f.status();
      // auto data = f.value();
      // keys.push_back(data.get_data_accessor().data()[data.get_flattened_offset()]);
      MDIO_ASSIGN_OR_RETURN(auto resolution, _resolve_future<void>(f));
      auto data = std::get<0>(resolution);
      auto data_ptr = static_cast<const T*>(std::get<1>(resolution));
      auto offset = std::get<2>(resolution);
      // auto n = std::get<3>(resolution);  // Not required
      keys.push_back(data_ptr[offset]);
    }
#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Waiting for reads to complete for " << sort_key << " ... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif

    // 2) Build and stable-sort an index array [0…n-1] by key
    std::vector<size_t> idx(n);
    std::iota(idx.begin(), idx.end(), 0);
    std::stable_sort(idx.begin(), idx.end(),
                     [&](size_t a, size_t b) { return keys[a] < keys[b]; });
#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Sorting time for " << sort_key << " ... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif

    // 3) One linear, move-only pass into a temp buffer
    using Desc = std::decay_t<decltype(kept_runs_)>::value_type;
    std::vector<Desc> tmp;
    tmp.reserve(n);
    for (size_t new_pos = 0; new_pos < n; ++new_pos) {
      tmp.emplace_back(std::move(kept_runs_[idx[new_pos]]));
    }

    // 4) Steal the buffer back
    kept_runs_ = std::move(tmp);
#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Stealing buffer back time for " << sort_key << " ... ";
    timer(start);
#endif
    return absl::OkStatus();
  }

  template <typename T>
  Future<std::vector<T>> readSelection(const std::string& output_variable) {
#ifdef MDIO_INTERNAL_PROFILING
    auto start = std::chrono::high_resolution_clock::now();
#endif
    std::vector<Future<VariableData<void>>> reads;
    reads.reserve(kept_runs_.size());
    std::vector<T> ret;

    for (const auto& desc : kept_runs_) {
      MDIO_ASSIGN_OR_RETURN(auto ds, dataset_.isel(desc));
      MDIO_ASSIGN_OR_RETURN(auto var, ds.variables.at(output_variable));
      auto fut = var.Read();
      reads.push_back(fut);
      if (var.rank() == 1) {
        break;
      }
    }

#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Set up reading of " << output_variable << " ... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif

    for (auto& f : reads) {
      MDIO_ASSIGN_OR_RETURN(auto resolution, _resolve_future<void>(f));
      auto data = std::get<0>(resolution);
      auto data_ptr = static_cast<const T*>(std::get<1>(resolution));
      auto offset = std::get<2>(resolution);
      auto n = std::get<3>(resolution);
      std::vector<T> buffer(n);
      std::memcpy(buffer.data(), data_ptr + offset, n * sizeof(T));
      ret.insert(ret.end(), buffer.begin(), buffer.end());
    }

#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Reading time for " << output_variable << " ... ";
    timer(start);
#endif
    return ret;
  }

 private:
  Dataset& dataset_;
  tensorstore::IndexDomain<> base_domain_;
  std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> kept_runs_;
  std::map<std::string_view, VariableData<void>> cached_variables_;

  template <typename D>
  Future<void> _applyOp(D const& op) {
    if constexpr (is_value_descriptor_v<D>) {
      auto fut = filterByCoordinate(op);
      if (!fut.status().ok()) {
        return fut.status();
      }
      return absl::OkStatus();
    } else if constexpr (is_sort_key_v<D>) {
      using SortT = typename std::decay_t<D>::value_type;
      auto fut = sortSelectionByKey<SortT>(op.key);
      if (!fut.status().ok()) {
        return fut.status();
      }
      return absl::OkStatus();
    } else {
      return absl::UnimplementedError(
          "query(): RangeDescriptor and ListDescriptor not supported");
    }
  }

  // helper: expands readSelection<OutTs>(vars[I])...
  template<typename... OutTs, std::size_t... I>
  Future<std::tuple<std::vector<OutTs>...>> _readMultipleImpl(
      std::vector<std::string> const& vars,
      std::index_sequence<I...>)
  {
    // 1) start all reads
    auto futs = std::make_tuple(
      readSelection<OutTs>(vars[I])...
    );

    // 2) wait on them in order
    absl::Status st = absl::OkStatus();
    std::tuple<std::vector<OutTs>...> results;
    // fold over I...
    (
      [&](){
        if (!st.ok()) return;
        auto& f = std::get<I>(futs);
        st = f.status();
        if (st.ok()) std::get<I>(results) = std::move(f.value());
      }(),
      ...
    );
    if (!st.ok()) return st;
    return results;
  }

  template<typename... OutTs>
  Future<std::tuple<std::vector<OutTs>...>> _readMultiple(
      std::vector<std::string> const& vars)
  {
    return _readMultipleImpl<OutTs...>(
      vars, std::index_sequence_for<OutTs...>{}
    );
  }

  /*
  TODO: The built RangeDescriptors aren't behaving as I hoped.
  They are building the longest runs possible properly, however
  as it becomes disjointed we start to lose some info.

  e.g. We can have [0,1], [0, 25], [0, 120] but
  the last dimension is actually [0, 1000].

  What we should get instead is [0, 1], [0, 24], [0, 1000] and [0, 1], [24, 25],
  [0, 120]
  */

  template <typename T>
  Future<void> _init_runs(const ValueDescriptor<T>& descriptor) {
    using Interval = typename Variable<void>::Interval;
#ifdef MDIO_INTERNAL_PROFILING
    auto start = std::chrono::high_resolution_clock::now();
#endif
    MDIO_ASSIGN_OR_RETURN(auto var, dataset_.variables.at(
                                        std::string(descriptor.label.label())));

    const T* data_ptr;
    Index offset;
    Index n_samples;
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());
    if (cached_variables_.find(descriptor.label.label()) == cached_variables_.end()) {
      // TODO(BrianMichell): Ensure that the domain has not changed.
      std::cout << "Reading VariableData" << std::endl;
      auto fut = var.Read();
      MDIO_ASSIGN_OR_RETURN(auto resolution, _resolve_future<void>(fut));
      auto dataToCache = std::get<0>(resolution);
      cached_variables_.insert_or_assign(descriptor.label.label(), std::move(dataToCache));
    }
    auto it = cached_variables_.find(descriptor.label.label());
    if (it == cached_variables_.end()) {
      std::stringstream ss;
      ss << "Cached variable not found for coordinate '" << descriptor.label.label() << "'";
      return absl::NotFoundError(ss.str());
    }
   auto& data = it->second;
    data_ptr = static_cast<const T*>(data.get_data_accessor().data());
    offset = data.get_flattened_offset();
    n_samples = data.num_samples();

    auto current_pos = intervals;
    bool isInRun = false;
    std::vector<std::vector<Interval>> local_runs;

    std::size_t run_idx = offset;

#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Initialize and read time... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif

    for (mdio::Index idx = offset; idx < offset + n_samples; ++idx) {
      bool is_match = data_ptr[idx] == descriptor.value;

      if (is_match && !isInRun) {
        // The start of a new run
        isInRun = true;
        for (auto i = run_idx; i < idx; ++i) {
          // _current_position_increment<T>(current_pos, intervals);
          _current_position_increment<void>(current_pos, intervals);
        }
        // _current_position_stride<T>(current_pos, intervals, idx - run_idx);
        run_idx = idx;
        std::vector<Interval> run = current_pos;
        local_runs.push_back(std::move(run));
      } else if (is_match && isInRun) {
        // Somewhere in the middle of a run
        // do nothing TODO: Remove me
      } else if (!is_match && isInRun) {
        // The end of a run
        isInRun = false;
        // Use 1 less than the current index to ensure we get the correct end
        // location.
        for (auto i = run_idx; i < idx - 1; ++i) {
          _current_position_increment<void>(current_pos, intervals);
        }
        run_idx = idx;
        auto& last_run = local_runs.back();
        for (auto i = 0; i < current_pos.size(); ++i) {
          last_run[i].exclusive_max = current_pos[i].inclusive_min + 1;
        }
        // We need to advance to the actual current position
        _current_position_increment<void>(current_pos, intervals);
      } else if (!is_match && !isInRun) {
        // No run at all
        // do nothing TODO: Remove me
      } else {
        // base case TODO: Remove me
      }
    }

#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Build runs time... ";
    timer(start);
    start = std::chrono::high_resolution_clock::now();
#endif

    if (local_runs.empty()) {
      std::stringstream ss;
      ss << "No matches for coordinate '" << descriptor.label.label() << "'";
      return absl::NotFoundError(ss.str());
    }

    kept_runs_ = _from_intervals<void>(local_runs);
#ifdef MDIO_INTERNAL_PROFILING
    std::cout << "Finalize time... ";
    timer(start);
#endif
    return absl::OkStatus();
  }

  /**
   * @brief Using the existing runs, further filter the Dataset by the new
   * coordiante.
   */
  template <typename T>
  Future<void> _add_new_run(const ValueDescriptor<T>& descriptor) {
    using Interval = typename Variable<T>::Interval;
    std::vector<std::vector<Interval>> new_runs;

    std::vector<std::vector<Interval>>
        stored_intervals;  // Use this to ensure everything remains in memory
                           // until the Intervals are no longer needed.
    stored_intervals.reserve(kept_runs_.size());


    bool is_first_run = true;

    for (const auto& desc : kept_runs_) {
      MDIO_ASSIGN_OR_RETURN(auto ds, dataset_.isel(desc));
      MDIO_ASSIGN_OR_RETURN(
          auto var, ds.variables.get<T>(std::string(descriptor.label.label())));
      auto fut = var.Read();
      MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());

      if (is_first_run) {
        is_first_run = false;
        if (intervals.size() != kept_runs_[0].size()) {
          std::cout << "WARNING: Different coordinate dimensions detected. "
                       "This behavior is not yet supported."
                    << std::endl;
          std::cout
              << "\tFor expected behavior, please ensure all previous "
                 "dimensions are less than or equal to the current dimension."
              << std::endl;
        }
      }

      stored_intervals.push_back(std::move(
          intervals));  // Just to ensure nothing gets freed prematurely.
      MDIO_ASSIGN_OR_RETURN(auto resolution, _resolve_future<T>(fut));
      auto data = std::get<0>(resolution);
      auto data_ptr = std::get<1>(resolution);
      auto offset = std::get<2>(resolution);
      auto n = std::get<3>(resolution);

      auto current_pos = intervals;
      bool isInRun = false;

      std::size_t run_idx = offset;

      for (Index idx = offset; idx < offset + n; ++idx) {
        bool is_match = data_ptr[idx] == descriptor.value;
        if (is_match && !isInRun) {
          isInRun = true;
          for (auto i = run_idx; i < idx; ++i) {
            _current_position_increment<T>(current_pos, intervals);
          }
          run_idx = idx;
          std::vector<Interval> run = current_pos;
          new_runs.push_back(std::move(run));
        } else if (is_match && isInRun) {
          // Somewhere in the middle of a run
          // do nothing TODO: Remove me
        } else if (!is_match && isInRun) {
          // The end of a run
          // TODO(BrianMichell): Ensure we are using the correct index (see
          // above)
          isInRun = false;
          for (auto i = run_idx; i < idx; ++i) {
            _current_position_increment<T>(current_pos, intervals);
          }
          run_idx = idx;
          auto& last_run = new_runs.back();
          for (auto i = 0; i < current_pos.size(); ++i) {
            last_run[i].exclusive_max = current_pos[i].inclusive_min + 1;
          }
        } else if (!is_match && !isInRun) {
          // No run at all
          // do nothing TODO: Remove me
        } else {
          // base case TODO: Remove me
        }
      }
    }

    if (new_runs.empty()) {
      std::stringstream ss;
      ss << "No matches for coordinate '" << descriptor.label.label() << "'";
      return absl::NotFoundError(ss.str());
    }

    kept_runs_ = _from_intervals<T>(
        new_runs);  // TODO(BrianMichell): We need to ensure we don't
                    // accidentally drop any pre-sliced dimensions...
    return absl::OkStatus();
  }

  template <typename T>
  std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> _from_intervals(
      std::vector<std::vector<typename mdio::Variable<T>::Interval>>&
          intervals) {
    std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> ret;
    ret.reserve(intervals.size());
    for (auto const& run : intervals) {
      std::vector<mdio::RangeDescriptor<mdio::Index>> run_descs;
      run_descs.reserve(run.size());
      for (auto const& interval : run) {
        run_descs.emplace_back(mdio::RangeDescriptor<mdio::Index>{
            interval.label, interval.inclusive_min, interval.exclusive_max, 1});
      }
      ret.push_back(std::move(run_descs));
    }
    return ret;
  }

  /// Advance a multidimensional odometer position by one step.
  template <typename T>
  void _current_position_increment(
      std::vector<typename Variable<T>::Interval>&
          position,  // NOLINT (non-const)
      const std::vector<typename Variable<T>::Interval>& interval) const {
    for (std::size_t d = position.size(); d-- > 0;) {
      if (position[d].inclusive_min + 1 < interval[d].exclusive_max) {
        ++position[d].inclusive_min;
        return;
      }
      position[d].inclusive_min = interval[d].inclusive_min;
    }
  }

  template <typename T>
  void _current_position_stride(
      std::vector<typename Variable<T>::Interval>&
          position,  // NOLINT (non-const)
      const std::vector<typename Variable<T>::Interval>& interval,
      const std::size_t num_elements) {
    auto dims = position.size();
    if (position[dims - 1].exclusive_max <
        position[dims - 1].inclusive_min + num_elements) {
      position[dims - 1].inclusive_min =
          position[dims - 1].inclusive_min + num_elements;
      return;
    }
    for (auto i = 0; i < num_elements; ++i) {
      _current_position_increment<T>(position, interval);
    }
  }

  template <typename T>
  Result<std::tuple<VariableData<T>, const T*, Index, Index>> _resolve_future(
      Future<VariableData<T>>& fut) {  // NOLINT (non-const)
    if (!fut.status().ok()) return fut.status();
    auto data = fut.value();
    const T* data_ptr = data.get_data_accessor().data();
    Index offset = data.get_flattened_offset();
    Index n_samples = data.num_samples();
    return std::make_tuple(std::move(data), data_ptr, offset, n_samples);
  }
};

}  // namespace mdio

#endif  // MDIO_COORDINATE_SELECTOR_H_
