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


  template <typename T>
  mdio::Future<void> add_selection(const ValueDescriptor<T>& descriptor) {
    using Interval = typename Variable<T>::Interval;

    MDIO_ASSIGN_OR_RETURN(auto var, dataset_.variables.get<T>(std::string(descriptor.label.label())));
    auto fut = var.Read();
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());
    if (!fut.status().ok()) return fut.status();

    auto data = fut.value();
    const T* data_ptr = data.get_data_accessor().data();
    Index offset = data.get_flattened_offset();
    Index n_samples = data.num_samples();

    auto current_pos = intervals;
    bool isInRun = false;
    std::vector<std::vector<Interval>> local_runs;

    for (mdio::Index idx = offset; idx < offset + n_samples; ++idx) {
      if (data_ptr[idx] == descriptor.value) {
        if (!isInRun) {
          isInRun = true;
          std::vector<Interval> run = current_pos;
          for (auto& pos : run) {
            pos.exclusive_max = pos.inclusive_min + 1;
          }
          local_runs.push_back(std::move(run));
        } else {
          auto& run = local_runs.back();
          for (auto i=0; i<current_pos.size(); ++i) {
            run[i].exclusive_max = current_pos[i].inclusive_min + 1;
          }
        } 
      } else {
        isInRun = false;
      }
      _current_position_increment<T>(current_pos, intervals);
    }

    if (local_runs.empty()) {
      std::stringstream ss;
      ss << "No matches for coordinate '" << descriptor.label.label() << "'";
      return absl::NotFoundError(ss.str());
    }

    auto new_runs = _from_intervals<T>(local_runs);

    // First time calling add_selection_2
    if (kept_runs_.empty()) {
      kept_runs_ = std::move(new_runs);
    } else {
      // now intersect each kept_run with each new local run
      std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> new_kept;
      new_kept.reserve(kept_runs_.size() * local_runs.size());

      for (const auto& kept : kept_runs_) {
        for (const auto& run : new_runs) {
          // start from the old run
          auto intersection = kept;
          bool empty = false;

          // for each descriptor in the new run...
          for (const auto& d_new : run) {
            // try to find the same label in the kept run
            auto it = std::find_if(
              intersection.begin(), intersection.end(),
              [&](auto const& d_old) {
                return d_old.label.label() == d_new.label.label();
              });

            if (it != intersection.end()) {
              // intersect intervals
              auto& d_old = *it;
              auto new_min = std::max(d_old.start, d_new.start);
              auto new_max = std::min(d_old.stop, d_new.stop);
              if (new_min >= new_max) {
                empty = true;
                break;
              }
              d_old.start = new_min;
              d_old.stop = new_max;
            } else {
              // brand-new dimension: append it
              intersection.push_back(d_new);
            }
          }

          if (!empty) {
            new_kept.push_back(std::move(intersection));
          }
        }
      }

      kept_runs_ = std::move(new_kept);
    }

    return absl::OkStatus();
  }

  /// \brief Emit a RangeDescriptor per surviving tuple coordinate, without coalescing.
  std::vector<RangeDescriptor<Index>> range_descriptors() const {
    std::vector<mdio::RangeDescriptor<mdio::Index>> ret;
    ret.reserve(kept_runs_.size() * kept_runs_[0].size());
    for (auto const& run : kept_runs_) {
      for (auto const& interval : run) {  // TODO: This is not an interval!
        ret.emplace_back(RangeDescriptor<mdio::Index>{interval.label, interval.start, interval.stop, 1});
      }
    }
    return ret;
  }

  template <typename InputType, typename OutputType>
  Future<std::vector<OutputType>> run_view(const std::string& sort_key, const std::string& output_variable) {
    auto non_const_ds = dataset_;
    // const auto& descs = range_descriptors();
    const auto& descs = kept_runs_;

    std::vector<OutputType> ret;

    std::size_t descs_idx = 0;
    std::vector<std::pair<std::size_t, Future<VariableData<InputType>>>> run_list_fut;
    std::vector<std::pair<std::size_t, InputType>> run_list;

    bool sort_key_is_dimension = false;
    std::size_t total_volume = 0;

    run_list.reserve(descs.size());
    std::cout << "Grabbing " << descs.size() << " coordinates from " << sort_key << std::endl;
    for (const auto& desc : descs) {
      MDIO_ASSIGN_OR_RETURN(auto segmentedDs, non_const_ds.isel(desc));
      MDIO_ASSIGN_OR_RETURN(auto var, segmentedDs.variables.get<InputType>(sort_key));
      auto fut = var.Read();
      run_list_fut.emplace_back(descs_idx, fut);
      descs_idx++;

      total_volume += var.num_samples();

      if (var.rank() == 1) {
        sort_key_is_dimension = true;
        break;
      }
    }

    ret.reserve(total_volume);
    run_list.reserve(run_list_fut.size());

    // std::cout << "Awaiting futures..." << std::endl;
    for (auto& fut : run_list_fut) {
      if (!fut.second.status().ok()) return fut.second.status();
      auto data = fut.second.value();
      auto val = data.get_data_accessor().data()[data.get_flattened_offset()];
      run_list.emplace_back(fut.first, val);
    }
    run_list_fut.clear(); // Clear to save memory

    // std::cout << "Sorting..." << std::endl;
    std::stable_sort(run_list.begin(), run_list.end(), [](auto const& a, auto const& b) {
      return a.second < b.second;
    });

    // std::cout << "Building longest-run based vector..." << std::endl;
    // Now we build the Run<T> object.
    for (const auto& run : run_list) {
      auto desc = descs[run.first];
      MDIO_ASSIGN_OR_RETURN(auto segmentedDs, non_const_ds.isel(desc));
      MDIO_ASSIGN_OR_RETURN(auto var, segmentedDs.variables.template get<OutputType>(output_variable));
      auto fut = var.Read();
      if (!fut.status().ok()) return fut.status();
      auto data = fut.value();
      OutputType* data_ptr = data.get_data_accessor().data();
      Index offset = data.get_flattened_offset();
      Index n = data.num_samples();
      std::vector<OutputType> buffer(n);
      std::memcpy(buffer.data(), data_ptr + offset, n * sizeof(OutputType));
      ret.insert(ret.end(), buffer.begin(), buffer.end());

      if (sort_key_is_dimension) {
        // std::cout << "Returning early due to sort key being dimension" << std::endl;
        return ret;
      }

    }

    // std::cout << "Done!" << std::endl;
    return ret;
  }

private:
  const Dataset& dataset_;
  tensorstore::IndexDomain<> base_domain_;
  std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> kept_runs_;

  template <typename T>
  std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> _from_intervals(std::vector<std::vector<typename mdio::Variable<T>::Interval>>& intervals) {
    std::vector<std::vector<mdio::RangeDescriptor<mdio::Index>>> ret;
    ret.reserve(intervals.size());
    for (auto const& run : intervals) {
      std::vector<mdio::RangeDescriptor<mdio::Index>> run_descs;
      run_descs.reserve(run.size());
      for (auto const& interval : run) {
        run_descs.emplace_back(mdio::RangeDescriptor<mdio::Index>{interval.label, interval.inclusive_min, interval.exclusive_max, 1});
      }
      ret.push_back(std::move(run_descs));
    }
    return ret;
  }

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
