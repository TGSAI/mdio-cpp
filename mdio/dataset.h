// Copyright 2024 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#define MDIO_API_VERSION "1.0.0"

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mdio/dataset_factory.h"
#include "mdio/variable.h"
#include "mdio/variable_collection.h"
#include "mdio/zarr/zarr.h"
#include "tensorstore/driver/zarr/metadata.h"
#include "tensorstore/util/future.h"
#include "tensorstore/util/option.h"
#include "tensorstore/util/status.h"

// clang-format off
#include <nlohmann/json-schema.hpp>  // NOLINT
// clang-format on

namespace mdio {
namespace internal {

/**
 * @brief Retrieves the .zarray JSON metadata from the given `metadata`.
 *
 * This function derives the .zarray JSON metadata without actually reading it.
 * This is a compatibility wrapper that delegates to the zarr V2 implementation.
 *
 * @param metadata The input JSON metadata.
 * @return An `mdio::Result` containing the .zarray JSON metadata on success, or
 * an error on failure.
 * @deprecated Use zarr::v2::GetZarray directly for V2 stores.
 */
inline Result<nlohmann::json> get_zarray(const ::nlohmann::json metadata) {
  return zarr::v2::GetZarray(metadata);
}

/**
 * @brief Writes the metadata for the dataset.
 *
 * For Zarr V2, writes consolidated metadata (.zmetadata, .zgroup, .zattrs).
 * For Zarr V3, writes only root zarr.json (no consolidated metadata support).
 *
 * This overload auto-detects the Zarr version from the first variable's
 * driver specification.
 *
 * @param dataset_metadata The metadata for the dataset.
 * @param json_variables The JSON variables.
 * @return An `mdio::Future<void>` representing the asynchronous write.
 */
inline Future<void> write_zmetadata(
    const ::nlohmann::json& dataset_metadata,
    const std::vector<::nlohmann::json>& json_variables) {
  zarr::ZarrVersion version = zarr::ZarrVersion::kV2;
  if (!json_variables.empty()) {
    version = zarr::GetVersionFromSpec(json_variables[0]);
  }
  return zarr::WriteDatasetMetadata(version, dataset_metadata, json_variables);
}

/**
 * @brief Retrieves the .zmetadata for the dataset.
 * This is for executing a read on the dataset's consolidated metadata.
 * It will also attempt to infer the driver based on the prefix of the path.
 * It will default to the "file" driver if no prefix is found.
 * @param dataset_path The path to the dataset.
 */
inline Future<tensorstore::KvStore> dataset_kvs_store(
    const std::string& dataset_path) {
  // the tensorstore driver needs a bucket field
  ::nlohmann::json kvstore;

  absl::string_view output_file = dataset_path;

  if (absl::StartsWith(output_file, "gs://")) {
    absl::ConsumePrefix(&output_file, "gs://");
    kvstore["driver"] = "gcs";
  } else if (absl::StartsWith(output_file, "s3://")) {
    absl::ConsumePrefix(&output_file, "s3://");
    kvstore["driver"] = "s3";
  } else {
    kvstore["driver"] = "file";
    std::string path = std::string(output_file);
    if (!path.empty() && path.back() != '/') {
      path.push_back('/');
    }
    kvstore["path"] = path;
    return tensorstore::kvstore::Open(kvstore);
  }  // FIXME - we need azure support ...

  std::vector<std::string> file_parts = absl::StrSplit(output_file, '/');
  if (file_parts.size() < 2) {
    return absl::InvalidArgumentError(
        "gcs/s3 drivers requires [s3/gs]://[bucket]/[path_to_file]");
  }

  std::string bucket = file_parts[0];
  std::string filepath(file_parts[1]);
  for (std::size_t i = 2; i < file_parts.size(); ++i) {
    filepath += "/" + file_parts[i];
  }
  if (!filepath.empty() && filepath.back() != '/') {
    filepath.push_back('/');
  }
  // update the bucket and path ...
  kvstore["bucket"] = bucket;
  kvstore["path"] = filepath;

  return tensorstore::kvstore::Open(kvstore);
}

/**
 * @brief Retrieves the metadata for the dataset with auto-detection.
 * This version auto-detects the Zarr version by checking for V3 markers first.
 * @param dataset_path The path to the dataset.
 * @return An `mdio::Future` containing the metadata JSON on success, or an
 * error on failure.
 */
inline Future<std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>
from_zmetadata(const std::string& dataset_path) {
  auto kvs_future = mdio::internal::dataset_kvs_store(dataset_path);

  auto pair = tensorstore::PromiseFuturePair<
      std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>::Make();

  kvs_future.ExecuteWhenReady(
      [promise = std::move(pair.promise),
       dataset_path](tensorstore::ReadyFuture<tensorstore::KvStore> ready_kvs) {
        if (!ready_kvs.result().ok()) {
          promise.SetResult(
              internal::CheckMissingDriverStatus(ready_kvs.result().status()));
          return;
        }

        // Auto-detect version
        auto version_future = zarr::DetectVersion(ready_kvs.value());
        version_future.ExecuteWhenReady(
            [promise = std::move(promise), dataset_path,
             kvs = ready_kvs.value()](
                tensorstore::ReadyFuture<zarr::ZarrVersion> version_ready) {
              zarr::ZarrVersion version = zarr::ZarrVersion::kV2;
              if (version_ready.result().ok()) {
                version = version_ready.value();
              }
              auto result_future = zarr::ReadDatasetMetadata(
                  version, dataset_path,
                  tensorstore::MakeReadyFuture<tensorstore::KvStore>(kvs));

              result_future.ExecuteWhenReady(
                  [promise = std::move(promise)](
                      tensorstore::ReadyFuture<std::tuple<
                          ::nlohmann::json, std::vector<::nlohmann::json>>>
                          result) {
                    promise.SetResult(result.result());
                  });
            });
      });

  return pair.future;
}
}  // namespace internal

using coordinate_map =
    std::unordered_map<std::string, std::vector<std::string>>;

/**
 * @brief The Dataset class
 * The dataset represents a collection of variables sharing a common grid.
 */
class Dataset {
 public:
  Dataset(const nlohmann::json& metadata, const VariableCollection& variables,
          const coordinate_map& coordinates,
          const tensorstore::IndexDomain<>& domain)
      : metadata(metadata),
        variables(variables),
        coordinates(coordinates),
        domain(domain) {}

  friend std::ostream& operator<<(std::ostream& os, const Dataset& dataset) {
    // Output metadata
    os << "Metadata: " << dataset.metadata.dump(4) << "\n";

    // Output variables
    const auto keys = dataset.variables.get_iterable_accessor();
    for (const auto& key : keys) {
      os << "Variable: " << key
         << " - Dimensions: " << dataset.variables.at(key).value().dimensions()
         << "\n";
    }

    // Output coordinates
    for (const auto& [key, coord_list] : dataset.coordinates) {
      os << "Variable: " << key << " - Coordinates: ";
      for (std::size_t i = 0; i < coord_list.size(); i++) {
        os << coord_list[i];
        if (i < coord_list.size() - 1) {
          os << ", ";
        }
      }
      os << "\n";
    }

    // Output domain information
    os << "Domain: " << dataset.domain << "\n";

    return os;
  }

  /**
   * @brief Retrieves a variable from within the dataset.
   * @param variable_name The name of the variable to retrieve.
   * @return An `mdio::Result` containing the retrieved variable if successful,
   * or an error if the label is not found.
   */
  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Result<Variable<T, R, M>> get_variable(const std::string& variable_name) {
    // return a variable from within the dataset.
    return variables.get<T, R, M>(variable_name);
  }

  /**
   * @brief Gets the intervals of the the dataset.
   * @param 0 or more dimension labels for the intervals to retrieve.
   * No labels will return all intervals in the dataset.
   * @return A vector of intervals or NotFoundError if no intervals could be
   * found.
   */
  template <typename... DimensionIdentifier>
  mdio::Result<std::vector<Variable<>::Interval>> get_intervals(
      const DimensionIdentifier&... labels) const {
    std::vector<Variable<>::Interval> intervals;
    std::unordered_set<std::string_view> labels_set;
    auto idents = variables.get_iterable_accessor();
    for (auto& ident : idents) {
      MDIO_ASSIGN_OR_RETURN(auto var, variables.at(ident));
      auto intervalRes = var.get_intervals(labels...);
      if (intervalRes.status().ok()) {
        for (auto& interval : intervalRes.value()) {
          if (labels_set.count(interval.label.label()) == 0) {
            labels_set.insert(interval.label.label());
            intervals.push_back(interval);
          }
        }
      }
    }

    if (intervals.empty()) {
      return absl::NotFoundError("No intervals found for the given labels.");
    }
    return intervals;
  }

  /**
   * @brief Constructs a Dataset from a JSON schema with explicit Zarr version.
   * This method will validate the JSON schema against the MDIO Dataset schema
   * and use the specified Zarr format version.
   * @param json_schema The JSON schema to validate.
   * @param path The path to create/open the dataset.
   * @param zarr_version The Zarr format version to use (kV2 or kV3).
   * @param options Variadic options for dataset creation/opening.
   * @details \b Usage
   *
   * Create a Zarr V3 dataset given a schema and a path:
   * @code
   * auto dataset_future = mdio::Dataset::from_json(
   *   json_spec,
   *   dataset_path,
   *   mdio::zarr::ZarrVersion::kV3,
   *   mdio::constants::kCreate
   * );
   * @endcode
   *
   * @return An `mdio::Future` resolves to a Dataset if successful, or an error
   * if the schema is invalid.
   */
  template <typename... Option>
  static Future<Dataset> from_json(::nlohmann::json& json_schema /*NOLINT*/,
                                   const std::string& path,
                                   zarr::ZarrVersion zarr_version,
                                   Option&&... options) {
    // json describing the vars ...
    MDIO_ASSIGN_OR_RETURN(auto validated_schema,
                          Construct(json_schema, path, zarr_version))
    auto [dataset_metadata, json_vars] = validated_schema;

    return mdio::Dataset::Open(dataset_metadata, json_vars,
                               std::forward<Option>(options)...);
  }

  /**
   * @brief Constructs a Dataset from a JSON schema with optional Zarr version.
   * This method will validate the JSON schema against the MDIO Dataset schema
   * and optionally use the specified Zarr format version.
   * @param json_schema The JSON schema to validate.
   * @param path The path to create/open the dataset.
   * @param zarr_version Optional Zarr format version. If not specified,
   *        auto-detects from the schema or defaults to V2.
   * @param options Variadic options for dataset creation/opening.
   * @details \b Usage
   *
   * Create a dataset with optional Zarr version:
   * @code
   * // With explicit version
   * auto dataset_future = mdio::Dataset::from_json(
   *   json_spec,
   *   dataset_path,
   *   std::optional<mdio::zarr::ZarrVersion>(mdio::zarr::ZarrVersion::kV3),
   *   mdio::constants::kCreate
   * );
   *
   * // Without version (use std::nullopt for auto-detection or default to V2)
   * std::optional<mdio::zarr::ZarrVersion> version = std::nullopt;
   * auto dataset_future = mdio::Dataset::from_json(
   *   json_spec,
   *   dataset_path,
   *   version,
   *   mdio::constants::kCreate
   * );
   * @endcode
   *
   * @return An `mdio::Future` resolves to a Dataset if successful, or an error
   * if the schema is invalid.
   */
  template <typename... Option>
  static Future<Dataset> from_json(
      ::nlohmann::json& json_schema /*NOLINT*/, const std::string& path,
      std::optional<zarr::ZarrVersion> zarr_version, Option&&... options) {
    // json describing the vars ...
    MDIO_ASSIGN_OR_RETURN(auto validated_schema,
                          Construct(json_schema, path, zarr_version))
    auto [dataset_metadata, json_vars] = validated_schema;

    return mdio::Dataset::Open(dataset_metadata, json_vars,
                               std::forward<Option>(options)...);
  }

  /**
   * @brief Constructs a Dataset from a JSON schema.
   * This method will validate the JSON schema against the MDIO Dataset schema.
   * @param json_schema The JSON schema to validate.
   * @param path The path to create/open the dataset.
   * @param options Variadic options for dataset creation/opening.
   * @details \b Usage
   *
   * Create a dataset given a schema and a path, for a new dataset use options,
   * @code
   * auto dataset_future = mdio::Dataset::from_json(
   *   json_spec,
   *   dataset_path,
   *   mdio::constants::kCreate
   * );
   * @endcode
   *
   * @return An `mdio::Future` resolves to a Dataset if successful, or an error
   * if the schema is invalid.
   */
  template <typename... Option>
  static Future<Dataset> from_json(::nlohmann::json& json_schema /*NOLINT*/,
                                   const std::string& path,
                                   Option&&... options) {
    // json describing the vars ...
    MDIO_ASSIGN_OR_RETURN(auto validated_schema, Construct(json_schema, path))
    auto [dataset_metadata, json_vars] = validated_schema;

    return mdio::Dataset::Open(dataset_metadata, json_vars,
                               std::forward<Option>(options)...);
  }

  /**
   * @brief Performs an indexed slice on the Dataset
   * @param descriptors The descriptors to use for the slice.
   * @details \b Usage
   *
   * Supply a variadic list of object to slice along the dimension coordinates.
   * @code
   * mdio::RangeDescriptor<Index> desc1 = {"inline", 20, 120, 1};
   * mdio::RangeDescriptor<Index> desc2 = {"crossline", 100, 200, 1};
   *
   * MDIO_ASSIGN_OR_RETURN(
   *  auto slice, dataset.isel(desc1, desc2)
   * );
   * @endcode
   *
   * @return An `mdio::Result` containing a sliced Dataset if successful, or an
   * error if the slice is invalid.
   */
  template <typename... Descriptors>
  Result<Dataset> isel(Descriptors&... descriptors) {
    VariableCollection vars;

    // the shape of the new domain
    std::map<std::string, tensorstore::IndexDomainDimension<>> dims;
    std::vector<std::string> keys = variables.get_iterable_accessor();

    for (const auto& name : keys) {
      MDIO_ASSIGN_OR_RETURN(auto variable,
                            variables.at(name).value().slice(
                                std::forward<Descriptors>(descriptors)...))
      // add to variable
      vars.add(name, variable);

      // FIXME - check consistent dims ...
      DimensionIndex idx = 0;
      for (const auto label : variable.get_store().domain().labels()) {
        // structarrays must have a byte dimension.
        if (!label.empty()) {
          dims[label] = variable.get_store().domain()[idx];
        }
        ++idx;
      }
    }

    size_t size = dims.size();
    std::vector<std::string> labels(size);
    std::vector<Index> origin(size);
    std::vector<Index> shape(size);

    DimensionIndex idx = 0;
    for (const auto& [key, val] : dims) {
      labels[idx] = key;
      origin[idx] = val.interval().inclusive_min();
      shape[idx] = val.interval().size();
      ++idx;
    }

    MDIO_ASSIGN_OR_RETURN(auto new_domain,
                          tensorstore::IndexDomainBuilder<>(size)
                              .origin(origin)
                              .shape(shape)
                              .labels(labels)
                              .Finalize())
    return Dataset{metadata, vars, coordinates, new_domain};
  }

  /**
   * @brief Internal use only.
   */
  template <typename First, typename... Rest>
  constexpr bool are_same() {
    return (... && std::is_same_v<typename outer_type<First>::type,
                                  typename outer_type<Rest>::type>);
  }

  // Generate an index sequence
  template <size_t... I>
  struct index_sequence {};

  template <size_t N, size_t... I>
  struct make_index_sequence : make_index_sequence<N - 1, N - 1, I...> {};

  template <size_t... I>
  struct make_index_sequence<0, I...> : index_sequence<I...> {};

  // Function to call `isel` with a parameter pack expanded from the vector
  /**
   * @brief Internal use only.
   * Calls the `isel` method with a parameter pack expanded from the vector.
   */
  template <std::size_t... I>
  Result<Dataset> call_isel_with_vector_impl(
      const std::vector<RangeDescriptor<Index>>& slices,
      std::index_sequence<I...>) {
    return isel(slices[I]...);
  }

  // Wrapper function that generates the index sequence
  /**
   * @brief This version of isel is only expected to be used interally.
   * Documentation is provided for clarity and usage in the case where
   * the number of descriptors cannot be known at compile time.
   * Calls the `isel` method with a vector of `RangeDescriptor` objects.
   * Limited to `internal::kMaxNumSlices` slices which may not be equal to the
   * number of descriptors.
   */
  Result<Dataset> isel(const std::vector<RangeDescriptor<Index>>& slices) {
    if (slices.empty()) {
      return absl::InvalidArgumentError("No slices provided.");
    }

    // 1) Group descriptors by their dimension label
    std::map<std::string_view, std::vector<RangeDescriptor<Index>>> groups;
    for (auto& desc : slices) {
      groups[desc.label.label()].push_back(desc);
    }

    // 2) Walk through each dimension-group and break it into kMax-sized windows
    Dataset current = *this;
    for (auto& [label, descs] : groups) {
      for (size_t i = 0; i < descs.size(); i += internal::kMaxNumSlices) {
        size_t end = std::min(i + internal::kMaxNumSlices, descs.size());
        std::vector<RangeDescriptor<Index>> window(descs.begin() + i,
                                                   descs.begin() + end);

        // 3) Pad this window up to kMax (if your impl still needs padding)
        window.reserve(internal::kMaxNumSlices);
        for (size_t p = window.size(); p < internal::kMaxNumSlices; ++p) {
          window.emplace_back(
              RangeDescriptor<Index>{internal::kInertSliceKey, 0, 1, 1});
        }

        MDIO_ASSIGN_OR_RETURN(
            current,
            current.call_isel_with_vector_impl(
                window, std::make_index_sequence<internal::kMaxNumSlices>{}));
      }
    }

    return current;
  }

  /**
   * @brief Internal use only.
   * Converts the `sel` descriptors to their `isel` equivalents.
   */
  template <typename... Descriptors>
  Result<std::map<std::string_view, std::vector<Index>>> descriptor_to_index(
      Descriptors... descriptors) {
    absl::Status trueStatus =
        absl::OkStatus();  // A hack to allow for true error status return.
    std::map<std::string_view, std::vector<Index>> label_to_indices;
    auto processDescriptor = [this, &label_to_indices,
                              &trueStatus](auto& descriptor) -> absl::Status {
      using ValueType =
          typename extract_descriptor_Ttype<decltype(descriptor)>::type;

      auto varRes =
          variables.get<ValueType>(std::string(descriptor.label.label()));
      if (!varRes.status().ok()) {
        trueStatus = varRes.status();
        return trueStatus;
      }
      auto var = varRes.value();
      auto varFut = var.Read();
      if (!varFut.status().ok()) {
        trueStatus = varFut.status();
        return trueStatus;
      }
      auto varDat = varFut.value();
      auto varAccessor = varDat.get_data_accessor();

      std::vector<Index> indices;
      auto offset = varDat.get_flattened_offset();
      if constexpr ((std::is_same_v<
                         Descriptors,
                         ListDescriptor<typename Descriptors::type>> &&
                     ...)) {
        std::set<ValueType> values;
        for (auto val : descriptor.values) {
          if (values.count(val) > 0) {
            trueStatus = absl::InvalidArgumentError(
                "Repeated value found in ListDescriptor.");
            return trueStatus;
          }
          values.insert(val);
          bool found = false;
          for (Index i = offset; i < var.num_samples() + offset; ++i) {
            if (varAccessor({i}) == val) {
              if (found) {
                trueStatus = absl::InvalidArgumentError(
                    "Repeated value found in ListDescriptor.");
                return trueStatus;
              }
              label_to_indices[descriptor.label.label()].push_back(i);
              found = true;
            }
          }
          if (!found) {
            trueStatus = absl::InvalidArgumentError(
                "Value not found in ListDescriptor.");
            return trueStatus;
          }
        }
      } else {
        // We must check for every occurance of the value
        for (Index i = offset; i < var.num_samples() + offset; ++i) {
          if (varAccessor({i}) == descriptor.value) {
            label_to_indices[descriptor.label.label()].push_back(i);
          }
        }
      }

      // Add the indices to the map
      return absl::OkStatus();
    };

    auto status = (processDescriptor(descriptors).ok() && ...);
    if (!status) {
      return trueStatus;
    }

    return label_to_indices;
  }

  /**
   * @brief Performs a label-based slice on the Dataset
   * This method will slice the Dataset based on coordinates rather than
   * indicies. It may generate multiple slices depending on the type of
   * descriptors provided.
   * @param descriptors The descriptors to use for the slice. May be
   * `RangeDescriptor`, `ValueDescriptor`, or `ListDescriptor`.
   */
  template <typename... Descriptors>
  Result<Dataset> sel(Descriptors... descriptors) {
    /*
    Case 1: ValueDescriptor with repeated values: Get all occurrences of the
    value Case 2: ListDescriptor with repeated values (single element): Return
    Invalid Reindexing error Case 3: ListDescriptor with repeated values
    (multiple elements): Return Invalid Reindexing error (Case 2) Case 4: Same
    as Case 1 Case 5: RangeDescriptor with repeated values: Return Invalid
    Reindexing error Case 6: RangeDescriptor with unique values: Get the range
    from start to stop, include everything in-between

    Case fail: label is not 1D
    Case fail: label is repeated. This is a dictionary in xarray, so not
    allowed.
    */

    /*
    Valid cases:
      Case 1: ValueDescriptor with unique value: Get the single value.
        No error state.
        Get the index and convert to conventional isel.
      Case 2: ValueDescriptor with non-unique value: Get all occurrences of the
    value. No error state. Case 3: ListDescriptor with unique values: Get the
    individual values. Error state for repeated values. Case 4: RangeDescriptor
    with unique start and stop values: Get the range from start to stop, include
    everything in-between. Error state for repeated start/stop values. Any
    repeated values that are not start/stop are fair game. Get the start and
    stop indicies and create a single isel.
    */

    // Check that all descriptors are of the same outer type
    if (!are_same<Descriptors...>()) {
      return absl::InvalidArgumentError(
          "All descriptors must be of the same type.");
    }

    // Validate each descriptor (for example, ListDescriptor not yet supported)
    auto validateDescriptors = [this](auto& descriptor) {
      using DescriptorType = typename outer_type<decltype(descriptor)>::type;
      if constexpr (std::is_same_v<
                        std::remove_reference_t<DescriptorType>,
                        ListDescriptor<typename std::remove_reference_t<
                            decltype(descriptor)>::type>>) {
        return absl::UnimplementedError(
            "Support for ListDescriptor is not yet implemented.");
      }
      // TODO(BrianMichell): Remove this check when SliceDescriptor is removed
      if constexpr (std::is_same_v<DescriptorType, SliceDescriptor>) {
        return absl::InvalidArgumentError(
            "SliceDescriptor is deprecated and will be removed in future "
            "versions. Please use RangeDescriptor instead.\nThe sel method "
            "does not support SliceDescriptor.");
      }

      MDIO_ASSIGN_OR_RETURN(
          auto var, variables.at(std::string(descriptor.label.label())));
      if (var.dimensions().rank() != 1) {
        return absl::InvalidArgumentError("Label must be 1D.");
      }

      return absl::OkStatus();
    };

    std::set<std::string_view> labels;

    // Call validateDescriptors for each descriptor
    {
      // Manage the scope of status
      absl::Status status;
      for (auto& descriptor : {descriptors...}) {
        status = validateDescriptors(descriptor);
        if (!status.ok()) {
          return status;
        }
        if (labels.count(descriptor.label.label()) > 0) {
          return absl::InvalidArgumentError("Label must not be repeated.");
        }
        if (descriptor.label.index() !=
            std::numeric_limits<DimensionIndex>::max()) {
          return absl::InvalidArgumentError(
              "Expected label to be a dimension name but got an index.");
        }
        labels.insert(descriptor.label.label());
      }
    }

    // Check if the descriptors are of type ValueDescriptor
    if constexpr ((std::is_same_v<
                       Descriptors,
                       ValueDescriptor<typename Descriptors::type>> &&
                   ...)) {
      auto slicer = descriptor_to_index(descriptors...);
      if (!slicer.status().ok()) {
        return slicer.status();
      }

      auto label_to_indices = slicer.value();

      // Build out the slice descriptors
      std::vector<RangeDescriptor<Index>> slices;
      for (auto& elem : label_to_indices) {
        auto size = elem.second.size();
        for (int i = 0; i < size; ++i) {
          slices.emplace_back(RangeDescriptor<Index>(
              {elem.first, elem.second[i], elem.second[i] + 1, 1}));
        }
      }

      if (slices.empty()) {
        return absl::InvalidArgumentError(
            "No slices could be made from the given descriptors.");
      }
      // The map 'label_to_indices' is now populated with all the relevant
      // indices. You can now proceed with further processing based on this map.

      return isel(
          static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
    } else if constexpr ((std::is_same_v</*NOLINT: readability/braces*/
                                         Descriptors,
                                         ListDescriptor<
                                             typename Descriptors::type>> &&
                          ...)) {
      auto slicer = descriptor_to_index(descriptors...);
      if (!slicer.status().ok()) {
        return slicer.status();
      }

      auto label_to_indices = slicer.value();

      // Build out the slice descriptors
      std::vector<RangeDescriptor<Index>> slices;
      for (auto& elem : label_to_indices) {
        auto size = elem.second.size();
        for (int i = 0; i < size; ++i) {
          slices.emplace_back(RangeDescriptor<Index>(
              {elem.first, elem.second[i], elem.second[i] + 1, 1}));
        }
      }

      if (slices.empty()) {
        return absl::InvalidArgumentError(
            "No slices could be made from the given descriptors.");
      }
      // The map 'label_to_indices' is now populated with all the relevant
      // indices. You can now proceed with further processing based on this map.

      return isel(
          static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
    } else {
      std::map<std::string_view, std::pair<Index, Index>>
          label_to_range;  // pair.first = start, pair.second = stop
      absl::Status trueStatus =
          absl::OkStatus();  // A hack to allow for true error status return.

      auto processDescriptor = [this, &label_to_range,
                                &trueStatus](auto& descriptor) -> absl::Status {
        using ValueType =
            typename extract_descriptor_Ttype<decltype(descriptor)>::type;

        if (descriptor.start == descriptor.stop) {
          trueStatus = absl::InvalidArgumentError(
              "Start and stop values must be different.");
          return trueStatus;
        }

        auto varRes =
            variables.get<ValueType>(std::string(descriptor.label.label()));
        if (!varRes.status().ok()) {
          trueStatus = varRes.status();
          return trueStatus;
        }
        auto var = varRes.value();
        auto varFut = var.Read();
        if (!varFut.status().ok()) {
          trueStatus = varFut.status();
          return trueStatus;
        }
        auto varDat = varFut.value();
        auto varAccessor = varDat.get_data_accessor();

        std::pair<bool, Index> start = {false, 0};
        std::pair<bool, Index> stop = {false, 0};
        auto offset = varDat.get_flattened_offset();

        for (Index i = offset; i < var.num_samples() + offset; i++) {
          if (varAccessor({i}) == descriptor.start) {
            if (start.first) {
              trueStatus = absl::InvalidArgumentError("Repeated start value.");
              return trueStatus;
            }
            start = {true, i};
          }
          if (varAccessor({i}) == descriptor.stop) {
            if (stop.first) {
              trueStatus = absl::InvalidArgumentError("Repeated stop value.");
              return trueStatus;
            }
            stop = {true, i};
          }
        }

        if (!start.first) {
          trueStatus = absl::InvalidArgumentError("Start value not found.");
          return trueStatus;
        }
        if (!stop.first) {
          trueStatus = absl::InvalidArgumentError("Stop value not found.");
          return trueStatus;
        }

        // Xarray behavior is to effectively remove the Variable in this case.
        if (start.second >= stop.second) {
          trueStatus = absl::UnimplementedError(
              "Start value happens after stop value. This is not a supported "
              "case.");
          return trueStatus;
        }
        // This case should be caught by the earlier check, but it's here for
        // completeness.
        if (label_to_range.count(descriptor.label.label()) > 0) {
          trueStatus =
              absl::InvalidArgumentError("Label must not be repeated.");
          return trueStatus;
        }
        label_to_range[descriptor.label.label()] = {start.second, stop.second};
        return absl::OkStatus();
      };

      auto status = (processDescriptor(descriptors).ok() && ...);
      if (!status) {
        return trueStatus;
      }

      std::vector<RangeDescriptor<Index>> slices;
      for (auto& elem : label_to_range) {
        slices.emplace_back(RangeDescriptor<Index>(
            {elem.first, elem.second.first, elem.second.second + 1, 1}));
      }

      if (slices.empty()) {
        return absl::InvalidArgumentError(
            "No slices could be made from the given descriptors.");
      }

      return isel(
          static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
    }

    return absl::OkStatus();
  }

  /**
   * @brief Performs a label-based slice on the Dataset
   * @param descriptors The descriptors to use for the slice.
   * @return An `mdio::Result` containing a sliced Dataset if successful, or an
   * error if the slice is invalid.
   */
  Result<Dataset> operator[](const std::string& label) {
    // extract the variable (+ coordinates)
    VariableCollection vars;

    MDIO_ASSIGN_OR_RETURN(auto var, variables.get(label))
    vars.add(label, var);

    auto domain = var.dimensions();

    // collect and dimension variables.
    for (const auto& dim_label : domain.labels()) {
      if (!vars.contains_key(dim_label)) {
        MDIO_ASSIGN_OR_RETURN(auto var, variables.get(dim_label))
        vars.add(dim_label, var);
      }
    }

    // coordinates associated with the variable
    coordinate_map coords;
    if (coordinates.count(label) > 0) {
      for (const auto& coord_name : coordinates.at(label)) {
        MDIO_ASSIGN_OR_RETURN(auto coord, variables.get(coord_name))
        vars.add(coord_name, coord);
      }

      coords = {{label, coordinates.at(label)}};
    }

    return Dataset{metadata, vars, coords, domain};
  }

  /**
   * @brief Opens a Dataset from a file path.
   * This method will assume that the Dataset already exists at the specified
   * path.
   * @param dataset_path The path to the dataset.
   * @details \b Usage
   * @code
   * auto existing_dataset = mdio::Dataset::Open(
   *      dataset_path, mdio::constants::kOpen
   * );
   * @endcode
   * @return An `mdio::Future` containing a Dataset if successful, or an error
   * if the path is invalid.
   */
  template <typename S = std::string, typename... Option>
  static std::enable_if_t<(std::is_same_v<S, std::string>), Future<Dataset>>
  Open(const S& dataset_path, Option&&... options) {
    TransactionalOpenOptions transact_options;
    TENSORSTORE_RETURN_IF_ERROR(
        tensorstore::internal::SetAll(transact_options, options...));

    if (transact_options.open_mode != constants::kOpen) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "Open from path is only valid in open-mode.");
    }

    MDIO_ASSIGN_OR_RETURN(auto params_from_zmetadata,
                          mdio::internal::from_zmetadata(dataset_path).result());
    auto [dataset_metadata, json_vars] = params_from_zmetadata;

    return mdio::Dataset::Open(dataset_metadata, json_vars,
                               std::forward<Option>(options)...);
  }

  /**
   * @brief Opens the Dataset from a constructed JSON schema.
   * This method should be used in conjunction with the dataset_factory
   * Construct function. It expects a fully valid metadata and vector of MDIO
   * compliant Variable specs.
   * @param metadata The metadata for the dataset.
   * @param json_variables A vector of correctly constructed Variable specs.
   * @return An `mdio::Result` containing a Dataset if successful, or an error
   * if the schema is invalid.
   */
  template <typename... Option>
  static Future<Dataset> Open(
      const ::nlohmann::json& metadata,
      const std::vector<::nlohmann::json>& json_variables,
      Option&&... options) {
    // I need to know if we are intending to create a dataset from scratch?
    TransactionalOpenOptions transact_options;
    TENSORSTORE_RETURN_IF_ERROR(
        tensorstore::internal::SetAll(transact_options, options...));
    bool do_create = transact_options.open_mode == constants::kCreateClean ||
                     transact_options.open_mode == constants::kCreate;

    // FIXME - publish dataset
    std::vector<Future<mdio::Variable<>>> variables;
    std::vector<tensorstore::Promise<void>> promises;
    std::vector<tensorstore::AnyFuture> futures;
    for (const auto& json : json_variables) {
      auto pair = tensorstore::PromiseFuturePair<void>::Make();

      auto var = mdio::Variable<>::Open(json, std::forward<Option>(options)...);

      // Attach a continuation to the first future
      var.ExecuteWhenReady(
          [promise = std::move(pair.promise)](
              tensorstore::ReadyFuture<mdio::Variable<>> readyVar) {
            // When firstFuture is ready, fulfill the second promise
            promise.SetResult(
                absl::OkStatus());  // Set appropriate result or status
          });

      variables.push_back(std::move(var));
      promises.push_back(std::move(pair.promise));
      futures.push_back(std::move(pair.future));
    }

    // here we have to publish the zmetadata ...
    if (do_create) {
      futures.push_back(
          mdio::internal::write_zmetadata(metadata, json_variables));
    }

    // ready when everything's available ...
    auto all_done_future = tensorstore::WaitAllFuture(futures);

    auto pair = tensorstore::PromiseFuturePair<Dataset>::Make();
    all_done_future.ExecuteWhenReady(
        [promise = std::move(pair.promise), variables = std::move(variables),
         metadata](tensorstore::ReadyFuture<void> readyFut) {
          if (metadata.contains("api_version") &&
              !metadata.contains("apiVersion")) {
            promise.SetResult(
                absl::Status(absl::StatusCode::kInvalidArgument,
                             "Detected MDIO v0 dataset model " +
                                 metadata["api_version"].get<std::string>() +
                                 " but expected v1"));
            return;
          }
          mdio::VariableCollection collection;
          mdio::coordinate_map coords;
          std::unordered_map<std::string, Index> shape_size;

          for (const auto& fvar : variables) {
            // we should have waited for this to be ready so it's not blocking
            // ...
            auto _var = fvar.result();
            if (!_var.ok()) {
              promise.SetResult(_var.status());
              continue;
            }
            auto var = _var.value();

            collection.add(var.get_variable_name(), std::move(var));
            // update coordinates if any:
            auto meta = var.getMetadata();
            if (meta.contains("coordinates")) {
              // Because of how Variable is set up, we need to break down a
              // space delimited string to the vector
              std::string coords_str = meta["coordinates"].get<std::string>();
              std::vector<std::string> coords_vec =
                  absl::StrSplit(coords_str, ' ');
              coords[var.get_variable_name()] = coords_vec;
            }

            auto domain = var.dimensions();
            auto shape = domain.shape().cbegin();
            for (const auto& label : domain.labels()) {
              // FIXME check that if exists shape is the same ...
              shape_size[label] = *shape;
              ++shape;
            }
          }

          std::vector<std::string> keys;
          std::vector<Index> values;

          keys.reserve(shape_size.size());
          values.reserve(shape_size.size());
          for (const auto& pair : shape_size) {
            keys.push_back(std::move(pair.first));
            values.push_back(pair.second);
          }

          auto dataset_domain =
              tensorstore::IndexDomainBuilder<>(shape_size.size())
                  .shape(values)
                  .labels(keys)
                  .Finalize();

          if (!dataset_domain.ok()) {
            promise.SetResult(dataset_domain.status());
            return;
          }

          Dataset new_dataset{metadata, collection, coords,
                              dataset_domain.value()};
          promise.SetResult(std::move(new_dataset));
        });
    return pair.future;
  }

  /**
   * @brief Selects a field from a Variable with a structured data type.
   * This will need to re-open the Variable.
   * The new Variable will replace the original one in the Dataset once the
   * future has been resolved. Attempting to access the Variable before the
   * future has been resolved may result in a race condition.
   * @param variableName The name of the variable to select the field from.
   * @param fieldName The name of the field to select.
   * @return An `mdio::Future` if the selection was valid and successful, or an
   * error if the selection was invalid.
   */
  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Future<Variable<T, R, M>> SelectField(const std::string& variableName,
                                        const std::string& fieldName) {
    // Ensure that the variable exists in the Dataset
    if (!variables.contains_key(variableName)) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "Variable '" + variableName + "' not found in the dataset.");
    }

    // Grab the Variable from the Dataset
    MDIO_ASSIGN_OR_RETURN(auto var, variables.at(variableName));
    // Preserve the intervals so it can be re-sliced to the same dimensions
    MDIO_ASSIGN_OR_RETURN(auto intervals, var.get_intervals());
    intervals.pop_back();  // Remove the byte dimension (Doesn't matter if it
                           // doesn't exist)

    // Ensure that the Variable is of dtype structarray
    auto spec = var.spec();
    if (!spec.status().ok()) {
      // Something went wrong with Tensorstore retrieving the spec
      return spec.status();
    }
    auto specJsonResult = spec.value().ToJson(IncludeDefaults{});
    if (!specJsonResult.status().ok()) {
      return specJsonResult.status();
    }
    nlohmann::json specJson = specJsonResult.value();
    if (!specJson["metadata"]["dtype"].is_array()) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "Variable '" + variableName + "' is not a structured dtype.");
    }

    // Ensure the field exists in the Variable
    int found = -1;
    if (fieldName == "") {
      found = -2;
    } else {
      for (std::size_t i = 0; i < specJson["metadata"]["dtype"].size(); i++) {
        if (specJson["metadata"]["dtype"][i][0] == fieldName) {
          found = i;
          break;
        }
      }
    }
    if (found == -1) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "Field: '" + fieldName + "' not found in Variable '" +
                              variableName + "'.");
    }

    // Detect Zarr version from the spec
    std::string driverName = specJson.contains("driver")
                                 ? specJson["driver"].get<std::string>()
                                 : "zarr";

    // Create a new Variable with the selected field
    std::string baseStr = R"(
            {
                "driver": "DRIVER_PLACEHOLDER",
                "field": "FIELD",
                "kvstore": {
                    "driver": "DRIVER",
                    "path": "PATH"
                }
            }
        )";
    nlohmann::json base = nlohmann::json::parse(baseStr, nullptr, false);
    base["driver"] = driverName;
    if (base.is_discarded()) {
      return absl::Status(absl::StatusCode::kInternal,
                          "Failed to parse base JSON.");
    }
    if (found >= 0) {
      base["field"] = specJson["metadata"]["dtype"][found][0];
    } else {
      base.erase("field");
    }
    base["kvstore"]["driver"] = specJson["kvstore"]["driver"];
    base["kvstore"]["path"] = specJson["kvstore"]["path"];
    // Remove trailing slashes. This causes issue #130
    while (base["kvstore"]["path"].get<std::string>().back() == '/') {
      base["kvstore"]["path"] =
          base["kvstore"]["path"].get<std::string>().substr(
              0, base["kvstore"]["path"].get<std::string>().size() - 1);
    }

    // Handle cloud stores
    if (specJson["kvstore"].contains("bucket")) {
      base["kvstore"]["bucket"] = specJson["kvstore"]["bucket"];
      std::string cloudPath = base["kvstore"]["path"].get<std::string>();
      // cloudPath
      //     .pop_back();  // We need to remove the trailing / from a cloud path
      base["kvstore"]["path"] = cloudPath;
    }

    auto fieldedVar = mdio::Variable<T, R, M>::Open(base, constants::kOpen);

    auto pair = tensorstore::PromiseFuturePair<mdio::Variable<T, R, M>>::Make();
    fieldedVar.ExecuteWhenReady(
        [this, promise = pair.promise, variableName, intervals](
            tensorstore::ReadyFuture<mdio::Variable<T, R, M>> readyFut) {
          auto ready_result = readyFut.result();
          if (!ready_result.ok()) {
            promise.SetResult(ready_result.status());
          } else {
            // Re-slice the Variable to the same dimensions as the original
            std::vector<mdio::RangeDescriptor<Index>> slices;
            slices.reserve(intervals.size() - 1);
            for (const auto& interval : intervals) {
              slices.emplace_back(mdio::RangeDescriptor<Index>(
                  {interval.label, interval.inclusive_min,
                   interval.exclusive_max, 1}));
            }
            auto slicedVarRes = ready_result.value().slice(slices);
            if (!slicedVarRes.status().ok()) {
              promise.SetResult(slicedVarRes.status());
            } else {
              this->variables.add(variableName, ready_result.value());
              promise.SetResult(slicedVarRes);
            }
          }
        });
    return pair.future;
  }

  /**
   * @brief Commits changes made to the Variables metadata to durable media.
   * @return A future representing the completion of the commit or an error if
   * no changes were made but the commit was requested.
   */
  tensorstore::Future<void> CommitMetadata() {
    auto keys = variables.get_iterable_accessor();

    // Build out list of modified variables
    std::vector<std::string> modifiedVariables;
    for (const auto& key : keys) {
      auto var = variables.at(key).value();
      if (var.was_updated() || var.should_publish()) {
        modifiedVariables.push_back(key);
      }
      if (var.should_publish()) {
        // Reset the flag. This should only get set by the trim util.
        var.set_metadata_publish_flag(false);
      }
    }

    // If nothing changed, we don't want to perform any writes
    if (modifiedVariables.empty()) {
      tensorstore::Future<void> err =
          tensorstore::MakeResult<tensorstore::Future<void>>(
              absl::InvalidArgumentError("No variables were modified."));
      return err;
    }

    // We need to update the entire .zmetadata file
    std::vector<nlohmann::json> json_vars;
    std::vector<std::shared_ptr<Variable<>>>
        vars;  // Keeps the Variables in memory using shared_ptr
    for (const auto& key : keys) {
      auto var = std::make_shared<Variable<>>(variables.at(key).value());
      vars.push_back(var);
      // Get the JSON, drop transform, and add attributes
      nlohmann::json json =
          var->get_store().spec().value().ToJson(IncludeDefaults{}).value();
      json.erase("transform");
      json.erase("dtype");
      json["metadata"].erase("filters");
      json["metadata"].erase("order");
      json["metadata"].erase("zarr_format");
      // On local file systems there is a trailing slash that needs to be
      // removed.
      std::string path = json["kvstore"]["path"].get<std::string>();
      path.pop_back();
      json["kvstore"]["path"] = path;
      nlohmann::json meta = var->getMetadata();
      if (meta.contains("coordinates")) {
        meta["attributes"]["coordinates"] = meta["coordinates"];
        meta.erase("coordinates");
      }
      if (meta.contains("dimension_names")) {
        meta["attributes"]["dimension_names"] = meta["dimension_names"];
        meta.erase("dimension_names");
      }
      if (meta.contains("long_name")) {
        meta["attributes"]["long_name"] = meta["long_name"];
        meta.erase("long_name");
      }
      if (meta.contains("metadata")) {
        if (meta["metadata"].contains("chunkGrid")) {
          meta["metadata"].erase("chunkGrid");  // We never serialize this
        }
        if (!meta.contains("attributes")) {
          meta["attributes"] = nlohmann::json::object();
        }
        meta["attributes"]["metadata"].merge_patch(meta["metadata"]);
        meta.erase("metadata");
      }
      json.update(meta);
      json_vars.emplace_back(json);
    }

    // Now let's get the .zmetadata going.
    auto zmetadata_future =
        mdio::internal::write_zmetadata(metadata, json_vars);
    // Finally we can loop through the updated Variables and update them.

    std::vector<tensorstore::Future<tensorstore::TimestampedStorageGeneration>>
        variableFutures;
    std::vector<tensorstore::Promise<tensorstore::TimestampedStorageGeneration>>
        promises;
    std::vector<tensorstore::AnyFuture> futures;

    for (const auto& key : modifiedVariables) {
      auto pair = tensorstore::PromiseFuturePair<
          tensorstore::TimestampedStorageGeneration>::Make();
      auto var = std::make_shared<Variable<>>(variables.at(key).value());
      auto updateFuture = var->PublishMetadata();
      updateFuture.ExecuteWhenReady(
          [promise = std::move(pair.promise),
           var](tensorstore::ReadyFuture<
                tensorstore::TimestampedStorageGeneration>
                    readyFut) { promise.SetResult(readyFut.result()); });
      variableFutures.push_back(std::move(updateFuture));
      futures.push_back(std::move(pair.future));
    }

    futures.push_back(zmetadata_future);

    auto all_done_future = tensorstore::WaitAllFuture(futures);

    auto pair = tensorstore::PromiseFuturePair<void>::Make();
    all_done_future.ExecuteWhenReady(
        [promise = std::move(pair.promise),
         updates = std::move(variableFutures)](
            tensorstore::ReadyFuture<void> readyFut) {
          for (const auto& update : updates) {
            auto _update = update.result();
            if (!_update.ok()) {
              promise.SetResult(_update.status());
              return;
            }
          }
          promise.SetResult(absl::OkStatus());
          return;
        });
    return pair.future;
  }

  /**
   * @brief Gets the Dataset level metadata.
   * @return A const reference to the Dataset's metadata.
   */
  const nlohmann::json& getMetadata() const { return metadata; }

  /// variables contained in the dataset
  VariableCollection variables;

  /// link a variable name to its coordinates via its name(s)
  coordinate_map coordinates;

  /// enumerate the dimensions
  tensorstore::IndexDomain<> domain;

 private:
  /// the metadata associated with the dataset (root .zattrs)
  ::nlohmann::json metadata;
};
}  // namespace mdio
