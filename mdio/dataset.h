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
#include "tensorstore/driver/zarr/metadata.h"
#include "tensorstore/util/future.h"

#include "tensorstore/index_space/index_transform.h"      // Transpose, MergeDims, DropDims
#include "tensorstore/index_space/index_domain_builder.h" // IndexDomainBuilder<1>
#include "tensorstore/index_space/index_domain.h"         // IndexDomainView, .origin(), .shape()
#include "tensorstore/index_space/dim_expression.h"       // DimRange, Dims

// #include "tensorstore/transformations/transpose.h"
// #include "tensorstore/transformations/merge_dims.h"
// #include "tensorstore/transformations/drop_dims.h"

// clang-format off
#include <nlohmann/json-schema.hpp>  // NOLINT
// clang-format on

namespace mdio {
namespace internal {

/**
 * @brief Retrieves the .zarray JSON metadata from the given `metadata`.
 *
 * This function derives the .zarray JSON metadata without actually reading it.
 *
 * @param metadata The input JSON metadata.
 * @return An `mdio::Result` containing the .zarray JSON metadata on success, or
 * an error on failure.
 */
Result<nlohmann::json> get_zarray(const ::nlohmann::json metadata) {
  // derive .zarray json metadata (without reading it).
  auto json =
      metadata;  // Why am I doing this? It's an extra copy that does nothing!
  nlohmann::json zarray;
  if (!json.contains("metadata")) {
    json["metadata"] = nlohmann::json::object();  // Just add an empty object
    json["metadata"]["attributes"] =
        nlohmann::json::object();  // We need attributes as well
  }

  // these fields can have defaults:
  if (!json["metadata"].contains("order")) {
    zarray["order"] = "C";
  } else {
    zarray["order"] = json["metadata"]["order"];
  }
  if (!json["metadata"].contains("filters")) {
    zarray["filters"] = nullptr;
  } else {
    zarray["filters"] = json["metadata"]["filters"];
  }

  if (!json["metadata"].contains("fill_value")) {
    zarray["fill_value"] = nullptr;
  } else {
    zarray["fill_value"] = json["metadata"]["fill_value"];
  }

  if (!json["metadata"].contains("zarr_format")) {
    zarray["zarr_format"] = 2;
  } else {
    zarray["zarr_format"] = json["metadata"]["zarr_format"];
  }

  if (!json["metadata"].contains("chunks") &&
      json["metadata"].contains("shape")) {
    zarray["chunks"] = json["metadata"]["shape"];
  } else {
    zarray["chunks"] = json["metadata"]["chunks"];
  }

  if (!json["metadata"].contains("compressor")) {
    zarray["compressor"] = nullptr;
  } else {
    zarray["compressor"] = json["metadata"]["compressor"];
  }

  if (!json["metadata"].contains("dimension_separator")) {
    zarray["dimension_separator"] = "/";
  } else {
    zarray["dimension_separator"] = json["metadata"]["dimension_separator"];
  }

  zarray["shape"] = json["metadata"]["shape"];
  zarray["dtype"] = json["metadata"]["dtype"];

  // fixme chunks must be configured ...
  MDIO_ASSIGN_OR_RETURN(
      auto zarr_metadata,
      tensorstore::internal_zarr::ZarrMetadata::FromJson(zarray))

  return ::nlohmann::json(zarr_metadata);
}

/**
 * @brief Writes the zmetadata for the dataset.
 *
 * @param dataset_metadata The metadata for the dataset.
 * @param json_variables The JSON variables.
 * @return An `mdio::Future<void>` representing the asynchronous write.
 */
Future<void> write_zmetadata(
    const ::nlohmann::json& dataset_metadata,
    const std::vector<::nlohmann::json>& json_variables) {
  // header material at the root of the dataset ...
  // Configure a kvstore (we can't deduce if it's in memory etc).
  // {
  //    "kvstore",
  //    {
  //        {"driver", "file"},
  //        {"path", "name"}
  //    }
  //}
  auto zattrs = dataset_metadata;

  // FIXME - generalize for zarr v3
  ::nlohmann::json zgroup;
  zgroup["zarr_format"] = 2;

  // The consolidated metadata for the datset
  ::nlohmann::json zmetadata;

  // FIXME - don't hard code here ...
  zmetadata["zarr_consolidated_format"] = 1;

  zmetadata["metadata"][".zattrs"] = zattrs;
  zmetadata["metadata"][".zgroup"] = zgroup;

  std::string zarray_key;
  std::string zattrs_key;
  std::string driver =
      json_variables[0]["kvstore"]["driver"].get<std::string>();

  for (const auto& json : json_variables) {
    zarray_key =
        std::filesystem::path(json["kvstore"]["path"]).stem() / ".zarray";
    zattrs_key =
        std::filesystem::path(json["kvstore"]["path"]).stem() / ".zattrs";

    MDIO_ASSIGN_OR_RETURN(zmetadata["metadata"][zarray_key], get_zarray(json))

    nlohmann::json fixedJson = json["attributes"];
    fixedJson["_ARRAY_DIMENSIONS"] = fixedJson["dimension_names"];
    fixedJson.erase("dimension_names");
    // We do not want to be seralizing the variable_name. It should be
    // self-describing
    if (fixedJson.contains("variable_name")) {
      fixedJson.erase("variable_name");
    }
    if (fixedJson.contains("long_name") &&
        fixedJson["long_name"].get<std::string>() == "") {
      fixedJson.erase("long_name");
    }
    if (fixedJson.contains("metadata")) {
      if (fixedJson["metadata"].contains("chunkGrid")) {
        fixedJson["metadata"].erase("chunkGrid");
      }
      for (auto& item : fixedJson["metadata"].items()) {
        fixedJson[item.key()] = std::move(item.value());
      }
      fixedJson.erase("metadata");
    }
    // Case where an empty array of coordinates were provided
    if (fixedJson.contains("coordinates")) {
      auto coords = fixedJson["coordinates"];
      if (coords.empty() ||
          (coords.is_string() && coords.get<std::string>() == "")) {
        fixedJson.erase("coordinates");
      }
    }
    zmetadata["metadata"][zattrs_key] = fixedJson;
  }

  nlohmann::json kvstore = nlohmann::json::object();
  kvstore["driver"] = driver;
  std::vector<std::string> file_parts = absl::StrSplit(
      json_variables[0]["kvstore"]["path"].get<std::string>(), '/');
  size_t toRemove = file_parts.back().size();
  std::string strippedPath =
      json_variables[0]["kvstore"]["path"].get<std::string>().substr(
          0, json_variables[0]["kvstore"]["path"].get<std::string>().size() -
                 toRemove - 1);
  kvstore["path"] = strippedPath;

  if (driver == "gcs" || driver == "s3") {
    kvstore["bucket"] =
        json_variables[0]["kvstore"]["bucket"].get<std::string>();
    std::string cloudPath = kvstore["path"].get<std::string>();
    kvstore["path"] = cloudPath;
  }

  auto kvs_future = tensorstore::kvstore::Open(kvstore);

  auto zattrs_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zattrs = std::move(zattrs)](const tensorstore::KvStore& kvstore) {
        return tensorstore::kvstore::Write(kvstore, "/.zattrs",
                                           absl::Cord(zattrs.dump(4)));
      },
      kvs_future);

  auto zmetadata_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zmetadata = std::move(zmetadata)](const tensorstore::KvStore& kvstore) {
        return tensorstore::kvstore::Write(kvstore, "/.zmetadata",
                                           absl::Cord(zmetadata.dump(4)));
      },
      kvs_future);

  auto zgroup_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zgroup = std::move(zgroup)](const tensorstore::KvStore& kvstore) {
        return tensorstore::kvstore::Write(kvstore, "/.zgroup",
                                           absl::Cord(zgroup.dump(4)));
      },
      kvs_future);

  return tensorstore::WaitAllFuture(zattrs_future, zmetadata_future,
                                    zgroup_future);
}

/**
 * @brief Retrieves the .zmetadata for the dataset.
 * This is for executing a read on the dataset's consolidated metadata.
 * It will also attempt to infer the driver based on the prefix of the path.
 * It will default to the "file" driver if no prefix is found.
 * @param dataset_path The path to the dataset.
 */
Future<tensorstore::KvStore> dataset_kvs_store(
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
    kvstore["path"] = output_file;
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
  // update the bucket and path ...
  kvstore["bucket"] = bucket;
  kvstore["path"] = filepath;

  return tensorstore::kvstore::Open(kvstore);
}

/**
 * @brief Retrieves the .zmetadata for the dataset.
 * This is for executing a read on the dataset's consolidated metadata.
 * It will also attempt to infer the driver based on the prefix of the path.
 * It will default to the "file" driver if no prefix is found.
 * @param dataset_path The path to the dataset.
 * @return An `mdio::Future` containing the .zmetadata JSON on success, or an
 * error on failure.
 */
Future<std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>
from_zmetadata(const std::string& dataset_path) {
  // e.g. dataset_path = "zarrs/acceptance/";
  //  FIXME - enable async
  auto kvs_future = mdio::internal::dataset_kvs_store(dataset_path).result();

  if (!kvs_future.ok()) {
    return internal::CheckMissingDriverStatus(kvs_future.status());
  }
  auto kvs_read_result =
      tensorstore::kvstore::Read(kvs_future.value(), ".zmetadata").result();
  if (!kvs_read_result.ok()) {
    return internal::CheckMissingDriverStatus(kvs_read_result.status());
  }

  ::nlohmann::json zmetadata;
  try {
    zmetadata =
        ::nlohmann::json::parse(std::string(kvs_read_result.value().value));
  } catch (const nlohmann::json::parse_error& e) {
    // It's a common error to not have a trailing slash on the dataset path.
    if (!dataset_path.empty() && dataset_path.back() != '/') {
      std::string fixPath = dataset_path + "/";
      return mdio::internal::from_zmetadata(fixPath);
    }
    return absl::Status(absl::StatusCode::kInvalidArgument, e.what());
  }

  if (!zmetadata.contains("metadata")) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "zmetadata does not contain metadata.");
  }

  if (!zmetadata["metadata"].contains(".zattrs")) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "zmetadata does not contain dataset metadata.");
  }

  auto dataset_metadata = zmetadata["metadata"][".zattrs"];

  std::string driver = "file";
  // TODO(BrianMichell): Make this more robust. May be invalid if the stored
  // path gets mangled somehow. Infer the driver
  if (dataset_path.length() > 5) {
    if (dataset_path.substr(0, 5) == "gs://") {
      driver = "gcs";
    } else if (dataset_path.substr(0, 5) == "s3://") {
      driver = "s3";
    }
  }

  std::string bucket;
  std::string cloudPath;
  if (driver != "file") {
    std::string providedPath =
        dataset_path.substr(5);  // Strip the gs:// or s3://
    size_t bucketLen = providedPath.find_first_of('/');
    bucket = providedPath.substr(0, bucketLen);  // Extract the bucket name
    cloudPath = providedPath.substr(
        bucketLen + 1, providedPath.length() - 2);  // Extract the path
  }

  // Remove .zattrs from metadata
  zmetadata["metadata"].erase(".zattrs");
  std::vector<nlohmann::json> json_vars_from_zmeta;
  // Assemble a list of json for opening the variables in the dataset.
  for (auto& element : zmetadata["metadata"].items()) {
    // FIXME - remove hard code .zarray
    if (element.key().substr(element.key().find_last_of(".") + 1) == "zarray") {
      std::string variable_name =
          element.key().substr(0, element.key().find("/"));
      nlohmann::json new_dict = {
          {"driver", "zarr"},
          {"kvstore",
           {{"driver", driver}, {"path", dataset_path + "/" + variable_name}}}};
      if (driver != "file") {
        new_dict["kvstore"]["bucket"] = bucket;
        new_dict["kvstore"]["path"] = cloudPath + variable_name;
      }
      json_vars_from_zmeta.push_back(new_dict);
    }
  }
  if (!json_vars_from_zmeta.size()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "Not variables found in zmetadata.");
  }

  return tensorstore::ReadyFuture<
      std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>(
      std::make_tuple(dataset_metadata, json_vars_from_zmeta));
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

  // Result<Dataset> CoordinateTransform(const std::vector<std::string>& new_domain_dims) const {
  //   bool dimensionTransform = true;
  //   for (const auto& dim : new_domain_dims) {
  //     MDIO_ASSIGN_OR_RETURN(auto var, variables.at(dim));
  //     MDIO_ASSIGN_OR_RETURN(auto shape, var.get_store_shape());
  //     if (shape.size() != 1) {
  //       dimensionTransform = false;
  //       return absl::UnimplementedError("Only Dimension Coordinate Transforms are supported currently.");
  //     }
  //   }

  //   if (dimensionTransform) {
  //     std::vector<Index> new_order;
  //     MDIO_ASSIGN_OR_RETURN(auto imageVar, variables.at("image"));
  //     MDIO_ASSIGN_OR_RETURN(auto imageIntervals, imageVar.get_intervals());
  //     for (std::size_t imageDimIdx = 0; imageDimIdx < imageIntervals.size(); ++imageDimIdx) {
  //       std::string dimLabel(imageIntervals[imageDimIdx].label.label());
  //       for (std::size_t newDimIdx = 0; newDimIdx < new_domain_dims.size(); ++newDimIdx) {
  //         if (new_domain_dims[newDimIdx] == dimLabel) {
  //           new_order.push_back(newDimIdx);
  //           break;
  //         }
  //       }
  //     }
  //     MDIO_ASSIGN_OR_RETURN(auto flipped, imageVar.get_mutable_store() | tensorstore::AllDims().Transpose(new_order));
  //     std::cout << "The flipped order should be: " << std::endl;
  //     for (const auto& dim : new_order) {
  //       std::cout << "\t" << dim << std::endl;
  //     }
  //     std::cout << std::endl;
  //     std::cout << "Image variable before setting flipped store: " << std::endl;
  //     std::cout << imageVar << std::endl;
  //     imageVar.set_store(flipped);
  //     std::cout << "Image variable after setting flipped store: " << std::endl;
  //     std::cout << imageVar << std::endl;
  //     return Dataset{metadata, variables, coordinates, domain};
  //   }

  //   return absl::UnimplementedError("CoordinateTransform reached unexpected path.");
  // }

  Result<Dataset> CoordinateTransform(
    const std::vector<std::string>& new_domain_dims) const {
  // 1) Determine if this is the “all 1‑D coords” fast path
  bool dimensionTransform = true;
  for (const auto& dim : new_domain_dims) {
    MDIO_ASSIGN_OR_RETURN(auto var, variables.at(dim));
    MDIO_ASSIGN_OR_RETURN(auto shape, var.get_store_shape());
    if (shape.size() != 1) {
      dimensionTransform = false;
      break;
    }
  }

  // 2) Make mutable copies of everything you’ll need
  auto new_vars = variables;
  // NOTE: domain is a tensorstore::IndexDomain<…>, so we won’t index it by string
  auto new_coords = coordinates;

  // 3) Grab a reference to the real “image” variable in your map
  MDIO_ASSIGN_OR_RETURN(auto imageVar, new_vars.at("image"));

  // 4) Build a map: original_axis_label → its numeric index
  MDIO_ASSIGN_OR_RETURN(auto imageIntervals, imageVar.get_intervals());
  absl::flat_hash_map<std::string, tensorstore::DimensionIndex> axis_map;
  for (tensorstore::DimensionIndex i = 0; i < imageIntervals.size(); ++i) {
    axis_map[imageIntervals[i].label.label()] = i;
  }

  // 5) Compute the run‑time permutation vector
  std::vector<tensorstore::DimensionIndex> permuted_axes;
  permuted_axes.reserve(imageIntervals.size());

  if (dimensionTransform) {
    // Simple 1‑D case — each new_domain_dims entry is exactly one axis:
    for (const auto& label : new_domain_dims) {
      permuted_axes.push_back(axis_map[label]);
    }

  } else {
    // Mixed 1‑D & N‑D coordinates:
    for (const auto& coord_label : new_domain_dims) {
      // coordVar may have shape.size()>1, so it contributes multiple axes
      MDIO_ASSIGN_OR_RETURN(auto coordVar, new_vars.at(coord_label));
      MDIO_ASSIGN_OR_RETURN(auto coordIntervals, coordVar.get_intervals());
      for (const auto& iv : coordIntervals) {
        permuted_axes.push_back(axis_map[iv.label.label()]);
      }
    }

    // If you want to track the new list of coordinate names *for* “image”:
    new_coords["image"] = new_domain_dims;
    // (We leave `domain` alone here, since it’s an IndexDomain and must be
    // reordered via a TensorStore‐style transpose if you really need it.)
  }

  // 6) Apply the zero‑copy transpose to the store
  MDIO_ASSIGN_OR_RETURN(
      auto flipped_store,
      imageVar
        .get_mutable_store()
        | tensorstore::AllDims().Transpose(permuted_axes));


  std::cout << "Image variable before setting flipped store: " << std::endl;
  std::cout << imageVar << std::endl;
  imageVar.set_store(flipped_store);
  std::cout << "Image variable after setting flipped store: " << std::endl;
  std::cout << imageVar << std::endl;

  // 7) Return a new Dataset with the updated image var (and coords, if you set them)
  return Dataset{metadata,
                 std::move(new_vars),
                 std::move(new_coords),
                 domain  // unchanged; reorder via domain.Transform if/when needed
  };
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
   * @brief Constructs a Dataset from a JSON schema.
   * This method will validate the JSON schema against the MDIO Dataset schema.
   * @param json_schema The JSON schema to validate.
   * @details \b Usage
   *
   * Create  a dataset given a schema and a path, for a new dataset use options,
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

    // std::cout << "isel forwarded descriptors..." << std::endl;
    // ((std::cout << "Descriptor: " << descriptors.label.label() << " " 
    //             << descriptors.start << " " << descriptors.stop << " " 
    //             << descriptors.step << std::endl), ...);
    // std::cout << "================================================" << std::endl;

    // the shape of the new domain
    std::map<std::string, tensorstore::IndexDomainDimension<>> dims;
    std::vector<std::string> keys = variables.get_iterable_accessor();

    // std::cout << "keys: " << std::endl;
    // for (const auto& key : keys) {
    //   std::cout << key << std::endl;
    // }

    for (const auto& name : keys) {
      MDIO_ASSIGN_OR_RETURN(auto retreivedVar, variables.at(name));
      MDIO_ASSIGN_OR_RETURN(auto variable,
                            retreivedVar.slice(
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

  /// Merge overlapping or adjacent descriptors with the same label & step.
  // std::vector<RangeDescriptor<Index>> merge_adjacent(std::vector<RangeDescriptor<Index>> descs) {
  //   // 1) Bucket by (label, step)
  //   using Key = std::pair<std::string,Index>;
  //   std::map<Key, std::vector<RangeDescriptor<Index>>> buckets;
  //   for (auto &d : descs) {
  //     Key k = std::make_pair(std::string(d.label.label()), d.step);
  //     buckets[k].push_back(d);
  //   }

  //   std::vector<RangeDescriptor<Index>> result;
  //   result.reserve(descs.size());

  //   // 2) For each bucket, sort & merge
  //   for (auto &kv : buckets) {
  //     const auto &[label, step] = kv.first;
  //     auto &vec = kv.second;

  //     std::sort(vec.begin(), vec.end(),
  //               [](auto const &a, auto const &b) {
  //                 return a.start < b.start;
  //               });

  //     // 3) Sweep through and merge
  //     Index cur_start = vec[0].start;
  //     Index cur_stop  = vec[0].stop;

  //     for (size_t i = 1; i < vec.size(); ++i) {
  //       if (vec[i].start <= cur_stop) {
  //         // overlap or adjacent
  //         cur_stop = std::max(cur_stop, vec[i].stop);
  //       } else {
  //         // emit the completed run
  //         std::string lab = label;
  //         result.push_back({std::move(lab), cur_start, cur_stop, step});
  //         // start a new one
  //         cur_start = vec[i].start;
  //         cur_stop  = vec[i].stop;
  //       }
  //     }
  //     std::string lab = label;
  //     // emit final run for this bucket
  //     result.push_back({std::move(lab), cur_start, cur_stop, step});
  //   }

  //   return result;
  // }
  std::vector<RangeDescriptor<Index>> merge_adjacent(
    std::vector<RangeDescriptor<Index>> descs) {
  // 1) bucket by (label, step) using a string_view key
  using Key = std::pair<std::string_view, Index>;
  std::map<Key, std::vector<RangeDescriptor<Index>>> buckets;

  for (auto &d : descs) {
    buckets[{ d.label.label(), d.step }].push_back(d);
  }

  std::vector<RangeDescriptor<Index>> result;
  result.reserve(descs.size());

  // 2) sort & merge each bucket
  for (auto &kv : buckets) {
    auto &vec = kv.second;
    std::sort(vec.begin(), vec.end(),
              [](auto const &a, auto const &b) {
                return a.start < b.start;
              });

    Index cur_start = vec[0].start;
    Index cur_stop  = vec[0].stop;

    for (size_t i = 1; i < vec.size(); ++i) {
      if (vec[i].start <= cur_stop) {
        cur_stop = std::max(cur_stop, vec[i].stop);
      } else {
        // copy the *original* descriptor (with a safe label)
        auto run = vec[0];
        run.start = cur_start;
        run.stop  = cur_stop;
        result.push_back(std::move(run));

        cur_start = vec[i].start;
        cur_stop  = vec[i].stop;
      }
    }

    // emit the last run
    auto run = vec[0];
    run.start = cur_start;
    run.stop  = cur_stop;
    result.push_back(std::move(run));
  }

  return result;
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

    auto reducedSlices = merge_adjacent(slices);

    bool do_simple_slice = true;

    // Build a set of just the labels
    std::set<std::string_view> labels;
    labels.insert(reducedSlices[0].label.label());
    for (auto i=1; i<reducedSlices.size(); i++) {
      if (labels.count(reducedSlices[i].label.label()) > 0) {
        do_simple_slice = false;
        // break;  // Don't break here, we can check all the labels and see if we are left with a single dimension.
      }
      labels.insert(reducedSlices[i].label.label());
    }

    // If we are left with a single dimension, we can just do a simple slice.
    if (labels.size() == 1) {
      do_simple_slice = true;
    }

    // Debugging print for all RangeDescriptors pending
    // std::cout << "Reduced slices: " << std::endl;
    // for (auto &slice : reducedSlices) {
    //   std::cout << "[" << slice.label.label() << ", " << slice.start << ", " << slice.stop << ", " << slice.step << "]" << std::endl;
    // }

    // Pre-emptively split the RangeDescriptors if there are too many.
    // This is not the final logic that we want.
    if (reducedSlices.size() > internal::kMaxNumSlices) {
      // std::cout << "Recursively slicing the dataset..." << std::endl;
      std::size_t halfElements = reducedSlices.size() / 2;
      if (halfElements % 2 != 0) {
        halfElements += 1;
      }
      std::vector<RangeDescriptor<Index>> firstHalf(reducedSlices.begin(), reducedSlices.begin() + halfElements);
      std::vector<RangeDescriptor<Index>> secondHalf(reducedSlices.begin() + halfElements, reducedSlices.end());
      MDIO_ASSIGN_OR_RETURN(auto ds, isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(firstHalf)));
      return ds.isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(secondHalf));
    }

    if (do_simple_slice) {
      std::vector<RangeDescriptor<Index>> slicesCopy = reducedSlices;
      for (int i = reducedSlices.size(); i <= internal::kMaxNumSlices; i++) {
        slicesCopy.emplace_back(
            RangeDescriptor<Index>({internal::kInertSliceKey, 0, 1, 1}));
      }

      // Generate the index sequence and call the implementation
      return call_isel_with_vector_impl(
          slicesCopy, std::make_index_sequence<internal::kMaxNumSlices>{});
    } else {
      std::vector<RangeDescriptor<Index>> simpleSlices;
      std::vector<RangeDescriptor<Index>> complexSlices;
      auto simpleLabel = reducedSlices[0].label.label();
      for (auto &slice : reducedSlices) {
        if (slice.label.label() != simpleLabel) {
          complexSlices.push_back(slice);
        } else {
          simpleSlices.push_back(slice);
        }
      }
      MDIO_ASSIGN_OR_RETURN(auto ds, isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(simpleSlices)));
      return ds.isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(complexSlices));
    }
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
          for (Index i = offset; i < varDat.num_samples() + offset; ++i) {
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
        for (Index i = offset; i < varDat.num_samples() + offset; ++i) {
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

      return isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
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

      return isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
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

        for (Index i = offset; i < varDat.num_samples() + offset; i++) {
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

      return isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(slices));
    }

    return absl::OkStatus();
  }

  template <typename T>
  void _current_position_increment(std::vector<typename Variable<T>::Interval>& positionInterval, const std::vector<typename Variable<T>::Interval>& interval) {
    for (std::size_t d = positionInterval.size(); d-- > 0; ) {
      if (positionInterval[d].inclusive_min + 1 < interval[d].exclusive_max) {
        ++positionInterval[d].inclusive_min;
        return;
      }
      positionInterval[d].inclusive_min = interval[d].inclusive_min;
    }

    // Should be unreachable.
  }

  template <typename T>
  Future<Dataset> where(const ValueDescriptor<T>& coord_desc) {
  // 1) Lookup the coordinate Variable<T>
  auto varRes =
      variables.get<T>(std::string(coord_desc.label.label()));
  if (!varRes.status().ok()) {
    return varRes.status();
  }
  auto var = varRes.value();

  MDIO_ASSIGN_OR_RETURN(auto interval, var.get_intervals());
  std::vector<typename Variable<T>::Interval> currentPos;  // Hacky, we will use this to track our current position in the dataset.
  for (const auto& i : interval) {
    currentPos.push_back(i);
  }

  // 2) Read its data
  auto varFut = var.Read();
  if (!varFut.status().ok()) {
    return varFut.status();
  }
  auto varDat = varFut.value();

  // **Use the flattened data pointer + offset for N‑D arrays:**
  auto* data_ptr = varDat.get_data_accessor().data();
  Index offset   = varDat.get_flattened_offset();
  Index nSamples = varDat.num_samples();

  // 3) Collect all flat indices where coord == target value
  std::vector<Index> indices;
  std::vector<RangeDescriptor<Index>> elementwiseSlices;
  for (Index idx = offset; idx < offset + nSamples; ++idx) {
    if (data_ptr[idx] == coord_desc.value) {
      // std::cout << "Found value at index: " << idx << std::endl;
      indices.push_back(idx);
      for (const auto& pos : currentPos) {
        elementwiseSlices.emplace_back(RangeDescriptor<Index>({pos.label, pos.inclusive_min, pos.inclusive_min+1, 1}));
        // std::cout << pos << std::endl;
      }
    }
    this->_current_position_increment<T>(currentPos, interval);
  }

  if (indices.empty()) {
    return absl::NotFoundError(
      "where(): no entries match coordinate '" +
      std::string(coord_desc.label.label()) + "'");
  }

  // TODO(BrianMichell): Coalesce the slices into fewer descriptors.

  // std::cout << "All RangeDescriptors: " << std::endl;
  // for (const auto& slice : elementwiseSlices) {
  //   // std::cout << slice << std::endl;
  //   std::cout << "[" << slice.label << ", " << slice.start << ", " << slice.stop << ", " << slice.step << "]" << std::endl;
  // }

  MDIO_ASSIGN_OR_RETURN(auto ds, isel(static_cast<const std::vector<RangeDescriptor<Index>>&>(elementwiseSlices)));
  // TODO(BrianMichell): Make this method more async friendly.
  return tensorstore::ReadyFuture<Dataset>(std::move(ds));
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
    TENSORSTORE_INTERNAL_ASSIGN_OPTIONS_OR_RETURN(TransactionalOpenOptions,
                                                  transact_options, options)

    if (transact_options.open_mode != constants::kOpen) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "Open from path is only valid in open-mode.");
    }

    MDIO_ASSIGN_OR_RETURN(auto params_from_zmetadata,
                          mdio::internal::from_zmetadata(dataset_path).result())
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
    TENSORSTORE_INTERNAL_ASSIGN_OPTIONS_OR_RETURN(TransactionalOpenOptions,
                                                  transact_options, options)
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

    // Create a new Variable with the selected field
    std::string baseStr = R"(
            {
                "driver": "zarr",
                "field": "FIELD",
                "kvstore": {
                    "driver": "DRIVER",
                    "path": "PATH"
                }
            }
        )";
    nlohmann::json base = nlohmann::json::parse(baseStr, nullptr, false);
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
      cloudPath
          .pop_back();  // We need to remove the trailing / from a cloud path
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
