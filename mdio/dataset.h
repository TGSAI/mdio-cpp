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

#include <fstream>
#include <map>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mdio/dataset_factory.h"
#include "mdio/variable.h"
#include "mdio/variable_collection.h"
#include "tensorstore/driver/zarr/metadata.h"
#include "tensorstore/util/future.h"

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
    fixedJson["_ARRAY_DIMENSIONS"] = json["attributes"]["dimension_names"];
    fixedJson.erase("dimension_names");
    // We do not want to be seralizing the variable_name. It should be
    // self-describing
    fixedJson.erase("variable_name");
    if (fixedJson.contains("long_name") &&
        fixedJson["long_name"].get<std::string>() == "") {
      fixedJson.erase("long_name");
    }
    if (fixedJson.contains("metadata")) {
      fixedJson["metadata"].erase("chunkGrid");
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
                                           absl::Cord(zattrs.dump()));
      },
      kvs_future);

  auto zmetadata_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zmetadata = std::move(zmetadata)](const tensorstore::KvStore& kvstore) {
        return tensorstore::kvstore::Write(kvstore, "/.zmetadata",
                                           absl::Cord(zmetadata.dump()));
      },
      kvs_future);

  auto zgroup_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zgroup = std::move(zgroup)](const tensorstore::KvStore& kvstore) {
        return tensorstore::kvstore::Write(kvstore, "/.zgroup",
                                           absl::Cord(zgroup.dump()));
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

  auto kvs_read_result =
      tensorstore::kvstore::Read(kvs_future.value(), ".zmetadata").result();

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
    os << "Metadata: " << dataset.metadata.dump() << "\n";

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
   * mdio::SliceDescriptor desc1 = {"inline", 20, 120, 1};
   * mdio::SliceDescriptor desc2 = {"crossline", 100, 200, 1};
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
    all_done_future.ExecuteWhenReady([promise = std::move(pair.promise),
                                      variables = std::move(variables),
                                      metadata](tensorstore::ReadyFuture<void>
                                                    readyFut) {
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
          std::vector<std::string> coords_vec = absl::StrSplit(coords_str, ' ');
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

      auto dataset_domain = tensorstore::IndexDomainBuilder<>(shape_size.size())
                                .shape(values)
                                .labels(keys)
                                .Finalize();

      if (!dataset_domain.ok()) {
        promise.SetResult(dataset_domain.status());
        return;
      }

      Dataset new_dataset{metadata, collection, coords, dataset_domain.value()};
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
  Future<Variable<>> SelectField(const std::string variableName,
                                 const std::string fieldName) {
    // Ensure that the variable exists in the Dataset
    if (!variables.contains_key(variableName)) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "Variable '" + variableName + "' not found in the dataset.");
    }

    // Grab the Variable from the Dataset
    auto varRes = variables.get(variableName);
    if (!varRes.status().ok()) {
      return varRes.status();
    }
    mdio::Variable var = varRes.value();

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
    nlohmann::json base = nlohmann::json::parse(baseStr);
    if (found >= 0) {
      base["field"] = specJson["metadata"]["dtype"][found][0];
    } else {
      base.erase("field");
    }
    base["kvstore"]["driver"] = specJson["kvstore"]["driver"];
    base["kvstore"]["path"] = specJson["kvstore"]["path"];

    // Handle cloud stores
    if (specJson["kvstore"].contains("bucket")) {
      base["kvstore"]["bucket"] = specJson["kvstore"]["bucket"];
      std::string cloudPath = base["kvstore"]["path"].get<std::string>();
      cloudPath
          .pop_back();  // We need to remove the trailing / from a cloud path
      base["kvstore"]["path"] = cloudPath;
    }

    auto fieldedVar = mdio::Variable<>::Open(base, constants::kOpen);

    auto pair = tensorstore::PromiseFuturePair<mdio::Variable<>>::Make();
    fieldedVar.ExecuteWhenReady(
        [this, promise = pair.promise,
         variableName](tensorstore::ReadyFuture<mdio::Variable<>> readyFut) {
          auto ready_result = readyFut.result();
          if (!ready_result.ok()) {
            promise.SetResult(ready_result.status());
          } else {
            this->variables.add(variableName, ready_result.value());
            promise.SetResult(ready_result);
          }
        });
    return pair.future;
  }

  tensorstore::Future<void> CommitMetadata() {
    auto keys = variables.get_iterable_accessor();

    // Build out list of modified variables
    std::vector<std::string> modifiedVariables;
    for (const auto& key : keys) {
      modifiedVariables.push_back(key);
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
    std::vector<Variable<>>
        vars;  // Keeps the Variables in memory. Fix for premature reference
               // decrement in LLVM compiler.
    for (auto key : keys) {
      auto var = variables.at(key).value();
      vars.push_back(var);
      // Get the JSON, drop transform, and add attributes
      nlohmann::json json =
          var.get_store().spec().value().ToJson(IncludeDefaults{}).value();
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
      nlohmann::json meta = var.getMetadata();
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
        meta["metadata"].erase("chunkGrid");  // We never serialize this
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

    vars.clear();  // Clear the vector so we can add only the modified Variables
    for (auto key : modifiedVariables) {
      auto pair = tensorstore::PromiseFuturePair<
          tensorstore::TimestampedStorageGeneration>::Make();
      auto var = variables.at(key).value();
      vars.push_back(var);
      auto updateFuture = var.PublishMetadata();
      updateFuture.ExecuteWhenReady(
          [promise = std::move(pair.promise)](
              tensorstore::ReadyFuture<
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

  const nlohmann::json& getMetadata() const { return metadata; }

  // variables contained in the dataset
  VariableCollection variables;

  // link a variable name to its coordinates via its name(s)
  coordinate_map coordinates;

  // enumerate the dimensions
  tensorstore::IndexDomain<> domain;

 private:
  // the metadata associated with the dataset (root .zattrs)
  ::nlohmann::json metadata;
};
}  // namespace mdio
