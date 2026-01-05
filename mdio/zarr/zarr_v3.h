// Copyright 2026 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MDIO_ZARR_ZARR_V3_H_
#define MDIO_ZARR_ZARR_V3_H_

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "mdio/impl.h"
#include "mdio/zarr/zarr_driver.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/util/future.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace mdio {
namespace zarr {
namespace v3 {

/**
 * @brief Gets the Zarr V3 driver name.
 * @return constexpr std::string_view The driver name.
 */
constexpr std::string_view kDriverName = "zarr3";

/**
 * @brief The Zarr format version for V3.
 */
constexpr int kZarrFormat = 3;

/**
 * @brief The node type for a group in Zarr V3.
 */
constexpr std::string_view kGroupNodeType = "group";

/**
 * @brief The node type for an array in Zarr V3.
 */
constexpr std::string_view kArrayNodeType = "array";

/**
 * @brief Converts a dtype string to Zarr V3 format.
 *
 * Zarr V3 uses a different dtype specification than V2.
 *
 * @param dtype The input dtype string.
 * @return Result<nlohmann::json> The Zarr V3 dtype specification.
 */
inline Result<nlohmann::json> ToZarrDtype(const std::string& dtype) {
  // Zarr V3 uses different dtype specification
  if (dtype == "int8") return "int8";
  if (dtype == "int16") return "int16";
  if (dtype == "int32") return "int32";
  if (dtype == "int64") return "int64";
  if (dtype == "uint8") return "uint8";
  if (dtype == "uint16") return "uint16";
  if (dtype == "uint32") return "uint32";
  if (dtype == "uint64") return "uint64";
  if (dtype == "float16") return "float16";
  if (dtype == "float32") return "float32";
  if (dtype == "float64") return "float64";
  if (dtype == "bool") return "bool";
  if (dtype == "complex64") return "complex64";
  if (dtype == "complex128") return "complex128";
  return absl::InvalidArgumentError("Unknown dtype: " + dtype);
}

/**
 * @brief Creates the root zarr.json for a Zarr V3 group.
 *
 * @param attributes Optional attributes to include.
 * @return nlohmann::json The zarr.json content for the group.
 */
inline nlohmann::json CreateGroupMetadata(
    const nlohmann::json& attributes = nlohmann::json::object()) {
  nlohmann::json zarr_json;
  zarr_json["zarr_format"] = kZarrFormat;
  zarr_json["node_type"] = kGroupNodeType;
  if (!attributes.empty()) {
    zarr_json["attributes"] = attributes;
  }
  return zarr_json;
}

/**
 * @brief Creates the zarr.json for a Zarr V3 array.
 *
 * @param shape The array shape.
 * @param chunks The chunk shape.
 * @param dtype The data type.
 * @param attributes Optional attributes.
 * @param fill_value Optional fill value.
 * @param codecs Optional codec configuration.
 * @return nlohmann::json The zarr.json content for the array.
 */
inline nlohmann::json CreateArrayMetadata(
    const std::vector<int64_t>& shape, const std::vector<int64_t>& chunks,
    const std::string& dtype,
    const nlohmann::json& attributes = nlohmann::json::object(),
    const nlohmann::json& fill_value = nullptr,
    const nlohmann::json& codecs = nlohmann::json::array()) {
  nlohmann::json zarr_json;
  zarr_json["zarr_format"] = kZarrFormat;
  zarr_json["node_type"] = kArrayNodeType;
  zarr_json["shape"] = shape;

  // Chunk configuration for V3
  zarr_json["chunk_grid"] = {{"name", "regular"},
                             {"configuration", {{"chunk_shape", chunks}}}};

  // Chunk key encoding (default to slash separator)
  zarr_json["chunk_key_encoding"] = {{"name", "default"},
                                     {"configuration", {{"separator", "/"}}}};

  auto dtype_result = ToZarrDtype(dtype);
  if (dtype_result.ok()) {
    zarr_json["data_type"] = dtype_result.value();
  } else {
    zarr_json["data_type"] = dtype;
  }

  zarr_json["fill_value"] = fill_value;

  // Default codecs if not specified
  if (codecs.empty()) {
    zarr_json["codecs"] = nlohmann::json::array({{{"name", "bytes"}}});
  } else {
    zarr_json["codecs"] = codecs;
  }

  if (!attributes.empty()) {
    zarr_json["attributes"] = attributes;
  }

  return zarr_json;
}

/**
 * @brief Prepares MDIO attributes for Zarr V3 format.
 *
 * In Zarr V3, attributes are embedded directly in zarr.json.
 * For xarray compatibility, dimension_names is converted to _ARRAY_DIMENSIONS.
 *
 * @param json The variable JSON specification.
 * @return nlohmann::json The prepared attributes for Zarr V3.
 */
inline nlohmann::json PrepareVariableAttributes(const nlohmann::json& json) {
  nlohmann::json attrs = nlohmann::json::object();

  if (json.contains("attributes")) {
    attrs = json["attributes"];
  }

  // Convert dimension_names to _ARRAY_DIMENSIONS for xarray compatibility
  if (attrs.contains("dimension_names")) {
    attrs["_ARRAY_DIMENSIONS"] = attrs["dimension_names"];
    attrs.erase("dimension_names");
  }

  // Remove variable_name as it's self-describing from path
  if (attrs.contains("variable_name")) {
    attrs.erase("variable_name");
  }

  // Handle empty long_name
  if (attrs.contains("long_name") &&
      attrs["long_name"].get<std::string>() == "") {
    attrs.erase("long_name");
  }

  // Flatten metadata
  if (attrs.contains("metadata")) {
    if (attrs["metadata"].contains("chunkGrid")) {
      attrs["metadata"].erase("chunkGrid");
    }
    for (auto& item : attrs["metadata"].items()) {
      attrs[item.key()] = std::move(item.value());
    }
    attrs.erase("metadata");
  }

  // Handle empty coordinates
  if (attrs.contains("coordinates")) {
    auto coords = attrs["coordinates"];
    if (coords.empty() ||
        (coords.is_string() && coords.get<std::string>() == "")) {
      attrs.erase("coordinates");
    }
  }

  return attrs;
}

/**
 * @brief Writes metadata for a Zarr V3 dataset (NO consolidated metadata).
 *
 * For Zarr V3, we write individual zarr.json files for each array and the
 * group. Consolidated metadata is NOT supported for Zarr V3.
 *
 * @param dataset_metadata The metadata for the dataset.
 * @param json_variables The JSON variables.
 * @param context Optional TensorStore context for credentials/configuration.
 * @return An `mdio::Future<void>` representing the asynchronous write.
 */
inline Future<void> WriteMetadata(
    const ::nlohmann::json& dataset_metadata,
    const std::vector<::nlohmann::json>& json_variables,
    tensorstore::Context context = tensorstore::Context::Default()) {
  // Determine the base path and driver
  std::string driver =
      json_variables[0]["kvstore"]["driver"].get<std::string>();

  std::vector<std::string> file_parts = absl::StrSplit(
      json_variables[0]["kvstore"]["path"].get<std::string>(), '/');
  size_t toRemove = file_parts.back().size();
  std::string basePath =
      json_variables[0]["kvstore"]["path"].get<std::string>().substr(
          0, json_variables[0]["kvstore"]["path"].get<std::string>().size() -
                 toRemove - 1);
  // Ensure basePath has trailing slash for directory operations
  if (!basePath.empty() && basePath.back() != '/') {
    basePath.push_back('/');
  }

  nlohmann::json kvstore = nlohmann::json::object();
  kvstore["driver"] = driver;
  kvstore["path"] = basePath;

  if (driver == "gcs" || driver == "s3") {
    kvstore["bucket"] =
        json_variables[0]["kvstore"]["bucket"].get<std::string>();
  }

  auto kvs_future = tensorstore::kvstore::Open(kvstore, context);

  // Create root metadata (variables are discovered via directory listing,
  // not stored in zarr.json)
  auto root_metadata = CreateGroupMetadata(dataset_metadata);

  auto root_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [root_metadata](const tensorstore::KvStore& kvs) {
        return tensorstore::kvstore::Write(kvs, "zarr.json",
                                           absl::Cord(root_metadata.dump(4)));
      },
      kvs_future);

  // Collect all write futures
  std::vector<tensorstore::AnyFuture> write_futures;
  write_futures.push_back(root_future);

  // Note: Individual array zarr.json files are written by TensorStore
  // when the variable is created, so we don't need to write them here.
  // We only write the root group metadata.

  return tensorstore::WaitAllFuture(write_futures);
}

/**
 * @brief Reads dataset metadata from Zarr V3 format (non-consolidated).
 *
 * For Zarr V3, we read the root zarr.json and then discover arrays
 * by listing the kvstore contents and checking each zarr.json for
 * node_type="array" (skipping groups).
 *
 * @param dataset_path The path to the dataset.
 * @param kvs_future A future to the KvStore.
 * @return A future containing dataset metadata and variable JSON specs.
 */
inline Future<std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>
ReadMetadata(const std::string& dataset_path,
             tensorstore::Future<tensorstore::KvStore> kvs_future) {
  auto pair = tensorstore::PromiseFuturePair<
      std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>::Make();

  kvs_future.ExecuteWhenReady(
      [promise = std::move(pair.promise),
       dataset_path](tensorstore::ReadyFuture<tensorstore::KvStore> ready_kvs) {
        if (!ready_kvs.result().ok()) {
          promise.SetResult(ready_kvs.result().status());
          return;
        }

        auto kvs = ready_kvs.value();

        // Read root zarr.json
        auto root_read_future = tensorstore::kvstore::Read(kvs, "zarr.json");

        root_read_future.ExecuteWhenReady(
            [promise = std::move(promise), dataset_path,
             kvs](tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
                      root_read_ready) mutable {
              if (!root_read_ready.result().ok()) {
                promise.SetResult(root_read_ready.result().status());
                return;
              }

              ::nlohmann::json root_json;
              try {
                root_json = ::nlohmann::json::parse(
                    std::string(root_read_ready.value().value));
              } catch (const nlohmann::json::parse_error& e) {
                promise.SetResult(absl::InvalidArgumentError(
                    std::string("JSON parse error: ") + e.what()));
                return;
              }

              // Extract attributes as dataset metadata
              nlohmann::json dataset_metadata = nlohmann::json::object();
              if (root_json.contains("attributes")) {
                dataset_metadata = root_json["attributes"];
              }

              // Infer the driver from path prefix
              std::string driver = "file";
              if (dataset_path.length() > 5) {
                if (dataset_path.substr(0, 5) == "gs://") {
                  driver = "gcs";
                } else if (dataset_path.substr(0, 5) == "s3://") {
                  driver = "s3";
                }
              }

              // Normalize path - remove trailing slash if present
              std::string normalized_path = dataset_path;
              while (!normalized_path.empty() &&
                     normalized_path.back() == '/') {
                normalized_path.pop_back();
              }

              // Use kvstore List to discover child zarr.json files
              auto list_future = tensorstore::kvstore::ListFuture(kvs);
              list_future.ExecuteWhenReady(
                  [promise = std::move(promise), dataset_metadata,
                   normalized_path, driver,
                   kvs](tensorstore::ReadyFuture<
                        std::vector<tensorstore::kvstore::ListEntry>>
                            list_ready) mutable {
                    if (!list_ready.result().ok()) {
                      promise.SetResult(list_ready.result().status());
                      return;
                    }

                    // Find all child zarr.json files (pattern: NAME/zarr.json)
                    std::vector<std::string> candidates;
                    for (const auto& entry : list_ready.value()) {
                      std::string key = entry.key;
                      // Look for pattern: something/zarr.json (one level deep)
                      size_t slash_pos = key.find('/');
                      if (slash_pos != std::string::npos &&
                          key.substr(slash_pos + 1) == "zarr.json") {
                        std::string name = key.substr(0, slash_pos);
                        // Avoid duplicates
                        if (std::find(candidates.begin(), candidates.end(),
                                      name) == candidates.end()) {
                          candidates.push_back(name);
                        }
                      }
                    }

                    if (candidates.empty()) {
                      promise.SetResult(absl::InvalidArgumentError(
                          "No Zarr V3 arrays found in directory."));
                      return;
                    }

                    // Read all child zarr.json files to check node_type
                    using ReadFuture =
                        tensorstore::Future<tensorstore::kvstore::ReadResult>;
                    auto read_futures =
                        std::make_shared<std::vector<ReadFuture>>();
                    std::vector<tensorstore::AnyFuture> any_futures;

                    for (const auto& name : candidates) {
                      auto f =
                          tensorstore::kvstore::Read(kvs, name + "/zarr.json");
                      read_futures->push_back(f);
                      any_futures.push_back(std::move(f));
                    }

                    // Wait for all reads and then filter by node_type
                    auto all_reads = tensorstore::WaitAllFuture(any_futures);
                    all_reads.ExecuteWhenReady(
                        [promise = std::move(promise), dataset_metadata,
                         normalized_path, driver, candidates, read_futures](
                            tensorstore::ReadyFuture<void>) mutable {
                          std::vector<nlohmann::json> json_vars;

                          for (size_t i = 0; i < candidates.size(); ++i) {
                            const auto& read_result =
                                (*read_futures)[i].result();
                            if (!read_result.ok() ||
                                !read_result->has_value()) {
                              continue;
                            }
                            try {
                              auto child_json = ::nlohmann::json::parse(
                                  std::string(read_result->value));
                              // Only include arrays, skip groups
                              if (child_json.contains("node_type") &&
                                  child_json["node_type"].get<std::string>() ==
                                      "array") {
                                nlohmann::json var_spec = {
                                    {"driver", std::string(kDriverName)},
                                    {"kvstore",
                                     {{"driver", driver},
                                      {"path", normalized_path + "/" +
                                                   candidates[i]}}}};
                                json_vars.push_back(var_spec);
                              }
                            } catch (const nlohmann::json::exception&) {
                              // Skip malformed zarr.json
                            }
                          }

                          if (json_vars.empty()) {
                            promise.SetResult(absl::InvalidArgumentError(
                                "No Zarr V3 arrays found in directory."));
                            return;
                          }

                          promise.SetResult(
                              std::make_tuple(dataset_metadata, json_vars));
                        });
                  });
            });
      });

  return pair.future;
}

/**
 * @brief Creates a Variable spec for Zarr V3.
 * @param variable_name The name of the variable.
 * @param path The path prefix.
 * @return nlohmann::json The variable spec stub.
 */
inline nlohmann::json CreateVariableSpec(const std::string& variable_name,
                                         const std::string& path = "") {
  nlohmann::json spec = {
      {"driver", std::string(kDriverName)},
      {"kvstore", {{"driver", "file"}, {"path", variable_name}}},
      {"metadata",
       {{"data_type", "DATA_TYPE"},
        {"shape", nlohmann::json::array()},
        {"chunk_grid",
         {{"name", "regular"},
          {"configuration", {{"chunk_shape", nlohmann::json::array()}}}}}}},
      {"attributes", nlohmann::json::object()}};
  return spec;
}

/**
 * @brief Writes variable attributes to zarr.json (embedded in array metadata).
 *
 * For Zarr V3, attributes are part of the zarr.json file, not a separate
 * .zattrs file.
 *
 * @param store The TensorStore to write to.
 * @param json_var The variable attributes JSON.
 * @param isCloudStore Whether the store is a cloud store.
 * @return Future<tensorstore::TimestampedStorageGeneration> The write result.
 */
template <typename T, DimensionIndex R, ReadWriteMode M>
Future<tensorstore::TimestampedStorageGeneration> WriteVariableAttributes(
    const tensorstore::TensorStore<T, R, M>& store,
    const ::nlohmann::json& json_var, bool isCloudStore) {
  // For Zarr V3, we need to read the existing zarr.json and update attributes
  auto read_future = tensorstore::kvstore::Read(store.kvstore(), "zarr.json");

  auto pair = tensorstore::PromiseFuturePair<
      tensorstore::TimestampedStorageGeneration>::Make();

  read_future.ExecuteWhenReady([promise = std::move(pair.promise), store,
                                json_var](tensorstore::ReadyFuture<
                                          tensorstore::kvstore::ReadResult>
                                              ready_result) {
    nlohmann::json zarr_json;
    if (ready_result.result().ok() && ready_result.value().has_value()) {
      try {
        zarr_json =
            nlohmann::json::parse(std::string(ready_result.value().value));
      } catch (...) {
        zarr_json = nlohmann::json::object();
      }
    }

    // Extract dimension_names before preparing attributes (goes at root level)
    nlohmann::json dimension_names;
    if (json_var.contains("dimension_names")) {
      dimension_names = json_var["dimension_names"];
    }

    // Prepare and update attributes
    auto attrs =
        PrepareVariableAttributes(nlohmann::json{{"attributes", json_var}});
    zarr_json["attributes"] = attrs;

    // Add dimension_names at root level (Zarr V3 spec)
    if (!dimension_names.is_null()) {
      zarr_json["dimension_names"] = dimension_names;
    }

    // Write back
    auto write_result = tensorstore::kvstore::Write(
        store.kvstore(), "zarr.json", absl::Cord(zarr_json.dump(4)));
    write_result.ExecuteWhenReady(
        [promise = std::move(promise)](
            tensorstore::ReadyFuture<tensorstore::TimestampedStorageGeneration>
                write_ready) { promise.SetResult(write_ready.result()); });
  });

  return pair.future;
}

/**
 * @brief Reads variable attributes from zarr.json.
 *
 * @param kvstore The KvStore to read from.
 * @return Future<nlohmann::json> The attributes JSON.
 */
inline Future<nlohmann::json> ReadVariableAttributes(
    const tensorstore::KvStore& kvstore) {
  auto pair = tensorstore::PromiseFuturePair<nlohmann::json>::Make();

  auto read_future = tensorstore::kvstore::Read(kvstore, "zarr.json");
  read_future.ExecuteWhenReady(
      [promise = std::move(pair.promise)](
          tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
              ready_result) {
        if (!ready_result.result().ok()) {
          promise.SetResult(ready_result.result().status());
          return;
        }
        try {
          auto zarr_json =
              nlohmann::json::parse(std::string(ready_result.value().value));
          nlohmann::json attrs;
          if (zarr_json.contains("attributes")) {
            attrs = zarr_json["attributes"];
          } else {
            attrs = nlohmann::json::object();
          }
          // Read dimension_names from root level and add to attrs for MDIO
          if (zarr_json.contains("dimension_names")) {
            attrs["dimension_names"] = zarr_json["dimension_names"];
          }
          promise.SetResult(attrs);
        } catch (const nlohmann::json::parse_error& e) {
          promise.SetResult(absl::InvalidArgumentError(
              std::string("JSON parse error: ") + e.what()));
        }
      });

  return pair.future;
}

/**
 * @brief Helper to convert Zarr V3 metadata to MDIO-compatible format.
 *
 * Converts _ARRAY_DIMENSIONS back to dimension_names for internal MDIO use.
 *
 * @param zarr_json The Zarr V3 zarr.json content.
 * @return nlohmann::json The MDIO-compatible metadata.
 */
inline nlohmann::json ConvertToMdioMetadata(const nlohmann::json& zarr_json) {
  nlohmann::json mdio_metadata;

  if (zarr_json.contains("attributes")) {
    mdio_metadata = zarr_json["attributes"];
  }

  // Convert _ARRAY_DIMENSIONS back to dimension_names for MDIO
  if (mdio_metadata.contains("_ARRAY_DIMENSIONS")) {
    mdio_metadata["dimension_names"] = mdio_metadata["_ARRAY_DIMENSIONS"];
    mdio_metadata.erase("_ARRAY_DIMENSIONS");
  }

  // Also check root level dimension_names (V3 spec)
  if (zarr_json.contains("dimension_names")) {
    mdio_metadata["dimension_names"] = zarr_json["dimension_names"];
  }

  return mdio_metadata;
}

}  // namespace v3
}  // namespace zarr
}  // namespace mdio

#endif  // MDIO_ZARR_ZARR_V3_H_
