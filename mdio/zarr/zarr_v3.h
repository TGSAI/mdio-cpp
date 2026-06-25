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
#include <unordered_set>
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

// ============================================================================
// V3-Specific JSON Utilities
// ============================================================================

/**
 * @brief Checks if a JSON object represents a Zarr V3 array.
 * @param json The zarr.json content.
 * @return True if node_type is "array".
 */
inline bool IsArrayMetadata(const nlohmann::json& json) {
  return GetJsonString(json, "node_type") == std::string(kArrayNodeType);
}

/**
 * @brief The Zarr V3 data_type name used for structured (record) dtypes.
 */
constexpr std::string_view kStructDataTypeName = "struct";

/**
 * @brief Extracts the field names of a Zarr V3 structured data_type.
 *
 * Zarr V3 represents structured dtypes as an object:
 * @code
 * {
 *   "name": "struct",
 *   "configuration": {
 *     "fields": [
 *       {"name": "cdp-x", "data_type": "int32"},
 *       {"name": "cdp-y", "data_type": "int32"}
 *     ]
 *   }
 * }
 * @endcode
 *
 * For backwards compatibility this also accepts the legacy array-of-pairs
 * layout (e.g. [["cdp-x", "int32"], ["cdp-y", "int32"]]).
 *
 * @param data_type The Zarr V3 "data_type" metadata value.
 * @return Field names in declaration order, or empty if not structured.
 */
inline std::vector<std::string> GetStructFieldNames(
    const nlohmann::json& data_type) {
  std::vector<std::string> names;

  // Current V3 layout: object with name == "struct".
  if (data_type.is_object() &&
      GetJsonString(data_type, "name") == std::string(kStructDataTypeName) &&
      data_type.contains("configuration") &&
      data_type["configuration"].contains("fields") &&
      data_type["configuration"]["fields"].is_array()) {
    for (const auto& field : data_type["configuration"]["fields"]) {
      if (field.is_object() && field.contains("name")) {
        names.push_back(field["name"].get<std::string>());
      }
    }
    return names;
  }

  // Legacy layout: array of [name, type] pairs.
  if (data_type.is_array() && !data_type.empty() && data_type[0].is_array()) {
    for (const auto& field : data_type) {
      if (field.is_array() && !field.empty()) {
        names.push_back(field[0].get<std::string>());
      }
    }
  }

  return names;
}

/**
 * @brief Extracts the name of a Zarr V3 data_type value.
 *
 * A V3 data_type is either a bare string (e.g. "int32") or an object carrying a
 * "name" field (e.g. {"name": "fixed_length_utf32", "configuration": {...}}).
 *
 * @param data_type The Zarr V3 "data_type" metadata value.
 * @return The data_type name, or an empty string if it cannot be determined.
 */
inline std::string GetDataTypeName(const nlohmann::json& data_type) {
  if (data_type.is_string()) {
    return data_type.get<std::string>();
  }
  if (data_type.is_object() && data_type.contains("name") &&
      data_type["name"].is_string()) {
    return data_type["name"].get<std::string>();
  }
  return "";
}

/**
 * @brief Reports whether a Zarr V3 data_type is a metadata-only dtype.
 *
 * Python `mdio` stores some variables using zarr v3 string/bytes/datetime
 * extension dtypes. The canonical example is the SEG-Y file header
 * (`segy_file_header`): a scalar `fixed_length_utf32` array whose real content
 * (`textHeader`, `binaryHeader`) lives entirely in the array `attributes`.
 * TensorStore's zarr3 driver only supports numeric dtypes (and the `struct`
 * record dtype), so from C++'s perspective these arrays carry no openable data
 * and are treated as metadata-only.
 *
 * @param data_type The Zarr V3 "data_type" metadata value.
 * @return True if the dtype cannot be opened as a tensorstore array.
 */
inline bool IsMetadataOnlyDataType(const nlohmann::json& data_type) {
  static const std::unordered_set<std::string> kMetadataOnlyNames = {
      "fixed_length_utf32",    "fixed_length_ascii", "string",
      "variable_length_utf8",  "raw_bytes",          "null_terminated_bytes",
      "variable_length_bytes", "numpy.datetime64",   "numpy.timedelta64"};
  return kMetadataOnlyNames.count(GetDataTypeName(data_type)) > 0;
}

// ============================================================================
// Variable Discovery Utilities
// ============================================================================

/**
 * @brief Extracts child array names from kvstore list entries.
 *
 * Looks for entries matching pattern "NAME/zarr.json" and returns unique NAMEs.
 *
 * @param entries The list entries from kvstore.
 * @return Vector of unique child directory names.
 */
inline std::vector<std::string> ExtractChildArrayCandidates(
    const std::vector<tensorstore::kvstore::ListEntry>& entries) {
  std::vector<std::string> candidates;
  for (const auto& entry : entries) {
    const std::string& key = entry.key;
    size_t slash_pos = key.find('/');
    if (slash_pos != std::string::npos &&
        key.substr(slash_pos + 1) == "zarr.json") {
      std::string name = key.substr(0, slash_pos);
      if (std::find(candidates.begin(), candidates.end(), name) ==
          candidates.end()) {
        candidates.push_back(name);
      }
    }
  }
  return candidates;
}

/**
 * @brief Builds a variable spec for a Zarr V3 array.
 * @param driver The kvstore driver name.
 * @param base_path The base dataset path.
 * @param var_name The variable/array name.
 * @return The JSON spec for opening this variable.
 */
inline nlohmann::json BuildVariableSpec(const std::string& driver,
                                        const std::string& base_path,
                                        const std::string& var_name) {
  return {
      {"driver", std::string(kDriverName)},
      {"kvstore", {{"driver", driver}, {"path", base_path + "/" + var_name}}}};
}

// ============================================================================
// Dtype Conversion
// ============================================================================

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

inline tensorstore::Result<nlohmann::json> BuildHeaderOnlyArrayMetadata(
    const nlohmann::json& json) {
  if (!json.contains("_mdio_array_metadata")) {
    return absl::InvalidArgumentError(
        "V3 header-only variable is missing _mdio_array_metadata.");
  }

  nlohmann::json array_metadata = json["_mdio_array_metadata"];
  if (json.contains("attributes")) {
    array_metadata["attributes"] = PrepareVariableAttributes(json);
  }
  return array_metadata;
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

  // TensorStore writes numeric arrays, but metadata-only arrays are never opened
  // through TensorStore. Persist their child zarr.json files explicitly.
  for (const auto& json : json_variables) {
    if (!json.contains("_mdio_header_only") ||
        !json["_mdio_header_only"].get<bool>()) {
      continue;
    }

    auto array_metadata = BuildHeaderOnlyArrayMetadata(json);
    if (!array_metadata.ok()) {
      return array_metadata.status();
    }

    std::string path = json["kvstore"]["path"].get<std::string>();
    std::vector<std::string> path_parts = absl::StrSplit(path, '/');
    std::string var_name = path_parts.back();
    std::string zarr_json_key = var_name + "/zarr.json";

    auto header_future = tensorstore::MapFutureValue(
        tensorstore::InlineExecutor{},
        [array_metadata = array_metadata.value(),
         zarr_json_key](const tensorstore::KvStore& kvs) {
          return tensorstore::kvstore::Write(
              kvs, zarr_json_key, absl::Cord(array_metadata.dump(4)));
        },
        kvs_future);
    write_futures.push_back(header_future);
  }

  return tensorstore::WaitAllFuture(write_futures);
}

// ============================================================================
// Async Metadata Discovery
// ============================================================================

namespace internal {

/**
 * @brief State for V3 async metadata discovery operations.
 *
 * This struct holds all state needed across the async callback chain,
 * avoiding deeply nested lambdas and making the flow clearer.
 */
struct V3MetadataState {
  using ResultType = std::tuple<nlohmann::json, std::vector<nlohmann::json>>;
  using PromiseType = tensorstore::Promise<ResultType>;
  using ReadFuture = tensorstore::Future<tensorstore::kvstore::ReadResult>;

  PromiseType promise;
  tensorstore::KvStore kvs;
  nlohmann::json dataset_metadata;
  std::string dataset_path;
  std::string driver;
  std::string normalized_path;
  std::vector<std::string> candidates;
  std::shared_ptr<std::vector<ReadFuture>> read_futures;

  explicit V3MetadataState(PromiseType p, const std::string& path)
      : promise(std::move(p)),
        dataset_path(path),
        driver(InferDriverFromPath(path)),
        normalized_path(NormalizePath(path)) {}

  /// Completes with an error status.
  void Fail(absl::Status status) { promise.SetResult(std::move(status)); }

  /// Completes successfully with the discovered metadata and variables.
  void Complete(std::vector<nlohmann::json> json_vars) {
    promise.SetResult(std::make_tuple(dataset_metadata, std::move(json_vars)));
  }

  /// Builds a variable spec for the given variable name.
  nlohmann::json MakeVariableSpec(const std::string& var_name) const {
    return BuildVariableSpec(driver, normalized_path, var_name);
  }

  /// Filters read results to build variable specs for arrays only.
  std::vector<nlohmann::json> BuildVariableSpecs() const {
    std::vector<nlohmann::json> json_vars;
    for (size_t i = 0; i < candidates.size(); ++i) {
      const auto& result = (*read_futures)[i].result();
      if (!result.ok() || !result->has_value()) continue;

      auto parsed = ParseJsonFromReadResult(*result);
      if (parsed.ok() && IsArrayMetadata(parsed.value())) {
        if (parsed.value().contains("data_type") &&
            IsMetadataOnlyDataType(parsed.value()["data_type"])) {
          auto spec = MakeVariableSpec(candidates[i]);
          spec["_mdio_header_only"] = true;
          spec["_mdio_array_metadata"] = parsed.value();
          json_vars.push_back(std::move(spec));
          continue;
        }
        json_vars.push_back(MakeVariableSpec(candidates[i]));
      }
    }
    return json_vars;
  }
};

/// Step 4: Process read results and complete.
inline void OnV3ChildReadsComplete(std::shared_ptr<V3MetadataState> state,
                                   tensorstore::ReadyFuture<void>) {
  auto json_vars = state->BuildVariableSpecs();
  if (json_vars.empty()) {
    state->Fail(absl::InvalidArgumentError("No Zarr V3 arrays found."));
    return;
  }
  state->Complete(std::move(json_vars));
}

/// Step 3: Read all child zarr.json files and filter by node_type.
inline void OnV3ListComplete(
    std::shared_ptr<V3MetadataState> state,
    tensorstore::ReadyFuture<std::vector<tensorstore::kvstore::ListEntry>>
        list_ready) {
  if (!list_ready.result().ok()) {
    state->Fail(list_ready.result().status());
    return;
  }

  state->candidates = ExtractChildArrayCandidates(list_ready.value());
  if (state->candidates.empty()) {
    state->Fail(absl::InvalidArgumentError("No Zarr V3 arrays found."));
    return;
  }

  // Read all child zarr.json files
  state->read_futures =
      std::make_shared<std::vector<V3MetadataState::ReadFuture>>();
  std::vector<tensorstore::AnyFuture> any_futures;

  for (const auto& name : state->candidates) {
    auto f = tensorstore::kvstore::Read(state->kvs, name + "/zarr.json");
    state->read_futures->push_back(f);
    any_futures.push_back(std::move(f));
  }

  auto all_reads = tensorstore::WaitAllFuture(any_futures);
  all_reads.ExecuteWhenReady([state](tensorstore::ReadyFuture<void> ready) {
    OnV3ChildReadsComplete(state, std::move(ready));
  });
}

/// Step 2: Parse root metadata and list kvstore contents.
inline void OnV3RootReadComplete(
    std::shared_ptr<V3MetadataState> state,
    tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult> root_ready) {
  if (!root_ready.result().ok()) {
    state->Fail(root_ready.result().status());
    return;
  }

  auto root_json = ParseJsonFromReadResult(root_ready.value());
  if (!root_json.ok()) {
    state->Fail(root_json.status());
    return;
  }

  state->dataset_metadata = GetJsonObject(root_json.value(), "attributes");

  auto list_future = tensorstore::kvstore::ListFuture(state->kvs);
  list_future.ExecuteWhenReady(
      [state](
          tensorstore::ReadyFuture<std::vector<tensorstore::kvstore::ListEntry>>
              ready) { OnV3ListComplete(state, std::move(ready)); });
}

/// Step 1: Start reading root zarr.json.
inline void OnV3KvStoreReady(
    std::shared_ptr<V3MetadataState> state,
    tensorstore::ReadyFuture<tensorstore::KvStore> kvs_ready) {
  if (!kvs_ready.result().ok()) {
    state->Fail(kvs_ready.result().status());
    return;
  }

  state->kvs = kvs_ready.value();
  auto root_read = tensorstore::kvstore::Read(state->kvs, "zarr.json");
  root_read.ExecuteWhenReady(
      [state](
          tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult> ready) {
        OnV3RootReadComplete(state, std::move(ready));
      });
}

}  // namespace internal

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

  auto state = std::make_shared<internal::V3MetadataState>(
      std::move(pair.promise), dataset_path);

  kvs_future.ExecuteWhenReady(
      [state](tensorstore::ReadyFuture<tensorstore::KvStore> ready) {
        internal::OnV3KvStoreReady(state, std::move(ready));
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
