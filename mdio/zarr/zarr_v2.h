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

#ifndef MDIO_ZARR_ZARR_V2_H_
#define MDIO_ZARR_ZARR_V2_H_

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "mdio/impl.h"
#include "mdio/zarr/zarr_driver.h"
#include "tensorstore/driver/zarr/dtype.h"
#include "tensorstore/driver/zarr/metadata.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/util/future.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace mdio {
namespace zarr {
namespace v2 {

/**
 * @brief Gets the Zarr V2 driver name.
 * @return constexpr std::string_view The driver name.
 */
constexpr std::string_view kDriverName = "zarr";

/**
 * @brief The Zarr format version for V2.
 */
constexpr int kZarrFormat = 2;

/**
 * @brief The consolidated metadata format version.
 */
constexpr int kConsolidatedFormat = 1;

/**
 * @brief Retrieves the .zarray JSON metadata from the given metadata.
 *
 * This function derives the .zarray JSON metadata without actually reading it.
 *
 * @param metadata The input JSON metadata.
 * @return An `mdio::Result` containing the .zarray JSON metadata on success, or
 * an error on failure.
 */
inline Result<nlohmann::json> GetZarray(const ::nlohmann::json& metadata) {
  auto json = metadata;
  nlohmann::json zarray;
  if (!json.contains("metadata")) {
    json["metadata"] = nlohmann::json::object();
    json["metadata"]["attributes"] = nlohmann::json::object();
  }

  // These fields can have defaults:
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
    zarray["zarr_format"] = kZarrFormat;
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

  MDIO_ASSIGN_OR_RETURN(
      auto zarr_metadata,
      tensorstore::internal_zarr::ZarrMetadata::FromJson(zarray));

  return ::nlohmann::json(zarr_metadata);
}

/**
 * @brief Extracts the field names of a Zarr V2 structured dtype.
 *
 * Zarr V2 represents structured dtypes as a numpy-style list of fields
 * (e.g. [["cdp-x", "<i4"], ["cdp-y", "<i4"]]). Parsing is delegated to
 * TensorStore so all numpy dtype variants are handled consistently.
 *
 * @param dtype The Zarr V2 "dtype" metadata value.
 * @return Field names in declaration order, or empty if not structured.
 */
inline std::vector<std::string> GetStructFieldNames(
    const nlohmann::json& dtype) {
  std::vector<std::string> names;
  auto parsed = tensorstore::internal_zarr::ParseDType(dtype);
  if (parsed.ok() && parsed.value().has_fields) {
    for (const auto& field : parsed.value().fields) {
      names.push_back(field.name);
    }
  }
  return names;
}

/**
 * @brief Creates the .zgroup JSON for Zarr V2.
 * @return nlohmann::json The .zgroup content.
 */
inline nlohmann::json CreateZgroup() {
  nlohmann::json zgroup;
  zgroup["zarr_format"] = kZarrFormat;
  return zgroup;
}

/**
 * @brief Prepares variable attributes JSON for .zattrs file.
 *
 * @param json The variable JSON specification.
 * @return nlohmann::json The prepared attributes.
 */
inline nlohmann::json PrepareVariableAttributes(const nlohmann::json& json) {
  nlohmann::json fixedJson = json["attributes"];
  fixedJson["_ARRAY_DIMENSIONS"] = fixedJson["dimension_names"];
  fixedJson.erase("dimension_names");

  // We do not want to be serializing the variable_name
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
  return fixedJson;
}

/**
 * @brief Writes the consolidated metadata (.zmetadata) for a dataset.
 *
 * @param dataset_metadata The metadata for the dataset.
 * @param json_variables The JSON variables.
 * @param context Optional TensorStore context for credentials/configuration.
 * @return An `mdio::Future<void>` representing the asynchronous write.
 */
inline Future<void> WriteConsolidatedMetadata(
    const ::nlohmann::json& dataset_metadata,
    const std::vector<::nlohmann::json>& json_variables,
    tensorstore::Context context = tensorstore::Context::Default()) {
  auto zattrs = dataset_metadata;
  ::nlohmann::json zgroup = CreateZgroup();

  // The consolidated metadata for the dataset
  ::nlohmann::json zmetadata;
  zmetadata["zarr_consolidated_format"] = kConsolidatedFormat;
  zmetadata["metadata"][".zattrs"] = zattrs;
  zmetadata["metadata"][".zgroup"] = zgroup;

  std::string driver =
      json_variables[0]["kvstore"]["driver"].get<std::string>();

  for (const auto& json : json_variables) {
    // Extract variable name from path (last component)
    std::string path = json["kvstore"]["path"].get<std::string>();
    std::vector<std::string> path_parts = absl::StrSplit(path, '/');
    std::string var_name = path_parts.back();

    std::string zarray_key = var_name + "/.zarray";
    std::string zattrs_key = var_name + "/.zattrs";

    MDIO_ASSIGN_OR_RETURN(zmetadata["metadata"][zarray_key], GetZarray(json));
    zmetadata["metadata"][zattrs_key] = PrepareVariableAttributes(json);
  }

  // Extract base path by removing the last component (variable name)
  std::vector<std::string> file_parts = absl::StrSplit(
      json_variables[0]["kvstore"]["path"].get<std::string>(), '/');
  size_t toRemove = file_parts.back().size();
  std::string basePath =
      json_variables[0]["kvstore"]["path"].get<std::string>().substr(
          0, json_variables[0]["kvstore"]["path"].get<std::string>().size() -
                 toRemove - 1);

  nlohmann::json kvstore = nlohmann::json::object();
  kvstore["driver"] = driver;
  kvstore["path"] = basePath;

  if (driver == "gcs" || driver == "s3") {
    kvstore["bucket"] =
        json_variables[0]["kvstore"]["bucket"].get<std::string>();
  }

  auto kvs_future = tensorstore::kvstore::Open(kvstore, context);

  auto zattrs_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zattrs = std::move(zattrs)](const tensorstore::KvStore& kvs) {
        return tensorstore::kvstore::Write(kvs, "/.zattrs",
                                           absl::Cord(zattrs.dump(4)));
      },
      kvs_future);

  auto zmetadata_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zmetadata = std::move(zmetadata)](const tensorstore::KvStore& kvs) {
        return tensorstore::kvstore::Write(kvs, "/.zmetadata",
                                           absl::Cord(zmetadata.dump(4)));
      },
      kvs_future);

  auto zgroup_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{},
      [zgroup = std::move(zgroup)](const tensorstore::KvStore& kvs) {
        return tensorstore::kvstore::Write(kvs, "/.zgroup",
                                           absl::Cord(zgroup.dump(4)));
      },
      kvs_future);

  return tensorstore::WaitAllFuture(zattrs_future, zmetadata_future,
                                    zgroup_future);
}

// ============================================================================
// Async Metadata Discovery for V2
// ============================================================================

namespace internal {

/**
 * @brief State for V2 consolidated metadata reading operations.
 *
 * This struct holds all state needed across the async callback chain.
 */
struct V2MetadataState {
  using ResultType = std::tuple<nlohmann::json, std::vector<nlohmann::json>>;
  using PromiseType = tensorstore::Promise<ResultType>;

  PromiseType promise;
  tensorstore::KvStore kvs;
  std::string dataset_path;
  std::string normalized_path;
  std::string driver;
  std::string bucket;
  std::string cloud_path;

  explicit V2MetadataState(PromiseType p, const std::string& path)
      : promise(std::move(p)),
        dataset_path(path),
        normalized_path(NormalizePathWithSlash(path)),
        driver(InferDriverFromPath(path)) {
    if (driver != "file") {
      auto cloud_parts = ExtractCloudPath(path);
      bucket = cloud_parts.first;
      cloud_path = NormalizePathWithSlash(cloud_parts.second);
    }
  }

  /// Completes with an error status.
  void Fail(absl::Status status) { promise.SetResult(std::move(status)); }

  /// Completes successfully.
  void Complete(nlohmann::json metadata, std::vector<nlohmann::json> vars) {
    promise.SetResult(std::make_tuple(std::move(metadata), std::move(vars)));
  }

  /// Builds a variable spec for the given variable name.
  nlohmann::json BuildVariableSpec(const std::string& var_name) const {
    nlohmann::json spec = {{"driver", std::string(kDriverName)},
                           {"kvstore", {{"driver", driver}}}};
    if (driver != "file") {
      spec["kvstore"]["bucket"] = bucket;
      spec["kvstore"]["path"] = cloud_path + var_name;
    } else {
      spec["kvstore"]["path"] = normalized_path + var_name;
    }
    return spec;
  }
};

/// Step 2: Parse .zmetadata and build variable specs.
inline void OnZmetadataRead(
    std::shared_ptr<V2MetadataState> state,
    tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult> read_ready) {
  if (!read_ready.result().ok()) {
    state->Fail(read_ready.result().status());
    return;
  }

  auto parsed = ParseJsonFromReadResult(read_ready.value());
  if (!parsed.ok()) {
    if (!state->dataset_path.empty() && state->dataset_path.back() != '/') {
      state->Fail(absl::InvalidArgumentError(
          "Failed to parse .zmetadata. Try adding a trailing slash to the "
          "path."));
    } else {
      state->Fail(parsed.status());
    }
    return;
  }

  const auto& zmetadata = parsed.value();

  if (!zmetadata.contains("metadata")) {
    state->Fail(
        absl::InvalidArgumentError("zmetadata does not contain metadata."));
    return;
  }

  if (!zmetadata["metadata"].contains(".zattrs")) {
    state->Fail(absl::InvalidArgumentError(
        "zmetadata does not contain dataset metadata."));
    return;
  }

  auto dataset_metadata = zmetadata["metadata"][".zattrs"];
  std::vector<nlohmann::json> json_vars;

  for (const auto& element : zmetadata["metadata"].items()) {
    const std::string& key = element.key();
    // Skip if not a .zarray entry
    size_t dot_pos = key.find_last_of('.');
    if (dot_pos == std::string::npos || key.substr(dot_pos + 1) != "zarray") {
      continue;
    }
    std::string var_name = ExtractVariableName(key);
    json_vars.push_back(state->BuildVariableSpec(var_name));
  }

  if (json_vars.empty()) {
    state->Fail(absl::InvalidArgumentError("No variables found in zmetadata."));
    return;
  }

  state->Complete(std::move(dataset_metadata), std::move(json_vars));
}

/// Step 1: Start reading .zmetadata.
inline void OnV2KvStoreReady(
    std::shared_ptr<V2MetadataState> state,
    tensorstore::ReadyFuture<tensorstore::KvStore> kvs_ready) {
  if (!kvs_ready.result().ok()) {
    state->Fail(kvs_ready.result().status());
    return;
  }

  state->kvs = kvs_ready.value();
  auto read_future = tensorstore::kvstore::Read(state->kvs, ".zmetadata");
  read_future.ExecuteWhenReady(
      [state](
          tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult> ready) {
        OnZmetadataRead(state, std::move(ready));
      });
}

}  // namespace internal

/**
 * @brief Reads and parses the consolidated metadata from a dataset path.
 *
 * @param dataset_path The path to the dataset.
 * @param kvs_future A future to the KvStore.
 * @return A future containing dataset metadata and variable JSON specs.
 */
inline Future<std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>
ReadConsolidatedMetadata(const std::string& dataset_path,
                         tensorstore::Future<tensorstore::KvStore> kvs_future) {
  auto pair = tensorstore::PromiseFuturePair<
      std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>::Make();

  auto state = std::make_shared<internal::V2MetadataState>(
      std::move(pair.promise), dataset_path);

  kvs_future.ExecuteWhenReady(
      [state](tensorstore::ReadyFuture<tensorstore::KvStore> ready) {
        internal::OnV2KvStoreReady(state, std::move(ready));
      });

  return pair.future;
}

/**
 * @brief Creates a Variable spec for Zarr V2.
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
       {{"dtype", "DATA_TYPE"},
        {"dimension_separator", "/"},
        {"shape", "SHAPE"},
        {"chunks", "CHUNKS"}}},
      {"attributes", nlohmann::json::object()}};
  return spec;
}

/**
 * @brief Writes variable attributes to .zattrs file.
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
  auto output_json = json_var;
  output_json["_ARRAY_DIMENSIONS"] = output_json["dimension_names"];
  output_json.erase("dimension_names");
  output_json.erase("variable_name");

  if (output_json.contains("metadata")) {
    output_json["metadata"].erase("chunkGrid");
    for (auto& item : output_json["metadata"].items()) {
      output_json[item.key()] = std::move(item.value());
    }
    output_json.erase("metadata");
  }
  if (output_json.contains("long_name")) {
    if (output_json["long_name"] == "") {
      output_json.erase("long_name");
    }
  }
  // Case where empty array of coordinates is provided
  if (output_json.contains("coordinates")) {
    auto coords = output_json["coordinates"];
    if (coords.empty() ||
        (coords.is_string() && coords.get<std::string>() == "")) {
      output_json.erase("coordinates");
    }
  }

  std::string outpath = ".zattrs";
  return tensorstore::kvstore::Write(store.kvstore(), outpath,
                                     absl::Cord(output_json.dump(4)));
}

/**
 * @brief Reads variable attributes from .zattrs file.
 *
 * @param kvstore The KvStore to read from.
 * @return Future<nlohmann::json> The attributes JSON.
 */
inline Future<nlohmann::json> ReadVariableAttributes(
    const tensorstore::KvStore& kvstore) {
  auto pair = tensorstore::PromiseFuturePair<nlohmann::json>::Make();

  auto read_future = tensorstore::kvstore::Read(kvstore, "/.zattrs");
  read_future.ExecuteWhenReady(
      [promise = std::move(pair.promise)](
          tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
              ready_result) {
        if (!ready_result.result().ok()) {
          promise.SetResult(ready_result.result().status());
          return;
        }
        auto attributes = nlohmann::json::parse(
            std::string(ready_result.value().value), nullptr, false);
        promise.SetResult(attributes);
      });

  return pair.future;
}

/**
 * @brief Converts a dtype string to Zarr V2 numpy-style dtype.
 * @param dtype The input dtype string.
 * @return Result<std::string> The Zarr V2 dtype string.
 */
inline Result<std::string> ToZarrDtype(const std::string& dtype) {
  if (dtype == "int8") return "<i1";
  if (dtype == "int16") return "<i2";
  if (dtype == "int32") return "<i4";
  if (dtype == "int64") return "<i8";
  if (dtype == "uint8") return "<u1";
  if (dtype == "uint16") return "<u2";
  if (dtype == "uint32") return "<u4";
  if (dtype == "uint64") return "<u8";
  if (dtype == "float16") return "<f2";
  if (dtype == "float32") return "<f4";
  if (dtype == "float64") return "<f8";
  if (dtype == "bool") return "|b1";
  if (dtype == "complex64") return "<c8";
  if (dtype == "complex128") return "<c16";
  return absl::InvalidArgumentError("Unknown dtype: " + dtype);
}

}  // namespace v2
}  // namespace zarr
}  // namespace mdio

#endif  // MDIO_ZARR_ZARR_V2_H_
