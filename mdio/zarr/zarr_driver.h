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

#ifndef MDIO_ZARR_ZARR_DRIVER_H_
#define MDIO_ZARR_ZARR_DRIVER_H_

#include <string>
#include <utility>
#include <vector>

#include "mdio/impl.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/util/future.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace mdio {
namespace zarr {

/**
 * @brief Enumeration representing supported Zarr format versions.
 */
enum class ZarrVersion {
  kV2 = 2,  ///< Zarr V2 format
  kV3 = 3   ///< Zarr V3 format
};

/**
 * @brief Returns the default Zarr version to use.
 * @return ZarrVersion The default version (V2 for backward compatibility).
 */
inline ZarrVersion GetDefaultVersion() { return ZarrVersion::kV2; }

/**
 * @brief Returns the TensorStore driver name for the given Zarr version.
 * @param version The Zarr version.
 * @return std::string The driver name ("zarr" for V2, "zarr3" for V3).
 */
inline std::string GetDriverName(ZarrVersion version) {
  switch (version) {
    case ZarrVersion::kV3:
      return "zarr3";
    case ZarrVersion::kV2:
    default:
      return "zarr";
  }
}

/**
 * @brief Parses a Zarr version from a string or integer.
 * @param version_spec JSON value that may be an integer (2 or 3) or string
 * ("2", "3", "v2", "v3").
 * @return Result<ZarrVersion> The parsed version or an error.
 */
inline Result<ZarrVersion> ParseVersion(const nlohmann::json& version_spec) {
  if (version_spec.is_number_integer()) {
    int ver = version_spec.get<int>();
    if (ver == 2) return ZarrVersion::kV2;
    if (ver == 3) return ZarrVersion::kV3;
    return absl::InvalidArgumentError(
        "Invalid zarr version: " + std::to_string(ver) +
        ". Supported versions are 2 and 3.");
  }
  if (version_spec.is_string()) {
    std::string ver = version_spec.get<std::string>();
    if (ver == "2" || ver == "v2" || ver == "V2") return ZarrVersion::kV2;
    if (ver == "3" || ver == "v3" || ver == "V3") return ZarrVersion::kV3;
    return absl::InvalidArgumentError(
        "Invalid zarr version string: '" + ver +
        "'. Supported versions are '2', '3', 'v2', 'v3'.");
  }
  return absl::InvalidArgumentError(
      "Invalid zarr version specification. Expected integer or string.");
}

/**
 * @brief Detects the Zarr version from an existing store by examining metadata
 * files.
 * @param kvstore The KvStore to examine.
 * @return Future<ZarrVersion> The detected version or an error if unable to
 * determine.
 */
inline Future<ZarrVersion> DetectVersion(const tensorstore::KvStore& kvstore) {
  // Check for zarr.json first (V3 indicator)
  auto v3_check = tensorstore::kvstore::Read(kvstore, "zarr.json");

  auto pair = tensorstore::PromiseFuturePair<ZarrVersion>::Make();

  v3_check.ExecuteWhenReady(
      [promise = pair.promise,
       kvstore](tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
                    v3_result) mutable {
        if (v3_result.result().ok() && v3_result.value().has_value()) {
          promise.SetResult(ZarrVersion::kV3);
          return;
        }

        // Check for .zgroup or .zmetadata (V2 indicators)
        auto v2_check = tensorstore::kvstore::Read(kvstore, ".zgroup");
        v2_check.ExecuteWhenReady(
            [promise = std::move(promise)](
                tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
                    v2_result) mutable {
              if (v2_result.result().ok() && v2_result.value().has_value()) {
                promise.SetResult(ZarrVersion::kV2);
              } else {
                // Default to V2 if we can't determine
                promise.SetResult(ZarrVersion::kV2);
              }
            });
      });

  return pair.future;
}

/**
 * @brief Base configuration for Zarr operations.
 */
struct ZarrConfig {
  /// The Zarr format version to use.
  ZarrVersion version = ZarrVersion::kV2;

  /// Whether to use consolidated metadata (only applicable for V2).
  bool use_consolidated_metadata = true;

  /// The dimension separator to use.
  std::string dimension_separator = "/";
};

/**
 * @brief Checks if consolidated metadata is supported for the given version.
 * @param version The Zarr version.
 * @return bool True if consolidated metadata is supported.
 */
inline bool SupportsConsolidatedMetadata(ZarrVersion version) {
  // Only Zarr V2 supports consolidated metadata
  return version == ZarrVersion::kV2;
}

/**
 * @brief Gets the metadata file name for the given Zarr version.
 * @param version The Zarr version.
 * @return std::string The metadata file name.
 */
inline std::string GetGroupMetadataFileName(ZarrVersion version) {
  switch (version) {
    case ZarrVersion::kV3:
      return "zarr.json";
    case ZarrVersion::kV2:
    default:
      return ".zgroup";
  }
}

/**
 * @brief Gets the array metadata file name for the given Zarr version.
 * @param version The Zarr version.
 * @param array_name The name of the array (used in path construction).
 * @return std::string The array metadata file path.
 */
inline std::string GetArrayMetadataFileName(ZarrVersion version,
                                            const std::string& array_name) {
  switch (version) {
    case ZarrVersion::kV3:
      return array_name + "/zarr.json";
    case ZarrVersion::kV2:
    default:
      return array_name + "/.zarray";
  }
}

/**
 * @brief Gets the attributes file name for the given Zarr version.
 * @param version The Zarr version.
 * @param array_name The name of the array (empty for root attributes).
 * @return std::string The attributes file path.
 */
inline std::string GetAttributesFileName(ZarrVersion version,
                                         const std::string& array_name = "") {
  switch (version) {
    case ZarrVersion::kV3:
      // In V3, attributes are embedded in zarr.json
      if (array_name.empty()) {
        return "zarr.json";
      }
      return array_name + "/zarr.json";
    case ZarrVersion::kV2:
    default:
      if (array_name.empty()) {
        return ".zattrs";
      }
      return array_name + "/.zattrs";
  }
}

/**
 * @brief Gets the consolidated metadata file name (V2 only).
 * @return std::string The consolidated metadata file name.
 */
inline std::string GetConsolidatedMetadataFileName() { return ".zmetadata"; }

// ============================================================================
// Shared Path and Driver Utilities
// ============================================================================

/**
 * @brief Infers the kvstore driver from a path prefix.
 * @param path The dataset path.
 * @return The driver name ("file", "gcs", or "s3").
 */
inline std::string InferDriverFromPath(const std::string& path) {
  if (path.compare(0, 5, "gs://") == 0) return "gcs";
  if (path.compare(0, 5, "s3://") == 0) return "s3";
  return "file";
}

/**
 * @brief Normalizes a path by removing trailing slashes.
 * @param path The input path.
 * @return The normalized path without trailing slashes.
 */
inline std::string NormalizePath(const std::string& path) {
  std::string result = path;
  while (!result.empty() && result.back() == '/') {
    result.pop_back();
  }
  return result;
}

/**
 * @brief Normalizes a path so it ends in exactly one trailing slash.
 *
 * Repeated trailing slashes are collapsed. Cloud object keys are not path
 * normalized by the store, so "a//var" and "a/var" are distinct objects.
 *
 * @param path The input path.
 * @return The normalized path with a single trailing slash.
 */
inline std::string NormalizePathWithSlash(const std::string& path) {
  std::string result = NormalizePath(path);
  if (result.empty()) {
    // Preserve an empty path, and collapse a root-only path back to "/".
    return path.empty() ? path : "/";
  }
  result.push_back('/');
  return result;
}

/**
 * @brief Extracts bucket and path from a cloud URL.
 * @param url The full cloud URL (gs://bucket/path or s3://bucket/path).
 * @return A pair of (bucket, path).
 */
inline std::pair<std::string, std::string> ExtractCloudPath(
    const std::string& url) {
  if (url.length() <= 5) return {"", ""};
  std::string without_scheme = url.substr(5);
  size_t bucket_end = without_scheme.find('/');
  if (bucket_end == std::string::npos) {
    return {without_scheme, ""};
  }
  return {without_scheme.substr(0, bucket_end),
          without_scheme.substr(bucket_end + 1)};
}

/**
 * @brief Resolved TensorStore kvstore location for a dataset path.
 *
 * Cloud URLs (`gs://`, `s3://`) are split into `bucket` plus a bucket-relative
 * `path`. File paths keep the original path. `path` has a trailing slash when
 * non-empty so a child name can be appended.
 */
struct KvStoreLocation {
  std::string driver;
  std::string bucket;
  std::string path;
};

/**
 * @brief Splits a dataset path into TensorStore kvstore fields.
 *
 * This is the single place that maps a user path onto `driver` / `bucket` /
 * `path`.
 *
 * @param dataset_path Local path or `gs://` / `s3://` URI.
 * @return The resolved location, or InvalidArgument for a missing cloud bucket.
 */
inline Result<KvStoreLocation> ResolveKvStoreLocation(
    const std::string& dataset_path) {
  std::string driver = InferDriverFromPath(dataset_path);
  if (driver == "file") {
    return KvStoreLocation{driver, "", NormalizePathWithSlash(dataset_path)};
  }

  auto [bucket, path] = ExtractCloudPath(dataset_path);
  if (bucket.empty()) {
    return absl::InvalidArgumentError(
        "Cloud path requires [gs/s3]://[bucket]/[path to file]");
  }
  return KvStoreLocation{driver, bucket, NormalizePathWithSlash(path)};
}

/**
 * @brief Builds a TensorStore kvstore JSON spec from a resolved location.
 *
 * For non-file drivers, `bucket` is always set. `child` is appended to `path`
 * (typically a variable / array name).
 *
 * @param loc The resolved dataset location.
 * @param child Optional child key appended to `path`.
 * @return The kvstore JSON object.
 */
inline nlohmann::json BuildKvStoreSpec(const KvStoreLocation& loc,
                                       const std::string& child = "") {
  nlohmann::json kvstore = {{"driver", loc.driver}, {"path", loc.path + child}};
  if (loc.driver != "file") {
    kvstore["bucket"] = loc.bucket;
  }
  return kvstore;
}

/**
 * @brief Builds a TensorStore Variable open spec (zarr/zarr3 + kvstore).
 * @param zarr_driver TensorStore array driver (`zarr` or `zarr3`).
 * @param loc The resolved dataset location.
 * @param var_name Variable / array name appended to the kvstore path.
 * @return The Variable spec JSON object.
 */
inline nlohmann::json BuildVariableSpec(const std::string& zarr_driver,
                                        const KvStoreLocation& loc,
                                        const std::string& var_name) {
  return {{"driver", zarr_driver},
          {"kvstore", BuildKvStoreSpec(loc, var_name)}};
}

// ============================================================================
// Shared JSON Utilities
// ============================================================================

/**
 * @brief Safely parses JSON from a kvstore read result.
 * @param read_result The read result containing raw bytes.
 * @return Result containing parsed JSON or an error.
 */
inline Result<nlohmann::json> ParseJsonFromReadResult(
    const tensorstore::kvstore::ReadResult& read_result) {
  if (!read_result.has_value()) {
    return absl::NotFoundError("Read result has no value");
  }
  try {
    return nlohmann::json::parse(std::string(read_result.value));
  } catch (const nlohmann::json::parse_error& e) {
    return absl::InvalidArgumentError(std::string("JSON parse error: ") +
                                      e.what());
  }
}

/**
 * @brief Safely gets a string value from JSON.
 * @param json The JSON object.
 * @param key The key to look up.
 * @param default_value The default if key doesn't exist.
 * @return The string value or default.
 */
inline std::string GetJsonString(const nlohmann::json& json,
                                 const std::string& key,
                                 const std::string& default_value = "") {
  if (json.contains(key) && json[key].is_string()) {
    return json[key].get<std::string>();
  }
  return default_value;
}

/**
 * @brief Safely gets a JSON object value.
 * @param json The JSON object.
 * @param key The key to look up.
 * @return The object value or empty object.
 */
inline nlohmann::json GetJsonObject(const nlohmann::json& json,
                                    const std::string& key) {
  if (json.contains(key) && json[key].is_object()) {
    return json[key];
  }
  return nlohmann::json::object();
}

/**
 * @brief Safely gets a JSON value with a default fallback.
 * @param json The JSON object.
 * @param key The key to look up.
 * @param default_value The default if key doesn't exist.
 * @return The value at key, or default_value.
 */
inline nlohmann::json GetJsonValue(const nlohmann::json& json,
                                   const std::string& key,
                                   nlohmann::json default_value) {
  if (json.contains(key)) {
    return json[key];
  }
  return default_value;
}

/**
 * @brief Extracts the variable name from a key path (e.g., "varname/.zarray").
 * @param key The full key path.
 * @return The variable name portion.
 */
inline std::string ExtractVariableName(const std::string& key) {
  size_t slash_pos = key.find('/');
  if (slash_pos != std::string::npos) {
    return key.substr(0, slash_pos);
  }
  return key;
}

}  // namespace zarr
}  // namespace mdio

#endif  // MDIO_ZARR_ZARR_DRIVER_H_
