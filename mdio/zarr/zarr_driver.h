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

}  // namespace zarr
}  // namespace mdio

#endif  // MDIO_ZARR_ZARR_DRIVER_H_
