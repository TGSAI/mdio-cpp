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

#ifndef MDIO_ZARR_ZARR_H_
#define MDIO_ZARR_ZARR_H_

/**
 * @file zarr.h
 * @brief Unified header for Zarr support in MDIO.
 *
 * This header provides a unified interface for working with both Zarr V2 and
 * Zarr V3 formats. It abstracts the differences between the two versions
 * and provides version-agnostic operations where possible.
 *
 * Key features:
 * - Zarr V2: Full support including consolidated metadata (.zmetadata)
 * - Zarr V3: Support without consolidated metadata (as per Zarr V3 spec)
 *
 * Usage:
 * @code
 * #include "mdio/zarr/zarr.h"
 *
 * // Detect version from existing store
 * auto version = mdio::zarr::DetectVersion(kvstore);
 *
 * // Write metadata for the appropriate version
 * if (version == mdio::zarr::ZarrVersion::kV2) {
 *   mdio::zarr::v2::WriteConsolidatedMetadata(metadata, variables);
 * } else {
 *   mdio::zarr::v3::WriteMetadata(metadata, variables);
 * }
 * @endcode
 */

#include <string>
#include <tuple>
#include <vector>

#include "mdio/zarr/zarr_driver.h"
#include "mdio/zarr/zarr_v2.h"
#include "mdio/zarr/zarr_v3.h"

namespace mdio {
namespace zarr {

/**
 * @brief Writes dataset metadata using the appropriate format for the version.
 *
 * For V2, this writes consolidated metadata (.zmetadata, .zgroup, .zattrs).
 * For V3, this writes only the root zarr.json (no consolidated metadata).
 *
 * @param version The Zarr version to use.
 * @param dataset_metadata The metadata for the dataset.
 * @param json_variables The JSON variables.
 * @param context Optional TensorStore context for credentials/configuration.
 * @return An `mdio::Future<void>` representing the asynchronous write.
 */
inline Future<void> WriteDatasetMetadata(
    ZarrVersion version, const ::nlohmann::json& dataset_metadata,
    const std::vector<::nlohmann::json>& json_variables,
    tensorstore::Context context = tensorstore::Context::Default()) {
  switch (version) {
    case ZarrVersion::kV3:
      return v3::WriteMetadata(dataset_metadata, json_variables, context);
    case ZarrVersion::kV2:
    default:
      return v2::WriteConsolidatedMetadata(dataset_metadata, json_variables,
                                           context);
  }
}

/**
 * @brief Reads dataset metadata using the appropriate format for the version.
 *
 * For V2, this reads from consolidated metadata (.zmetadata).
 * For V3, this reads from zarr.json and discovers arrays.
 *
 * @param version The Zarr version to use.
 * @param dataset_path The path to the dataset.
 * @param kvs_future A future to the KvStore.
 * @return A future containing dataset metadata and variable JSON specs.
 */
inline Future<std::tuple<::nlohmann::json, std::vector<::nlohmann::json>>>
ReadDatasetMetadata(ZarrVersion version, const std::string& dataset_path,
                    tensorstore::Future<tensorstore::KvStore> kvs_future) {
  switch (version) {
    case ZarrVersion::kV3:
      return v3::ReadMetadata(dataset_path, kvs_future);
    case ZarrVersion::kV2:
    default:
      return v2::ReadConsolidatedMetadata(dataset_path, kvs_future);
  }
}

/**
 * @brief Gets the driver name for the given version.
 * @param version The Zarr version.
 * @return std::string The driver name.
 */
inline std::string GetDriverNameForVersion(ZarrVersion version) {
  switch (version) {
    case ZarrVersion::kV3:
      return std::string(v3::kDriverName);
    case ZarrVersion::kV2:
    default:
      return std::string(v2::kDriverName);
  }
}

/**
 * @brief Converts a generic dtype string to the appropriate format.
 *
 * @param version The Zarr version.
 * @param dtype The input dtype string.
 * @return Result<nlohmann::json> The dtype in the appropriate format.
 */
inline Result<nlohmann::json> ConvertDtype(ZarrVersion version,
                                           const std::string& dtype) {
  switch (version) {
    case ZarrVersion::kV3: {
      MDIO_ASSIGN_OR_RETURN(auto result, v3::ToZarrDtype(dtype));
      return result;
    }
    case ZarrVersion::kV2:
    default: {
      MDIO_ASSIGN_OR_RETURN(auto result, v2::ToZarrDtype(dtype));
      return nlohmann::json(result);
    }
  }
}

/**
 * @brief Creates a variable spec stub for the given version.
 *
 * @param version The Zarr version.
 * @param variable_name The name of the variable.
 * @param path The path prefix.
 * @return nlohmann::json The variable spec stub.
 */
inline nlohmann::json CreateVariableSpec(ZarrVersion version,
                                         const std::string& variable_name,
                                         const std::string& path = "") {
  switch (version) {
    case ZarrVersion::kV3:
      return v3::CreateVariableSpec(variable_name, path);
    case ZarrVersion::kV2:
    default:
      return v2::CreateVariableSpec(variable_name, path);
  }
}

/**
 * @brief Checks if a JSON spec is for a Zarr V3 store.
 *
 * @param json_spec The JSON specification.
 * @return bool True if the spec is for Zarr V3.
 */
inline bool IsV3Spec(const nlohmann::json& json_spec) {
  if (json_spec.contains("driver")) {
    std::string driver = json_spec["driver"].get<std::string>();
    return driver == "zarr3";
  }
  return false;
}

/**
 * @brief Extracts the Zarr version from a JSON spec.
 *
 * @param json_spec The JSON specification.
 * @return ZarrVersion The detected version.
 */
inline ZarrVersion GetVersionFromSpec(const nlohmann::json& json_spec) {
  if (IsV3Spec(json_spec)) {
    return ZarrVersion::kV3;
  }
  return ZarrVersion::kV2;
}

/**
 * @brief Updates a JSON spec to use the specified Zarr version.
 *
 * @param json_spec The JSON specification to update.
 * @param version The target Zarr version.
 * @return nlohmann::json The updated specification.
 */
inline nlohmann::json UpdateSpecVersion(nlohmann::json json_spec,
                                        ZarrVersion version) {
  json_spec["driver"] = GetDriverNameForVersion(version);
  return json_spec;
}

}  // namespace zarr
}  // namespace mdio

#endif  // MDIO_ZARR_ZARR_H_
