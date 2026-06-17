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

#ifndef MDIO_DATASET_FACTORY_H_
#define MDIO_DATASET_FACTORY_H_

#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "mdio/dataset_validator.h"
#include "mdio/impl.h"
#include "mdio/zarr/zarr.h"

/**
 * @brief Encodes a string in base64
 * This function is intended to be an internal helper function for formatting
 * Variable specs
 * @param raw A string to be encoded
 * @return A string encoded in base64
 */
inline std::string encode_base64(const std::string raw) {
  std::string encoded = absl::Base64Escape(raw);
  return encoded;
}

/**
 * @brief Converts a Dataset spec dtype to a numpy style dtype (Zarr V2)
 * This function is intended to be an internal helper function for formatting
 * Variable specs It presupposes that all dtypes are little endian or endianless
 * if bool.
 * @param dtype A string representing the dtype of a Variable
 * @return A string representing the dtype in numpy format limited to the dtypes
 * supported by MDIO Dataset
 * @deprecated Use mdio::zarr::v2::ToZarrDtype for explicit V2 conversions.
 */
inline tensorstore::Result<std::string> to_zarr_dtype(const std::string dtype) {
  return mdio::zarr::v2::ToZarrDtype(dtype);
}

/**
 * @brief Converts a Dataset spec dtype to the appropriate Zarr format
 * @param dtype A string representing the dtype of a Variable
 * @param version The Zarr version to target
 * @return The dtype in the appropriate format for the version
 */
inline tensorstore::Result<nlohmann::json> to_zarr_dtype(
    const std::string dtype, mdio::zarr::ZarrVersion version) {
  return mdio::zarr::ConvertDtype(version, dtype);
}

/**
 * @brief Modifies a Variable spec to use proper Zarr dtype
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @param version The Zarr version to target (defaults to V2)
 * @return OkStatus if successful, InvalidArgumentError if dtype is not
 * supported
 */
inline absl::Status transform_dtype(
    nlohmann::json& input /*NOLINT*/, nlohmann::json& variable /*NOLINT*/,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  std::string dtype_key =
      (version == mdio::zarr::ZarrVersion::kV3) ? "data_type" : "dtype";

  if (input["dataType"].contains("fields")) {
    // Structured dtypes are supported in both V2 and V3
    nlohmann::json dtypeFields = nlohmann::json::array();
    for (const auto& field : input["dataType"]["fields"]) {
      auto dtype = to_zarr_dtype(field["format"], version);
      if (!dtype.status().ok()) {
        return dtype.status();
      }
      dtypeFields.emplace_back(nlohmann::json{field["name"], dtype.value()});
    }
    variable["metadata"][dtype_key] = dtypeFields;
  } else {
    auto dtype = to_zarr_dtype(input["dataType"], version);
    if (!dtype.status().ok()) {
      return dtype.status();
    }
    variable["metadata"][dtype_key] = dtype.value();
  }
  return absl::OkStatus();
}

/**
 * @brief Modifies a Variable spec to use proper Zarr compressor
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @param version The Zarr version to target (defaults to V2)
 * @return OkStatus if successful, InvalidArgumentError if compressor is invalid
 * for MDIO
 */
inline absl::Status transform_compressor(
    nlohmann::json& input /*NOLINT*/, nlohmann::json& variable /*NOLINT*/,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  if (version == mdio::zarr::ZarrVersion::kV3) {
    // V3 uses codec pipeline
    if (input.contains("compressor")) {
      if (!input["compressor"].contains("name") ||
          input["compressor"]["name"] != "blosc") {
        return absl::InvalidArgumentError("Only blosc compressor is supported");
      }

      nlohmann::json blosc_codec = {{"name", "blosc"}};
      blosc_codec["configuration"] = nlohmann::json::object();

      if (input["compressor"].contains("algorithm")) {
        blosc_codec["configuration"]["cname"] =
            input["compressor"]["algorithm"];
      } else {
        blosc_codec["configuration"]["cname"] = "lz4";
      }

      if (input["compressor"].contains("level")) {
        if (input["compressor"]["level"] > 9 ||
            input["compressor"]["level"] < 0) {
          return absl::InvalidArgumentError(
              "Compressor level must be between 0 and 9");
        }
        blosc_codec["configuration"]["clevel"] = input["compressor"]["level"];
      } else {
        blosc_codec["configuration"]["clevel"] = 5;
      }

      if (input["compressor"].contains("shuffle")) {
        blosc_codec["configuration"]["shuffle"] =
            input["compressor"]["shuffle"];
      } else {
        blosc_codec["configuration"]["shuffle"] = "shuffle";
      }

      if (input["compressor"].contains("blocksize")) {
        blosc_codec["configuration"]["blocksize"] =
            input["compressor"]["blocksize"];
      } else {
        blosc_codec["configuration"]["blocksize"] = 0;
      }

      // V3 uses a codec array: bytes first, then compression
      variable["metadata"]["codecs"] =
          nlohmann::json::array({{{"name", "bytes"}}, blosc_codec});
    }
    // If no compressor, use default bytes codec (already in stub)
  } else {
    // V2 uses compressor object
    if (input.contains("compressor")) {
      if (input["compressor"].contains("name")) {
        if (input["compressor"]["name"] != "blosc") {
          return absl::InvalidArgumentError(
              "Only blosc compressor is supported");
        }
        variable["metadata"]["compressor"]["id"] = input["compressor"]["name"];
      } else {
        return absl::InvalidArgumentError("Compressor name must be specified");
      }

      if (input["compressor"].contains("algorithm")) {
        variable["metadata"]["compressor"]["cname"] =
            input["compressor"]["algorithm"];
      } else {
        variable["metadata"]["compressor"]["cname"] = "lz4";
      }
      if (input["compressor"].contains("level")) {
        if (input["compressor"]["level"] > 9 ||
            input["compressor"]["level"] < 0) {
          return absl::InvalidArgumentError(
              "Compressor level must be between 0 and 9");
        }
        variable["metadata"]["compressor"]["clevel"] =
            input["compressor"]["level"];
      } else {
        variable["metadata"]["compressor"]["clevel"] = 5;
      }
      if (input["compressor"].contains("shuffle")) {
        variable["metadata"]["compressor"]["shuffle"] =
            input["compressor"]["shuffle"];
      } else {
        variable["metadata"]["compressor"]["shuffle"] = 1;
      }
      if (input["compressor"].contains("blocksize")) {
        variable["metadata"]["compressor"]["blocksize"] =
            input["compressor"]["blocksize"];
      } else {
        variable["metadata"]["compressor"]["blocksize"] = 0;
      }
    } else {
      variable["metadata"]["compressor"] = nullptr;
    }
  }
  return absl::OkStatus();
}

/**
 * @brief Modifies a Variable spec to use proper Zarr shape
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @param dimensionMap A map of dimension names to sizes
 * @param version The Zarr version to target (defaults to V2)
 * @return void -- Can only be successful because validation must always pass
 * before this step This presumes that the user does not attempt to use these
 * functions directly
 */
inline void transform_shape(
    nlohmann::json& input /*NOLINT*/, nlohmann::json& variable /*NOLINT*/,
    std::unordered_map<std::string, uint64_t>& dimensionMap /*NOLINT*/,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  nlohmann::json shape = nlohmann::json::array();
  if (input["dimensions"][0].is_object()) {
    for (auto& dimension : input["dimensions"]) {
      shape.emplace_back(dimensionMap[dimension["name"]]);
    }
  } else {
    for (auto& dimension : input["dimensions"]) {
      shape.emplace_back(dimensionMap[dimension]);
    }
  }
  variable["metadata"]["shape"] = shape;
}

/**
 * @brief Modifies a Variable spec to use proper Zarr metadata
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param metadata The "path" supplied in the Dataset metadata attributes spec
 * @param variable A Variable stub (Will be modified)
 * @return OkStatus if successful, InvalidArgumentError if the path is invalid
 */
inline absl::Status transform_metadata(const std::string& path,
                                       nlohmann::json& variable /*NOLINT*/) {
  // Use shared utilities for driver inference and path handling
  std::string driver = mdio::zarr::InferDriverFromPath(path);
  std::string var_name = variable["kvstore"]["path"].get<std::string>();

  variable["kvstore"]["driver"] = driver;

  if (driver == "file") {
    // Local filesystem - normalize path with trailing slash
    variable["kvstore"]["path"] =
        mdio::zarr::NormalizePathWithSlash(path) + var_name;
  } else {
    // Cloud storage (GCS or S3) - extract bucket and path
    auto [bucket, cloud_path] = mdio::zarr::ExtractCloudPath(path);
    if (bucket.empty()) {
      return absl::InvalidArgumentError(
          "Cloud path requires [gs/s3]://[bucket]/[path to file] name");
    }
    variable["kvstore"]["bucket"] = bucket;
    variable["kvstore"]["path"] =
        mdio::zarr::NormalizePathWithSlash(cloud_path) + var_name;
  }

  return absl::OkStatus();
}

/**
 * @brief Sets the Zarr fill value on a Variable stub based on its data type.
 *
 * The selected fill values mirror mdio-python's `fill_value_map`, which is the
 * source of truth for cross-implementation parity:
 *   - bool            -> null (no fill); Zarr V3 requires a concrete value, so
 *                        we emit false to match mdio-python materializing 0.
 *   - float{16,32,64} -> NaN
 *   - int{8,16,32,64} -> type maximum (e.g. int32 -> 2147483647)
 *   - uint{8..64}     -> type maximum (e.g. uint32 -> 4294967295)
 *   - complex{64,128} -> complex(NaN, NaN), encoded as [real, imag]
 *   - structured      -> zero-initialized bytes (base64 encoded)
 *
 * This must be applied to every Variable regardless of whether the spec carries
 * a "metadata" block, so that dimension coordinates (which often omit metadata)
 * receive the same fill values as data variables.
 *
 * @param json A MDIO Dataset Variable list element
 * @param variable A Variable stub (will be modified). Its dtype metadata must
 *                 already be populated via transform_dtype.
 * @param version The Zarr version to target
 */
inline void set_fill_value(const nlohmann::json& json,
                           nlohmann::json& variable /*NOLINT*/,
                           mdio::zarr::ZarrVersion version) {
  if (!json["dataType"].contains("fields")) {
    std::string dtype_str = json["dataType"].get<std::string>();
    variable["metadata"]["fill_value"] = nlohmann::json::value_t::null;
    if (dtype_str == "complex64" || dtype_str == "complex128") {
      variable["metadata"]["fill_value"] =
          nlohmann::json::array({std::nan(""), std::nan("")});
    } else if (!dtype_str.empty() && dtype_str[0] == 'f') {
      variable["metadata"]["fill_value"] = std::nan("");
    } else if (dtype_str == "bool") {
      if (version == mdio::zarr::ZarrVersion::kV3) {
        // Zarr V3 mandates a fill value; mdio-python resolves bool's null to
        // 0 (false) when materializing the array.
        variable["metadata"]["fill_value"] = false;
      }
      // Zarr V2 keeps the null fill value set above.
    } else if (dtype_str == "int8") {
      variable["metadata"]["fill_value"] = std::numeric_limits<int8_t>::max();
    } else if (dtype_str == "int16") {
      variable["metadata"]["fill_value"] = std::numeric_limits<int16_t>::max();
    } else if (dtype_str == "int32") {
      variable["metadata"]["fill_value"] = std::numeric_limits<int32_t>::max();
    } else if (dtype_str == "int64") {
      variable["metadata"]["fill_value"] = std::numeric_limits<int64_t>::max();
    } else if (dtype_str == "uint8") {
      variable["metadata"]["fill_value"] = std::numeric_limits<uint8_t>::max();
    } else if (dtype_str == "uint16") {
      variable["metadata"]["fill_value"] = std::numeric_limits<uint16_t>::max();
    } else if (dtype_str == "uint32") {
      variable["metadata"]["fill_value"] = std::numeric_limits<uint32_t>::max();
    } else if (dtype_str == "uint64") {
      variable["metadata"]["fill_value"] = std::numeric_limits<uint64_t>::max();
    }
  } else {
    // Structured dtypes for both V2 and V3 use zero-initialized bytes, matching
    // mdio-python's np.void zero fill. Accumulate the total number of bytes (N).
    uint16_t num_bytes = 0;
    std::string dtype_key =
        version == mdio::zarr::ZarrVersion::kV3 ? "data_type" : "dtype";
    for (const auto& field : variable["metadata"][dtype_key]) {
      std::string dtype = field[1].get<std::string>();
      if (version == mdio::zarr::ZarrVersion::kV2) {
        // V2 format: "<i2", "<f4", "<c8", etc. - number is byte size
        if (dtype.at(1) != 'c') {
          num_bytes += std::stoi(dtype.substr(2));
        } else {
          // Complex types: c8 = 8 bytes, c16 = 16 bytes
          if (dtype.at(2) == '8') {
            num_bytes += 8;
          } else {
            num_bytes += 16;
          }
        }
      } else {
        // V3 format: "int16", "float32", "complex64", etc. - number is bit
        // size
        if (dtype.find("complex") == 0) {
          // complex64 = 8 bytes, complex128 = 16 bytes
          int bits = std::stoi(dtype.substr(7));
          num_bytes += bits / 8;
        } else if (dtype == "bool") {
          num_bytes += 1;
        } else {
          // Extract the number from the end (int16 -> 16, float32 -> 32)
          size_t pos = dtype.find_first_of("0123456789");
          if (pos != std::string::npos) {
            int bits = std::stoi(dtype.substr(pos));
            num_bytes += bits / 8;
          }
        }
      }
    }
    std::string raw(num_bytes, '\0');
    variable["metadata"]["fill_value"] = encode_base64(raw);
  }
}

/**
 * @brief Constructs an MDIO Variable spec from an MDIO Dataset Variable list
 * element This function is intended to be an internal helper function for
 * formatting Variable specs
 * @param json A MDIO Dataset Variable list element
 * @param dimensionMap A map of dimension names to sizes
 * @param path The path for the variable
 * @param version The Zarr version to use (defaults to V2)
 * @return A Variable spec or an error if the Variable spec is invalid
 */
inline tensorstore::Result<nlohmann::json> from_json_to_spec(
    nlohmann::json& json /*NOLINT*/,
    std::unordered_map<std::string, uint64_t>& dimensionMap /*NOLINT*/,
    const std::string& path,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  nlohmann::json variableStub;

  if (version == mdio::zarr::ZarrVersion::kV3) {
    // Zarr V3 spec structure
    variableStub = R"(
        {
            "driver": "zarr3",
            "kvstore": {
                "driver": "file",
                "path": "VARIABLE_NAME"
            },
            "metadata": {
                "data_type": "DATA_TYPE",
                "shape": [],
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {
                        "chunk_shape": []
                    }
                },
                "chunk_key_encoding": {
                    "name": "default",
                    "configuration": {
                        "separator": "/"
                    }
                },
                "codecs": [{"name": "bytes"}]
            },
            "attributes": {}
        }
    )"_json;
  } else {
    // Zarr V2 spec structure
    variableStub = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "VARIABLE_NAME"
            },
            "metadata": {
                "dtype": "DATA_TYPE",
                "dimension_separator": "/",
                "shape": "SHAPE",
                "chunks": "CHUNKS"
            },
            "attributes": {}
        }
    )"_json;
  }
  variableStub["kvstore"]["path"] = json["name"];

  auto transformStatus = transform_dtype(json, variableStub, version);
  if (!transformStatus.ok()) {
    return transformStatus;
  }

  auto compressorStatus = transform_compressor(json, variableStub, version);
  if (!compressorStatus.ok()) {
    return compressorStatus;
  }

  transform_shape(json, variableStub, dimensionMap, version);

  if (json.contains("metadata")) {
    nlohmann::json chunkShape;
    if (json["metadata"].contains("chunkGrid")) {
      chunkShape = json["metadata"]["chunkGrid"]["configuration"]["chunkShape"];
    } else {
      chunkShape = variableStub["metadata"]["shape"];
    }

    if (version == mdio::zarr::ZarrVersion::kV3) {
      // V3 uses chunk_grid
      variableStub["metadata"]["chunk_grid"]["configuration"]["chunk_shape"] =
          chunkShape;
    } else {
      // V2 uses chunks
      variableStub["metadata"]["chunks"] = chunkShape;
    }

    variableStub["attributes"]["metadata"] = json["metadata"];
  } else {
    // No metadata supplied means no chunkGrid
    if (version == mdio::zarr::ZarrVersion::kV3) {
      variableStub["metadata"]["chunk_grid"]["configuration"]["chunk_shape"] =
          variableStub["metadata"]["shape"];
    } else {
      variableStub["metadata"]["chunks"] = variableStub["metadata"]["shape"];
    }
  }

  // Fill values depend only on the data type, so they must be set for every
  // Variable (including dimension coordinates that omit a "metadata" block) to
  // stay aligned with mdio-python.
  set_fill_value(json, variableStub, version);

  auto transform_result = transform_metadata(path, variableStub);
  if (!transform_result.ok()) {
    return transform_result;
  }

  // I think the longName field should be optional for a Variable but this
  // ensures the spec is valid.
  if (json.contains("longName")) {
    variableStub["attributes"]["long_name"] = json["longName"];
  } else {
    variableStub["attributes"]["long_name"] = "";
  }

  if (!json.contains("dimensions")) {
    nlohmann::json dimension_names = nlohmann::json::array();
    dimension_names.emplace_back(json["name"]);
    variableStub["attributes"]["dimension_names"] = dimension_names;
  } else if (json["dimensions"][0].is_object()) {
    nlohmann::json dimension_names = nlohmann::json::array();
    for (size_t i = 0; i < json["dimensions"].size(); ++i) {
      dimension_names.emplace_back(json["dimensions"][i]["name"]);
    }
    variableStub["attributes"]["dimension_names"] = dimension_names;
  } else {
    variableStub["attributes"]["dimension_names"] = json["dimensions"];
  }

  // We do not want to seralize "dimension coordinates"
  std::set<std::string> dims;
  for (auto& dim : variableStub["attributes"]["dimension_names"]) {
    dims.insert(dim.get<std::string>());
  }

  if (json.contains("coordinates")) {
    // This appears to need to be a space separated string, not a list
    std::string coordinates;
    std::size_t coords_size = json["coordinates"].size();
    for (size_t i = 0; i < coords_size; ++i) {
      std::string coord = json["coordinates"][i].get<std::string>();
      if (dims.count(coord) > 0) {
        continue;
      }
      coordinates += coord;
      if (i != coords_size - 1) {
        coordinates += " ";
      }
    }
    variableStub["attributes"]["coordinates"] = coordinates;
  }

  return variableStub;
}

/**
 * @brief Accumulates a map of the dimensions in a Dataset and their sizes
 * This function is intended to be an internal helper function for formatting
 * Variable specs It's intended to be used while the Variables are considered to
 * be the same size. This behavior will need to change as MDIO gains
 * functionality
 * @param spec A Dataset spec
 * @return A map of dimension names to sizes or error if the dimensions are not
 * consistently sized
 */
inline tensorstore::Result<std::unordered_map<std::string, uint64_t>>
get_dimensions(nlohmann::json& spec /*NOLINT*/) {
  std::unordered_map<std::string, uint64_t> dimensions;
  for (auto& variable : spec["variables"]) {
    if (variable["dimensions"][0].is_object()) {
      for (auto& dimension : variable["dimensions"]) {
        if (dimension["size"].get<uint64_t>() > mdio::constants::kMaxSize) {
          return absl::InvalidArgumentError(
              "Dimension " + dimension["name"].dump() +
              " exceeds maximum size of " +
              std::to_string(mdio::constants::kMaxSize));
        }
        if (dimensions.count(dimension["name"]) == 0) {
          dimensions[dimension["name"]] = dimension["size"];
        } else {
          if (dimensions[dimension["name"]] != dimension["size"]) {
            return absl::InvalidArgumentError("Dimension " +
                                              dimension["name"].dump() +
                                              " has conflicting sizes");
          }
        }
      }
    }
  }
  return dimensions;
}

/**
 * @brief Constructs a vector of valid Variable specs from a Dataset spec
 * This should be the only function called by the user to construct a Dataset
 * from a spec
 * @param spec A Dataset spec
 * @param path The path for the dataset
 * @param version Optional Zarr version to use. If not specified, auto-detects
 *                from spec's "zarr_version" field, defaulting to V2.
 * @return A vector of Variable specs or an error if the Dataset spec is invalid
 */
inline tensorstore::Result<
    std::tuple<nlohmann::json, std::vector<nlohmann::json>>>
Construct(nlohmann::json& spec /*NOLINT*/, const std::string& path,
          std::optional<mdio::zarr::ZarrVersion> version = std::nullopt) {
  // Determine the version to use
  mdio::zarr::ZarrVersion zarr_version = mdio::zarr::ZarrVersion::kV2;
  if (version.has_value()) {
    zarr_version = version.value();
  } else if (spec.contains("zarr_version")) {
    auto version_result = mdio::zarr::ParseVersion(spec["zarr_version"]);
    if (version_result.ok()) {
      zarr_version = version_result.value();
    }
  }

  // Validation should only return status codes. If it returns data then it
  // should be a "constructor"
  auto status = validate_dataset(spec);
  if (!status.ok()) {
    return status;
  }

  // This made more sense to validate in the constructor because I require this
  // data
  auto dimensions = get_dimensions(spec);
  if (!dimensions.status().ok()) {
    return dimensions.status();
  }

  std::unordered_map<std::string, uint64_t> dimensionMap = dimensions.value();

  std::vector<nlohmann::json> datasetSpec;
  for (auto& variable : spec["variables"]) {
    auto variableSpec =
        from_json_to_spec(variable, dimensionMap, path, zarr_version);
    if (!variableSpec.status().ok()) {
      return variableSpec.status();
    }
    datasetSpec.emplace_back(variableSpec.value());
  }
  if (!spec.contains("metadata")) {
    spec["metadata"] = nlohmann::json::object();
  }
  return std::make_tuple(spec["metadata"], datasetSpec);
}

#endif  // MDIO_DATASET_FACTORY_H_
