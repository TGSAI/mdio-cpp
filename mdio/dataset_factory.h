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
#include "mdio/variable.h"
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
 * @brief Single source of truth for how a Variable spec differs across Zarr
 * versions.
 *
 * Every version-specific naming and structural decision -- the metadata key
 * that holds the data type, the JSON spec stub, where the chunk shape lives,
 * and how compression is encoded -- is captured here so the transform_*
 * helpers can stay version-agnostic. Supporting a new Zarr version should be a
 * matter of extending LayoutFor() and the branch in MakeStub() rather than
 * threading new conditionals through every helper.
 */
struct ZarrLayout {
  mdio::zarr::ZarrVersion version;
  // Metadata key holding the data type ("dtype" for V2, "data_type" for V3).
  std::string dtype_key;
  // True when compression uses a V3 codec pipeline vs a V2 compressor object.
  bool uses_codec_pipeline;

  /**
   * @brief Builds the empty Variable spec stub for this version.
   * @return A JSON stub with version-appropriate driver and metadata skeleton.
   */
  nlohmann::json MakeStub() const {
    if (version == mdio::zarr::ZarrVersion::kV3) {
      return R"(
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
    }
    return R"(
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

  /**
   * @brief Writes the chunk shape into the version-appropriate metadata slot.
   *
   * V3 stores it under chunk_grid/configuration/chunk_shape; V2 under "chunks".
   * @param variable A Variable stub (will be modified).
   * @param chunk_shape The chunk shape array to write.
   */
  void SetChunkShape(nlohmann::json& variable /*NOLINT*/,
                     const nlohmann::json& chunk_shape) const {
    if (version == mdio::zarr::ZarrVersion::kV3) {
      variable["metadata"]["chunk_grid"]["configuration"]["chunk_shape"] =
          chunk_shape;
    } else {
      variable["metadata"]["chunks"] = chunk_shape;
    }
  }
};

/**
 * @brief Returns the layout describing version-specific spec differences.
 * @param version The Zarr version to target.
 * @return A ZarrLayout populated for that version.
 */
inline ZarrLayout LayoutFor(mdio::zarr::ZarrVersion version) {
  switch (version) {
    case mdio::zarr::ZarrVersion::kV3:
      return ZarrLayout{version, "data_type", true};
    case mdio::zarr::ZarrVersion::kV2:
    default:
      return ZarrLayout{version, "dtype", false};
  }
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
  const ZarrLayout layout = LayoutFor(version);

  if (input["dataType"].contains("fields")) {
    // Structured dtypes are supported in both V2 and V3
    nlohmann::json dtypeFields = nlohmann::json::array();
    for (const auto& field : input["dataType"]["fields"]) {
      MDIO_ASSIGN_OR_RETURN(auto dtype,
                            to_zarr_dtype(field["format"], version));
      dtypeFields.emplace_back(nlohmann::json{field["name"], dtype});
    }
    variable["metadata"][layout.dtype_key] = dtypeFields;
  } else {
    MDIO_ASSIGN_OR_RETURN(auto dtype,
                          to_zarr_dtype(input["dataType"], version));
    variable["metadata"][layout.dtype_key] = dtype;
  }
  return absl::OkStatus();
}

/**
 * @brief Maps a Blosc shuffle value to the numcodecs (Zarr V2) integer form.
 * Accepts the MDIO schema string enum (noshuffle/shuffle/bitshuffle) as well as
 * the legacy integer form, which is passed through unchanged.
 */
inline nlohmann::json blosc_shuffle_to_int(const nlohmann::json& shuffle) {
  if (shuffle.is_string()) {
    const std::string value = shuffle.get<std::string>();
    if (value == "noshuffle") {
      return 0;
    }
    if (value == "bitshuffle") {
      return 2;
    }
    // Default / "shuffle"
    return 1;
  }
  return shuffle;
}

/**
 * @brief Maps a Blosc shuffle value to the Zarr V3 codec string form.
 * Accepts the MDIO schema string enum as well as the legacy integer form
 * (0 -> noshuffle, 2 -> bitshuffle, otherwise shuffle).
 */
inline nlohmann::json blosc_shuffle_to_string(const nlohmann::json& shuffle) {
  if (shuffle.is_number_integer()) {
    const int value = shuffle.get<int>();
    if (value == 0) {
      return "noshuffle";
    }
    if (value == 2) {
      return "bitshuffle";
    }
    return "shuffle";
  }
  return shuffle;
}

/**
 * @brief Resolves the Blosc compressor name (cname).
 * Accepts the schema key "cname" and the legacy MDIO-cpp key "algorithm" for
 * backward compatibility, defaulting to "lz4" when neither is present.
 */
inline nlohmann::json resolve_blosc_cname(const nlohmann::json& compressor) {
  if (compressor.contains("cname")) {
    return compressor["cname"];
  }
  if (compressor.contains("algorithm")) {
    return compressor["algorithm"];
  }
  return "lz4";
}

/**
 * @brief Resolves and validates the Blosc compression level (clevel).
 * Accepts the schema key "clevel" and the legacy key "level", defaulting to 5
 * when neither is present.
 * @return The clevel value, or InvalidArgumentError if outside [0, 9].
 */
inline tensorstore::Result<nlohmann::json> resolve_blosc_clevel(
    const nlohmann::json& compressor) {
  if (compressor.contains("clevel") || compressor.contains("level")) {
    auto clevel = compressor.contains("clevel") ? compressor["clevel"]
                                                : compressor["level"];
    if (clevel > 9 || clevel < 0) {
      return absl::InvalidArgumentError(
          "Compressor level must be between 0 and 9");
    }
    return clevel;
  }
  return nlohmann::json(5);
}

/**
 * @brief Resolves the Blosc blocksize, defaulting to 0 when not present.
 */
inline nlohmann::json resolve_blosc_blocksize(
    const nlohmann::json& compressor) {
  if (compressor.contains("blocksize")) {
    return compressor["blocksize"];
  }
  return nlohmann::json(0);
}

/**
 * @brief Applies the Blosc compressor to a V3 codec pipeline.
 *
 * When no compressor is supplied the stub's default "bytes" codec is left
 * untouched. Otherwise a [bytes, blosc] codec array is emitted.
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (will be modified)
 * @return OkStatus if successful, InvalidArgumentError if the compressor is not
 * a supported Blosc compressor.
 */
inline absl::Status apply_v3_codecs(nlohmann::json& input /*NOLINT*/,
                                    nlohmann::json& variable /*NOLINT*/) {
  if (!input.contains("compressor")) {
    // If no compressor, use default bytes codec (already in stub)
    return absl::OkStatus();
  }
  if (!input["compressor"].contains("name") ||
      input["compressor"]["name"] != "blosc") {
    return absl::InvalidArgumentError("Only blosc compressor is supported");
  }

  nlohmann::json blosc_codec = {{"name", "blosc"}};
  blosc_codec["configuration"] = nlohmann::json::object();
  blosc_codec["configuration"]["cname"] =
      resolve_blosc_cname(input["compressor"]);

  MDIO_ASSIGN_OR_RETURN(auto clevel, resolve_blosc_clevel(input["compressor"]));
  blosc_codec["configuration"]["clevel"] = clevel;

  // V3 blosc codec expects shuffle as a string enum. Accept the schema's
  // string form as well as the legacy integer form (0/1/2).
  if (input["compressor"].contains("shuffle") &&
      !input["compressor"]["shuffle"].is_null()) {
    blosc_codec["configuration"]["shuffle"] =
        blosc_shuffle_to_string(input["compressor"]["shuffle"]);
  } else {
    blosc_codec["configuration"]["shuffle"] = "shuffle";
  }

  blosc_codec["configuration"]["blocksize"] =
      resolve_blosc_blocksize(input["compressor"]);

  // V3 uses a codec array: bytes first, then compression
  variable["metadata"]["codecs"] =
      nlohmann::json::array({{{"name", "bytes"}}, blosc_codec});
  return absl::OkStatus();
}

/**
 * @brief Applies the Blosc compressor to a V2 compressor object.
 *
 * When no compressor is supplied the compressor metadata is set to null.
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (will be modified)
 * @return OkStatus if successful, InvalidArgumentError if the compressor is not
 * a supported Blosc compressor.
 */
inline absl::Status apply_v2_compressor(nlohmann::json& input /*NOLINT*/,
                                        nlohmann::json& variable /*NOLINT*/) {
  if (!input.contains("compressor")) {
    variable["metadata"]["compressor"] = nullptr;
    return absl::OkStatus();
  }
  if (!input["compressor"].contains("name")) {
    return absl::InvalidArgumentError("Compressor name must be specified");
  }
  if (input["compressor"]["name"] != "blosc") {
    return absl::InvalidArgumentError("Only blosc compressor is supported");
  }

  nlohmann::json& compressor = variable["metadata"]["compressor"];
  compressor["id"] = input["compressor"]["name"];
  compressor["cname"] = resolve_blosc_cname(input["compressor"]);

  MDIO_ASSIGN_OR_RETURN(auto clevel, resolve_blosc_clevel(input["compressor"]));
  compressor["clevel"] = clevel;

  // V2 (numcodecs) blosc expects shuffle as an integer. Accept the schema's
  // string enum as well as the legacy integer form.
  if (input["compressor"].contains("shuffle") &&
      !input["compressor"]["shuffle"].is_null()) {
    compressor["shuffle"] =
        blosc_shuffle_to_int(input["compressor"]["shuffle"]);
  } else {
    compressor["shuffle"] = 1;
  }

  compressor["blocksize"] = resolve_blosc_blocksize(input["compressor"]);
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
  if (LayoutFor(version).uses_codec_pipeline) {
    return apply_v3_codecs(input, variable);
  }
  return apply_v2_compressor(input, variable);
}

/**
 * @brief Modifies a Variable spec to use proper Zarr shape
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @param dimensionMap A map of dimension names to sizes
 * @return void -- Can only be successful because validation must always pass
 * before this step This presumes that the user does not attempt to use these
 * functions directly
 */
inline void transform_shape(
    nlohmann::json& input /*NOLINT*/, nlohmann::json& variable /*NOLINT*/,
    std::unordered_map<std::string, uint64_t>& dimensionMap /*NOLINT*/) {
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
  std::string var_name = variable["kvstore"]["path"].get<std::string>();
  MDIO_ASSIGN_OR_RETURN(auto loc, mdio::zarr::ResolveKvStoreLocation(path));
  variable["kvstore"] = mdio::zarr::BuildKvStoreSpec(loc, var_name);
  return absl::OkStatus();
}

/**
 * @brief Returns the byte size of a single Zarr field dtype string.
 *
 * Handles both Zarr versions; the trailing number already encodes the size:
 *   - V2: "<i2"/"<c8"/"|u1" - the number is the byte size, so it is returned
 *         directly (complex c8 -> 8, c16 -> 16).
 *   - V3: "int16"/"complex64"/"bool" - the number is the bit size, so it is
 *         divided by 8 (complex64 -> 8). "bool" has no number and is 1 byte.
 *
 * @param dtype A single Zarr dtype string for one structured field
 * @param version The Zarr version the dtype string is formatted for
 * @return The size of the dtype in bytes
 */
inline uint16_t zarr_dtype_byte_size(const std::string& dtype,
                                     mdio::zarr::ZarrVersion version) {
  if (version == mdio::zarr::ZarrVersion::kV2) {
    return static_cast<uint16_t>(std::stoi(dtype.substr(2)));
  }
  if (dtype == "bool") {
    return 1;
  }
  const size_t pos = dtype.find_first_of("0123456789");
  if (pos == std::string::npos) {
    return 0;
  }
  return static_cast<uint16_t>(std::stoi(dtype.substr(pos)) / 8);
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
    // Scalar dtypes map directly to a fixed fill value. Floats use NaN, complex
    // uses [NaN, NaN], and integers use their type maximum. bool is version
    // dependent and handled below.
    static const std::unordered_map<std::string, nlohmann::json> kScalarFill = {
        {"float16", std::nan("")},
        {"float32", std::nan("")},
        {"float64", std::nan("")},
        {"complex64", nlohmann::json::array({std::nan(""), std::nan("")})},
        {"complex128", nlohmann::json::array({std::nan(""), std::nan("")})},
        {"int8", std::numeric_limits<int8_t>::max()},
        {"int16", std::numeric_limits<int16_t>::max()},
        {"int32", std::numeric_limits<int32_t>::max()},
        {"int64", std::numeric_limits<int64_t>::max()},
        {"uint8", std::numeric_limits<uint8_t>::max()},
        {"uint16", std::numeric_limits<uint16_t>::max()},
        {"uint32", std::numeric_limits<uint32_t>::max()},
        {"uint64", std::numeric_limits<uint64_t>::max()},
    };

    const std::string dtype_str = json["dataType"].get<std::string>();
    const auto it = kScalarFill.find(dtype_str);
    if (it != kScalarFill.end()) {
      variable["metadata"]["fill_value"] = it->second;
    } else if (dtype_str == "bool" && version == mdio::zarr::ZarrVersion::kV3) {
      // Zarr V3 mandates a fill value; mdio-python resolves bool's null to
      // 0 (false) when materializing the array. Zarr V2 keeps null.
      variable["metadata"]["fill_value"] = false;
    } else {
      variable["metadata"]["fill_value"] = nlohmann::json::value_t::null;
    }
  } else {
    // Structured dtypes for both V2 and V3 use zero-initialized bytes, matching
    // mdio-python's np.void zero fill. Sum the byte size of every field.
    const std::string dtype_key = LayoutFor(version).dtype_key;
    uint16_t num_bytes = 0;
    for (const auto& field : variable["metadata"][dtype_key]) {
      num_bytes += zarr_dtype_byte_size(field[1].get<std::string>(), version);
    }
    variable["metadata"]["fill_value"] =
        encode_base64(std::string(num_bytes, '\0'));
  }
}

/**
 * @brief Sets the chunk shape on a Variable stub.
 *
 * Uses the explicit chunkGrid from the spec's "metadata" block when present,
 * otherwise falls back to the full array shape (a single chunk per array). The
 * shape metadata must already be populated via transform_shape.
 *
 * @param json A MDIO Dataset Variable list element
 * @param variable A Variable stub (will be modified)
 * @param layout The version layout describing where the chunk shape lives
 */
inline void transform_chunks(const nlohmann::json& json,
                             nlohmann::json& variable /*NOLINT*/,
                             const ZarrLayout& layout) {
  nlohmann::json chunk_shape = variable["metadata"]["shape"];
  if (json.contains("metadata") && json["metadata"].contains("chunkGrid")) {
    chunk_shape = json["metadata"]["chunkGrid"]["configuration"]["chunkShape"];
  }
  layout.SetChunkShape(variable, chunk_shape);
}

/**
 * @brief Populates the version-agnostic "attributes" block of a Variable stub.
 *
 * Copies through any user metadata, resolves the optional long name, derives
 * dimension names (supporting both the object and string dimension forms), and
 * serializes non-dimension coordinates as a space-separated string. None of
 * this differs between Zarr versions.
 *
 * @param json A MDIO Dataset Variable list element
 * @param variable A Variable stub (will be modified)
 */
inline void transform_attributes(const nlohmann::json& json,
                                 nlohmann::json& variable /*NOLINT*/) {
  if (json.contains("metadata")) {
    variable["attributes"]["metadata"] = json["metadata"];
  }

  // The longName field should be optional for a Variable, but defaulting it to
  // an empty string keeps the spec valid.
  if (json.contains("longName")) {
    variable["attributes"]["long_name"] = json["longName"];
  } else {
    variable["attributes"]["long_name"] = "";
  }

  nlohmann::json dimension_names = nlohmann::json::array();
  if (!json.contains("dimensions")) {
    dimension_names.emplace_back(json["name"]);
  } else if (json["dimensions"][0].is_object()) {
    for (const auto& dimension : json["dimensions"]) {
      dimension_names.emplace_back(dimension["name"]);
    }
  } else {
    dimension_names = json["dimensions"];
  }
  variable["attributes"]["dimension_names"] = dimension_names;

  // We do not want to serialize "dimension coordinates"
  std::set<std::string> dims;
  for (const auto& dim : dimension_names) {
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
    variable["attributes"]["coordinates"] = coordinates;
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
  const ZarrLayout layout = LayoutFor(version);

  nlohmann::json variableStub = layout.MakeStub();
  variableStub["kvstore"]["path"] = json["name"];

  MDIO_RETURN_IF_ERROR(transform_dtype(json, variableStub, version));
  MDIO_RETURN_IF_ERROR(transform_compressor(json, variableStub, version));
  transform_shape(json, variableStub, dimensionMap);
  transform_chunks(json, variableStub, layout);

  // Fill values depend only on the data type, so they must be set for every
  // Variable (including dimension coordinates that omit a "metadata" block) to
  // stay aligned with mdio-python.
  set_fill_value(json, variableStub, version);

  MDIO_RETURN_IF_ERROR(transform_metadata(path, variableStub));

  transform_attributes(json, variableStub);

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
 *                from spec's "zarr_version" field, defaulting to V3.
 * @return A vector of Variable specs or an error if the Dataset spec is invalid
 */
inline tensorstore::Result<
    std::tuple<nlohmann::json, std::vector<nlohmann::json>>>
Construct(nlohmann::json& spec /*NOLINT*/, const std::string& path,
          std::optional<mdio::zarr::ZarrVersion> version = std::nullopt) {
  // Determine the version to use
  mdio::zarr::ZarrVersion zarr_version = mdio::zarr::ZarrVersion::kV3;
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

namespace mdio {
namespace internal {

// ==========================================================================
// Creation-JSON serialization helpers (Dataset::to_json)
// ==========================================================================

/**
 * @brief Reverse of zarr::v2::ToZarrDtype: maps a numpy-style dtype string to
 * the MDIO scalar name.
 *
 * One-byte dtypes accept both the "<" and "|" byte-order prefixes (numpy
 * emits "|" for types that carry no endianness).
 * @param dtype The numpy-style dtype string (e.g. "<i4").
 * @return The MDIO scalar name (e.g. "int32"), or InvalidArgumentError for an
 * unknown dtype.
 */
inline Result<std::string> FromZarrV2Dtype(const std::string& dtype) {
  if (dtype == "<i1" || dtype == "|i1") return "int8";
  if (dtype == "<i2") return "int16";
  if (dtype == "<i4") return "int32";
  if (dtype == "<i8") return "int64";
  if (dtype == "<u1" || dtype == "|u1") return "uint8";
  if (dtype == "<u2") return "uint16";
  if (dtype == "<u4") return "uint32";
  if (dtype == "<u8") return "uint64";
  if (dtype == "<f2") return "float16";
  if (dtype == "<f4") return "float32";
  if (dtype == "<f8") return "float64";
  if (dtype == "|b1") return "bool";
  if (dtype == "<c8") return "complex64";
  if (dtype == "<c16") return "complex128";
  return absl::InvalidArgumentError("Unknown Zarr V2 dtype: " + dtype);
}

/**
 * @brief Reads the stored user attributes of a Variable from its kvstore.
 *
 * V2 stores keep them in "<variable>/.zattrs" and V3 stores in
 * "<variable>/zarr.json" under "attributes". The stored form is authoritative
 * for "statsV1"/"unitsV1"/"attributes" because the in-memory UserAttributes
 * flatten unitsV1 objects (e.g. {"speed": "m/s"} becomes "m/s"), a form the
 * creation schema rejects.
 * @param var The Variable to read from.
 * @param version The Zarr version of the underlying store.
 * @return The stored attributes object; a missing attribute file yields an
 * empty object. Read and parse errors are propagated.
 */
inline Result<nlohmann::json> ReadStoredAttributes(
    const Variable<>& var, const zarr::ZarrVersion version) {
  const tensorstore::KvStore kvstore = var.get_store().kvstore();
  // The store's kvstore path keeps its trailing slash on local file systems;
  // joining it with a leading-slash key would form an invalid "//" path.
  const char* attribute_file =
      version == zarr::ZarrVersion::kV3 ? "zarr.json" : ".zattrs";
  const std::string separator =
      kvstore.path.empty() || kvstore.path.back() == '/' ? "" : "/";
  auto read_result =
      tensorstore::kvstore::Read(kvstore, separator + attribute_file).result();
  if (!read_result.ok()) {
    return read_result.status();
  }
  if (!read_result->has_value()) {
    return nlohmann::json::object();
  }
  const auto parsed =
      nlohmann::json::parse(std::string(read_result->value), nullptr, false);
  if (parsed.is_discarded()) {
    return absl::InvalidArgumentError(
        "Failed to parse stored attributes of variable '" +
        var.get_variable_name() + "'.");
  }
  if (version == zarr::ZarrVersion::kV3) {
    if (!parsed.contains("attributes")) {
      return nlohmann::json::object();
    }
    return parsed.at("attributes");
  }
  return parsed;
}

/**
 * @brief Builds the creation-schema struct fields from a V2 numpy-style dtype
 * array (e.g. [["cdp-x", "<i4"], ["cdp-y", "<i4"]]).
 */
inline Result<nlohmann::json> StructFieldsFromV2Dtype(
    const nlohmann::json& dtype, const std::string& variable_name) {
  nlohmann::json fields = nlohmann::json::array();
  for (const nlohmann::json& pair : dtype) {
    if (!pair.is_array() || pair.size() != 2 || !pair.at(0).is_string() ||
        !pair.at(1).is_string()) {
      return absl::InvalidArgumentError(
          "Variable '" + variable_name +
          "' has a malformed V2 struct dtype entry.");
    }
    MDIO_ASSIGN_OR_RETURN(const std::string format,
                          FromZarrV2Dtype(pair.at(1).get<std::string>()));
    fields.push_back({{"name", pair.at(0)}, {"format", format}});
  }
  return nlohmann::json{{"fields", fields}};
}

/**
 * @brief Builds the creation-schema struct fields from a V3 data_type
 * descriptor ({"name": "struct", "configuration": {"fields": [...]}}).
 */
inline Result<nlohmann::json> StructFieldsFromV3DataType(
    const nlohmann::json& data_type, const std::string& variable_name) {
  if (!data_type.is_object() || !data_type.contains("configuration") ||
      !data_type.at("configuration").contains("fields")) {
    return absl::InvalidArgumentError(
        "Variable '" + variable_name +
        "' has an unsupported V3 data_type layout.");
  }
  nlohmann::json fields = nlohmann::json::array();
  for (const nlohmann::json& field :
       data_type.at("configuration").at("fields")) {
    if (!field.contains("name") || !field.contains("data_type")) {
      return absl::InvalidArgumentError(
          "Variable '" + variable_name +
          "' has a struct field without a name or data_type.");
    }
    fields.push_back(
        {{"name", field.at("name")}, {"format", field.at("data_type")}});
  }
  return nlohmann::json{{"fields", fields}};
}

/**
 * @brief Builds the creation-schema "dataType" from a Variable's zarr spec.
 *
 * Scalars map through the zarr dtype metadata (V2 numpy-style strings, V3
 * MDIO names). Structured arrays translate their per-field layout into the
 * schema's {"fields": [{"name", "format"}]} form. The top-level spec "dtype"
 * is deliberately not used: for structured arrays it is the byte view
 * ("byte"), which the creation schema rejects.
 * @param spec The Variable's zarr spec (from Variable::spec()).
 * @param version The Zarr version of the underlying store.
 * @param variable_name The Variable name, for error context.
 * @return The creation-schema dataType (a scalar name or a fields object), or
 * InvalidArgumentError for an unsupported layout.
 */
inline Result<nlohmann::json> DataTypeFromSpec(
    const nlohmann::json& spec, const zarr::ZarrVersion version,
    const std::string& variable_name) {
  const nlohmann::json& metadata = spec.at("metadata");
  if (version == zarr::ZarrVersion::kV3) {
    const nlohmann::json& data_type = metadata.at("data_type");
    if (data_type.is_string()) {
      return data_type;
    }
    return StructFieldsFromV3DataType(data_type, variable_name);
  }
  const nlohmann::json& dtype = metadata.at("dtype");
  if (dtype.is_string()) {
    return FromZarrV2Dtype(dtype.get<std::string>());
  }
  if (dtype.is_array()) {
    return StructFieldsFromV2Dtype(dtype, variable_name);
  }
  return absl::InvalidArgumentError("Variable '" + variable_name +
                                    "' has an unsupported V2 dtype.");
}

/**
 * @brief Builds the creation-schema Blosc compressor from a zarr Blosc
 * configuration.
 *
 * The V2 (numcodecs) integer shuffle form is translated to the schema's
 * string enum; the V3 form already uses the string enum and passes through.
 * @param config The Blosc configuration (cname, clevel, shuffle, blocksize,
 * optional typesize).
 * @param variable_name The Variable name, for error context.
 * @return The creation-schema compressor object, or InvalidArgumentError when
 * a required key is missing.
 */
inline Result<nlohmann::json> BloscCompressorFromConfig(
    const nlohmann::json& config, const std::string& variable_name) {
  for (const auto* key : {"cname", "clevel", "shuffle", "blocksize"}) {
    if (!config.contains(key)) {
      return absl::InvalidArgumentError("Blosc configuration of variable '" +
                                        variable_name + "' is missing key '" +
                                        key + "'.");
    }
  }
  nlohmann::json compressor = {
      {"name", "blosc"},
      {"cname", config.at("cname")},
      {"clevel", config.at("clevel")},
      {"shuffle", blosc_shuffle_to_string(config.at("shuffle"))},
      {"blocksize", config.at("blocksize")}};
  if (config.contains("typesize")) {
    compressor["typesize"] = config.at("typesize");
  }
  return compressor;
}

/**
 * @brief Builds the creation-schema "compressor" from a Variable's zarr spec.
 *
 * V2 stores express compression as metadata.compressor (numcodecs form, null
 * when uncompressed); V3 stores as a metadata.codecs pipeline ([bytes] alone
 * means uncompressed, [bytes, blosc] means Blosc). Only Blosc is supported by
 * the creation path (transform_compressor), so any other codec is reported
 * instead of being silently dropped.
 * @param spec The Variable's zarr spec (from Variable::spec()).
 * @param version The Zarr version of the underlying store.
 * @param variable_name The Variable name, for error context.
 * @return The creation-schema compressor object, null when the Variable is
 * uncompressed, or InvalidArgumentError for an unsupported codec.
 */
inline Result<nlohmann::json> CompressorFromSpec(
    const nlohmann::json& spec, const zarr::ZarrVersion version,
    const std::string& variable_name) {
  const nlohmann::json& metadata = spec.at("metadata");
  if (version == zarr::ZarrVersion::kV3) {
    if (!metadata.contains("codecs")) {
      return absl::InvalidArgumentError("Variable '" + variable_name +
                                        "' has no codecs pipeline.");
    }
    for (const nlohmann::json& codec : metadata.at("codecs")) {
      if (!codec.is_object() || !codec.contains("name")) {
        return absl::InvalidArgumentError("Variable '" + variable_name +
                                          "' has a malformed codec.");
      }
      if (codec.at("name") == "bytes") {
        continue;  // Endianness codec, always present.
      }
      if (codec.at("name") != "blosc") {
        return absl::InvalidArgumentError(
            "Variable '" + variable_name + "' uses codec '" +
            codec.at("name").get<std::string>() +
            "', which the creation schema does not support.");
      }
      if (!codec.contains("configuration")) {
        return absl::InvalidArgumentError(
            "Variable '" + variable_name +
            "' has a blosc codec without configuration.");
      }
      return BloscCompressorFromConfig(codec.at("configuration"),
                                       variable_name);
    }
    return nlohmann::json();  // [bytes] only: uncompressed.
  }
  if (!metadata.contains("compressor") || metadata.at("compressor").is_null()) {
    return nlohmann::json();  // Uncompressed.
  }
  const nlohmann::json& compressor = metadata.at("compressor");
  if (!compressor.contains("id") || compressor.at("id") != "blosc") {
    return absl::InvalidArgumentError("Variable '" + variable_name +
                                      "' uses a compressor the creation "
                                      "schema does not support.");
  }
  return BloscCompressorFromConfig(compressor, variable_name);
}

/**
 * @brief Reads the chunk grid name from a Variable's zarr spec.
 *
 * V3 specs carry it in metadata.chunk_grid.name; V2 "chunks" arrays are
 * regular grids by definition, so "regular" is returned for them.
 */
inline std::string ChunkGridNameFromSpec(const nlohmann::json& spec,
                                         const zarr::ZarrVersion version) {
  if (version == zarr::ZarrVersion::kV3) {
    const nlohmann::json& metadata = spec.at("metadata");
    if (metadata.contains("chunk_grid") &&
        metadata.at("chunk_grid").contains("name")) {
      return metadata.at("chunk_grid").at("name").get<std::string>();
    }
  }
  return "regular";
}

/**
 * @brief Builds the creation-schema "dimensions" for a Variable.
 *
 * Dimension coordinates (rank 1, name matching the dimension label) use the
 * object form [{"name", "size"}] so the create path registers the dimension;
 * every other variable uses the string form. The trailing unlabeled byte axis
 * of structured arrays is not part of the logical schema and is dropped.
 * @param var The Variable to serialize.
 * @return The creation-schema dimensions entry, or InvalidArgumentError when
 * a dimension is unlabeled in a way the schema cannot express.
 */
inline Result<nlohmann::json> DimensionsFromVariable(const Variable<>& var) {
  const std::string& name = var.get_variable_name();
  auto dimensions_view = var.dimensions();
  std::vector<std::string_view> labels(dimensions_view.labels().begin(),
                                       dimensions_view.labels().end());
  std::vector<tensorstore::Index> shape(dimensions_view.shape().begin(),
                                        dimensions_view.shape().end());
  // Drop the trailing byte axis of structured arrays (unlabeled).
  while (!labels.empty() && labels.back().empty()) {
    labels.pop_back();
    shape.pop_back();
  }
  for (const std::string_view& label : labels) {
    if (label.empty()) {
      return absl::InvalidArgumentError(
          "Variable '" + name +
          "' has an unlabeled dimension that is not "
          "the trailing byte axis; the creation schema cannot express it.");
    }
  }
  if (labels.size() == 1 && labels.front() == name) {
    // Dimension coordinate: the object form registers name and size.
    nlohmann::json dimension = {{"name", name}, {"size", shape.front()}};
    return nlohmann::json::array({dimension});
  }
  return nlohmann::json(std::vector<std::string>(labels.begin(), labels.end()));
}

/**
 * @brief Serializes one Variable into a creation-schema variable entry.
 *
 * The zarr spec supplies the dataType, dimensions and chunk grid; the stored
 * attributes ("statsV1"/"unitsV1"/"attributes") are read from the kvstore
 * because the in-memory forms flatten unitsV1 objects (see
 * ReadStoredAttributes).
 * @param var The Variable to serialize.
 * @param coordinates The non-dimension coordinate names of the Variable.
 * @param defaults Whether to include default values in the spec
 * serialization.
 * @return The creation-schema variable entry, or an error.
 */
inline Result<nlohmann::json> VariableToCreationJson(
    const Variable<>& var, const std::vector<std::string>& coordinates,
    const IncludeDefaults defaults) {
  const std::string& name = var.get_variable_name();

  auto spec_result = var.spec();
  if (!spec_result.ok()) {
    return spec_result.status();
  }
  auto json_result = spec_result.value().ToJson(defaults);
  if (!json_result.ok()) {
    return json_result.status();
  }
  const nlohmann::json spec = json_result.value();
  const zarr::ZarrVersion version = zarr::GetVersionFromSpec(spec);

  nlohmann::json entry;
  entry["name"] = name;
  MDIO_ASSIGN_OR_RETURN(entry["dataType"],
                        DataTypeFromSpec(spec, version, name));
  MDIO_ASSIGN_OR_RETURN(entry["dimensions"], DimensionsFromVariable(var));

  if (!var.get_long_name().empty()) {
    entry["longName"] = var.get_long_name();
  }

  MDIO_ASSIGN_OR_RETURN(const nlohmann::json compressor,
                        CompressorFromSpec(spec, version, name));
  if (!compressor.is_null()) {
    entry["compressor"] = compressor;
  }

  nlohmann::json metadata_block;
  MDIO_ASSIGN_OR_RETURN(const auto chunk_shape, var.get_chunk_shape());
  metadata_block["chunkGrid"] = {
      {"name", ChunkGridNameFromSpec(spec, version)},
      {"configuration", {{"chunkShape", chunk_shape}}}};
  MDIO_ASSIGN_OR_RETURN(const nlohmann::json stored_attributes,
                        ReadStoredAttributes(var, version));
  for (const auto* attribute_key : {"statsV1", "unitsV1", "attributes"}) {
    if (stored_attributes.contains(attribute_key) &&
        !stored_attributes.at(attribute_key).is_null()) {
      metadata_block[attribute_key] = stored_attributes.at(attribute_key);
    }
  }
  entry["metadata"] = metadata_block;

  if (!coordinates.empty()) {
    entry["coordinates"] = coordinates;
  }
  return entry;
}

}  // namespace internal
}  // namespace mdio

#endif  // MDIO_DATASET_FACTORY_H_
