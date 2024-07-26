#ifndef DATASET_CONSTRUCTOR_H
#define DATASET_CONSTRUCTOR_H

#include "dataset_validator.h"
// #include "tensorstore/tensorstore.h"

#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"

/**
 * @brief Encodes a string in base64
 * This function is intended to be an internal helper function for formatting
 * Variable specs
 * @param raw A string to be encoded
 * @return A string encoded in base64
 */
std::string encode_base64(std::string raw) {
  std::string encoded = absl::Base64Escape(raw);
  return encoded;
}

/**
 * @brief Converts a Dataset spec dtype to a numpy style dtype
 * This function is intended to be an internal helper function for formatting
 * Variable specs It presupposes that all dtypes are little endian or endianless
 * if bool.
 * @param dtype A string representing the dtype of a Variable
 * @return A string representing the dtype in numpy format limited to the dtypes
 * supported by MDIO Dataset
 */
tensorstore::Result<std::string> to_zarr_dtype(std::string dtype) {
  // Convert the input dtype to Zarr dtype
  if (dtype == "int8") {
    return "<i1";
  } else if (dtype == "int16") {
    return "<i2";
  } else if (dtype == "int32") {
    return "<i4";
  } else if (dtype == "int64") {
    return "<i8";
  } else if (dtype == "uint8") {
    return "<u1";
  } else if (dtype == "uint16") {
    return "<u2";
  } else if (dtype == "uint32") {
    return "<u4";
  } else if (dtype == "uint64") {
    return "<u8";
  } else if (dtype == "float16") {
    return "<f2";
  } else if (dtype == "float32") {
    return "<f4";
  } else if (dtype == "float64") {
    return "<f8";
  } else if (dtype == "bool") {
    return "|b1";
  } else if (dtype == "complex64") {
    return "<c8";
  } else if (dtype == "complex128") {
    return "<c16";
  }
  return absl::InvalidArgumentError("Unknown dtype: " + dtype);
}

/**
 * @brief Modifies a Variable spec to use proper Zarr dtype
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @return OkStatus if successful, InvalidArgumentError if dtype is not
 * supported
 */
absl::Status transform_dtype(nlohmann::json& input, nlohmann::json& variable) {
  if (input["dataType"].contains("fields")) {
    nlohmann::json dtypeFields = nlohmann::json::array();
    for (const auto& field : input["dataType"]["fields"]) {
      auto dtype = to_zarr_dtype(field["format"]);
      if (!dtype.status().ok()) {
        return dtype.status();
      }
      dtypeFields.emplace_back(nlohmann::json{field["name"], dtype.value()});
    }
    variable["metadata"]["dtype"] = dtypeFields;
  } else {
    auto dtype = to_zarr_dtype(input["dataType"]);
    if (!dtype.status().ok()) {
      return dtype.status();
    }
    variable["metadata"]["dtype"] = dtype.value();
  }
  return absl::OkStatus();
}

/**
 * @brief Modifies a Variable spec to use proper Zarr compressor
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param input A MDIO Variable spec
 * @param variable A Variable stub (Will be modified)
 * @return OkStatus if successful, InvalidArgumentError if compressor is invalid
 * for MDIO
 */
absl::Status transform_compressor(nlohmann::json& input,
                                  nlohmann::json& variable) {
  if (input.contains("compressor")) {
    if (input["compressor"].contains("name")) {
      if (input["compressor"]["name"] != "blosc") {
        return absl::InvalidArgumentError("Only blosc compressor is supported");
      }
      variable["metadata"]["compressor"]["id"] = input["compressor"]["name"];
    } else {
      return absl::InvalidArgumentError("Compressor name must be specified");
    }

    if (input["compressor"].contains("algorithm")) {
      variable["metadata"]["compressor"]["cname"] =
          input["compressor"]["algorithm"];
    } else {  // DEFAULT
      variable["metadata"]["compressor"]["cname"] = "lz4";
    }
    if (input["compressor"].contains("level")) {
      // TODO: Is this done by the schema?
      if (input["compressor"]["level"] > 9 ||
          input["compressor"]["level"] < 0) {
        return absl::InvalidArgumentError(
            "Compressor level must be between 0 and 9");
      }
      variable["metadata"]["compressor"]["clevel"] =
          input["compressor"]["level"];
    } else {  // DEFAULT
      variable["metadata"]["compressor"]["clevel"] = 5;
    }
    if (input["compressor"].contains("shuffle")) {
      variable["metadata"]["compressor"]["shuffle"] =
          input["compressor"]["shuffle"];
    } else {  // DEFAULT
      variable["metadata"]["compressor"]["shuffle"] = 1;
    }
    if (input["compressor"].contains("blocksize")) {
      variable["metadata"]["compressor"]["blocksize"] =
          input["compressor"]["blocksize"];
    } else {  // DEFAULT
      variable["metadata"]["compressor"]["blocksize"] = 0;
    }
  } else {
    variable["metadata"]["compressor"] = nullptr;
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
 * @return void -- Can only be successful because validation must always pass
 * before this step This presumes that the user does not attempt to use these
 * functions directly
 */
void transform_shape(nlohmann::json& input, nlohmann::json& variable,
                     std::unordered_map<std::string, int>& dimensionMap) {
  if (input["dimensions"][0].is_object()) {
    nlohmann::json shape = nlohmann::json::array();
    for (auto& dimension : input["dimensions"]) {
      shape.emplace_back(dimensionMap[dimension["name"]]);
    }
    variable["metadata"]["shape"] = shape;
  } else {
    nlohmann::json shape = nlohmann::json::array();
    for (auto& dimension : input["dimensions"]) {
      shape.emplace_back(dimensionMap[dimension]);
    }
    variable["metadata"]["shape"] = shape;
  }
}

/**
 * @brief Modifies a Variable spec to use proper Zarr metadata
 * This function is intended to be an internal helper function for formatting
 * Variable specs It will modify with side-effect on "input"
 * @param metadata The "path" supplied in the Dataset metadata attributes spec
 * @param variable A Variable stub (Will be modified)
 * @return OkStatus if successful, InvalidArgumentError if the path is invalid
 */
absl::Status transform_metadata(const std::string& path,
                                nlohmann::json& variable) {
  std::string bucket =
      "NULL";  // Default value, if is NULL don't add a bucket field
  std::string driver = "file";
  if (absl::StartsWith(path, "gs://")) {
    driver = "gcs";
  } else if (absl::StartsWith(path, "s3://")) {
    driver = "s3";
  }

  std::string filepath;

  if (driver != "file") {
    std::string _path = path;
    // Ensure _path has a trailing slash or the mdio file will not be created
    // properly
    if (_path.back() != '/') {
      _path += '/';
    }
    _path = _path.substr(5);
    std::vector<std::string> file_parts = absl::StrSplit(_path, '/');
    if (file_parts.size() < 2) {
      return absl::InvalidArgumentError(
          "Cloud path requires [gs/s3]://[bucket]/[path to file] name");
    }
    bucket = file_parts[0];
    filepath = file_parts[1];
    for (std::size_t i = 2; i < file_parts.size(); ++i) {
      filepath += "/" + file_parts[i];
    }
    filepath += variable["kvstore"]["path"].get<std::string>();
  } else {
    filepath = path + "/";
    filepath += variable["kvstore"]["path"].get<std::string>();
  }
  variable["kvstore"]["path"] = filepath;
  variable["kvstore"]["driver"] = driver;
  if (bucket != "NULL") {
    variable["kvstore"]["bucket"] = bucket;
  }

  return absl::OkStatus();
}

/**
 * @brief Constructs an MDIO Variable spec from an MDIO Dataset Variable list
 * element This function is intended to be an internal helper function for
 * formatting Variable specs
 * @param json A MDIO Dataset Variable list element
 * @param dimensionMap A map of dimension names to sizes
 * @return A Variable spec or an error if the Variable spec is invalid
 */
tensorstore::Result<nlohmann::json> from_json_to_spec(
    nlohmann::json& json, std::unordered_map<std::string, int>& dimensionMap,
    const std::string& path) {
  nlohmann::json variableStub = R"(
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
  variableStub["kvstore"]["path"] = json["name"];

  auto transformStatus = transform_dtype(json, variableStub);
  if (!transformStatus.ok()) {
    return transformStatus;
  }

  auto compressorStatus = transform_compressor(json, variableStub);
  if (!compressorStatus.ok()) {
    return compressorStatus;
  }

  transform_shape(json, variableStub, dimensionMap);

  if (json.contains("metadata")) {
    if (json["metadata"].contains("chunkGrid")) {
      variableStub["metadata"]["chunks"] =
          json["metadata"]["chunkGrid"]["configuration"]["chunkShape"];
    } else {  // No chunking specified
      variableStub["metadata"]["chunks"] = variableStub["metadata"]["shape"];
    }

    if (!json["dataType"].contains("fields")) {
      variableStub["metadata"]["fill_value"] = nlohmann::json::value_t::null;
      if (json["dataType"] == "complex64") {
        std::string raw(8, '\0');
        variableStub["metadata"]["fill_value"] = encode_base64(raw);
      } else if (json["dataType"] == "complex128") {
        std::string raw(16, '\0');
        variableStub["metadata"]["fill_value"] = encode_base64(raw);
      } else if (json["dataType"].get<std::string>()[0] == 'f') {
        variableStub["metadata"]["fill_value"] = std::nan("");
      }
    } else {
      // Accumulate the total number of bytes (N)
      uint16_t num_bytes = 0;
      // We're going to use the variable dtype because it's already in byte
      // format
      std::string dtype;
      for (auto field : variableStub["metadata"]["dtype"]) {
        dtype = field[1].get<std::string>();
        if (dtype.at(1) != 'c') {
          num_bytes += std::stoi(dtype.substr(2));
        } else {  // Complex
          if (dtype.at(2) == '8') {
            num_bytes += 8;
          } else {
            num_bytes += 16;
          }
        }
      }
      std::string raw(num_bytes, '\0');
      variableStub["metadata"]["fill_value"] = encode_base64(raw);
    }

    variableStub["attributes"]["metadata"] = json["metadata"];
  } else {  // No metadata supplied means no chunkGrid
    variableStub["metadata"]["chunks"] = variableStub["metadata"]["shape"];
  }

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
tensorstore::Result<std::unordered_map<std::string, int>> get_dimensions(
    nlohmann::json& spec) {
  std::unordered_map<std::string, int> dimensions;
  for (auto& variable : spec["variables"]) {
    if (variable["dimensions"][0].is_object()) {
      for (auto& dimension : variable["dimensions"]) {
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
 * @return A vector of Variable specs or an error if the Dataset spec is invalid
 */
tensorstore::Result<std::tuple<nlohmann::json, std::vector<nlohmann::json>>>
Construct(nlohmann::json& spec, const std::string& path) {
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

  std::unordered_map<std::string, int> dimensionMap = dimensions.value();

  std::vector<nlohmann::json> datasetSpec;
  for (auto& variable : spec["variables"]) {
    auto variableSpec = from_json_to_spec(variable, dimensionMap, path);
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

#endif  // DATASET_CONSTRUCTOR_H
