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

#ifndef MDIO_DATASET_VALIDATOR_H_
#define MDIO_DATASET_VALIDATOR_H_

#include <fstream>
#include <string>
#include <unordered_set>

#include "mdio/dataset_schema.h"
#include "tensorstore/tensorstore.h"

// clang-format off
#include <nlohmann/json-schema.hpp>  // NOLINT
// clang-format on

/**
 * @brief Checks if a key exists in a map
 * Specific for our case of {coordinate: index} mapping
 */
bool contains(const std::unordered_set<std::string>& set,
              const std::string key) {
  return set.count(key);
}

/**
 * @brief Validates that a provided Dataset JSON spec conforms with the current
 * MDIO Dataset schema
 * @param spec A Dataset JSON spec
 * @return OkStatus if valid, NotFoundError if schema file load fails,
 * InvalidArgumentError if validation fails for any reason
 */
absl::Status validate_schema(nlohmann::json& spec /*NOLINT*/) {
  nlohmann::json targetSchema = nlohmann::json::parse(kDatasetSchema);

  nlohmann::json_schema::json_validator validator(
      nullptr, nlohmann::json_schema::default_string_format_check);
  validator.set_root_schema(targetSchema);

  try {
    validator.validate(spec);
  } catch (const std::exception& e) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "Validation failed, here is why: " + std::string(e.what()));
  }

  return absl::OkStatus();
}

/**
 * @brief Validates that all mentioned coordinates have a matching Variable in
 * the Dataset spec. This is intended to catch any Dataset specs that would pass
 * the schema validation but reference a coordinate that does not exist.
 * @param spec A Dataset JSON spec.
 * @return OkStatus if valid, InvalidArgumentError if a coordinate does not have
 * a matching Variable.
 */
absl::Status validate_coordinates_present(const nlohmann::json& spec) {
  // Build a mapping of all the dimension coordinates
  std::unordered_set<std::string>
      dimension;  //  name of all 1-d Variables who's name matches the dimension
                  //  name
  // Using this will ensure that a variable["coordinates"][i] has a defined
  // variable["name"]
  std::unordered_set<std::string> variables;  // Name of all Variables
  for (auto& variable : spec["variables"]) {
    variables.insert(variable["name"].dump());
    if (variable["dimensions"][0].is_object()) {
      if (variable["dimensions"].size() == 1 &&
          variable["dimensions"][0]["name"] == variable["name"]) {
        // This must be a dimension coordinate -- Must exist if a Variable has
        // the dimension
        dimension.insert(variable["name"].dump());
      }
    }
  }

  // Make sure all the Variables have a dimension coordinate
  for (auto& variable : spec["variables"]) {
    // No need to verify a dimension coordinate
    if (contains(dimension, variable["name"].dump())) {
      continue;
    }

    if (variable["dimensions"][0].is_object()) {  // list[NamedDimensions]
      for (auto& dimensionName : variable["dimensions"]) {
        if (!contains(dimension, dimensionName["name"].dump())) {
          return absl::Status(absl::StatusCode::kInvalidArgument,
                              "Variable " + variable["name"].dump() +
                                  " has a dimension " +
                                  dimensionName["name"].dump() +
                                  " that is not a dimension coordinate.");
        }
      }
    } else {  // list[str]
      for (auto& dimensionName : variable["dimensions"]) {
        if (!contains(dimension, dimensionName.dump())) {
          return absl::Status(absl::StatusCode::kInvalidArgument,
                              "Variable " + variable["name"].dump() +
                                  " has a dimension " + dimensionName.dump() +
                                  " that is not a dimension coordinate.");
        }
      }
    }

    // TODO(BrianMichell): Implement support for list[Coordinate] later on
    if (variable.contains("coordinates")) {
      for (auto& coordinate : variable["coordinates"]) {
        if (!contains(variables, coordinate.dump())) {
          return absl::Status(absl::StatusCode::kInvalidArgument,
                              "Variable " + variable["name"].dump() +
                                  " has a coordinate " + coordinate.dump() +
                                  " that is not a Variable.");
        }
      }
    }
  }

  return absl::OkStatus();
}

/**
 * @brief Validates that a provided Dataset JSON spec conforms with the current
 MDIO Dataset schema and Dataset construction rules
 * @param spec A Dataset JSON spec
 * @return OkStatus if valid, NotFoundError if schema file load fails,
 InvalidArgumentError if validation fails for any
 * reason

*/
absl::Status validate_dataset(nlohmann::json& spec /*NOLINT*/) {
  absl::Status schemaStatus = validate_schema(spec);
  if (!schemaStatus.ok()) {
    return schemaStatus;
  }

  absl::Status coordinateStatus = validate_coordinates_present(spec);
  if (!coordinateStatus.ok()) {
    return coordinateStatus;
  }

  return absl::OkStatus();
}

#endif  // MDIO_DATASET_VALIDATOR_H_
