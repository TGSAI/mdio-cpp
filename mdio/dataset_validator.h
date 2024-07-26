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

#ifndef MDIO_DATASET_VALIDATOR_H
#define MDIO_DATASET_VALIDATOR_H

#include <tensorstore/tensorstore.h>

#include <fstream>
#include <nlohmann/json-schema.hpp>
#include <regex>
#include <unordered_set>

#include "dataset_schema.h"

/**
 * @brief Checks if a string is a valid ISO8601 datetime
 * @param dateTimeStr A string representing a datetime
 * @return True if valid, false otherwise
 */
bool isISO8601DateTime(const std::string& dateTimeStr) {
  std::regex iso8601Regex(
      R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}(-\d{2}:\d{2}|Z)$)");
  return std::regex_match(dateTimeStr, iso8601Regex);
}

/**
 * @brief Checks if a key exists in a map
 * Specific for our case of {coordinate: index} mapping
 */
bool contains(std::unordered_set<std::string>& set, std::string key) {
  return set.count(key);
}

/**
 * @brief Validates that a provided Dataset JSON spec conforms with the current
 * MDIO Dataset schema
 * @param spec A Dataset JSON spec
 * @return OkStatus if valid, NotFoundError if schema file load fails,
 * InvalidArgumentError if validation fails for any reason
 */
absl::Status validate_schema(nlohmann::json& spec) {
  // This is a hack to fix the date-time format not working as intended with the
  // json-schema-validator

  nlohmann::json createdOn = nlohmann::json::object();
  if (spec.contains("metadata")) {
    if (!spec["metadata"].contains("createdOn")) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "CreatedOn field not found.");
    }
    createdOn = spec["metadata"]["createdOn"];
    spec["metadata"].erase("createdOn");
    if (!isISO8601DateTime(createdOn.get<std::string>())) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "CreatedOn field is not a valid ISO8601 datetime.");
    }
  }

  nlohmann::json targetSchema = nlohmann::json::parse(kDatasetSchema);

  nlohmann::json_schema::json_validator validator;
  validator.set_root_schema(targetSchema);

  try {
    validator.validate(spec);
  } catch (const std::exception& e) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "Validation failed, here is why: " + std::string(e.what()));
  }

  if (!createdOn.empty()) {
    spec["metadata"]["createdOn"] = createdOn;
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
absl::Status validate_coordinates_present(nlohmann::json& spec) {
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

    // TODO: Implement support for list[Coordinate] later on
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
absl::Status validate_dataset(nlohmann::json& spec) {
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

#endif  // MDIO_DATASET_VALIDATOR_H