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

#ifndef EXAMPLES_HELLO_MDIO_SRC_HELLO_MDIO_H_
#define EXAMPLES_HELLO_MDIO_SRC_HELLO_MDIO_H_

#include <mdio/mdio.h>

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>

/**
 * @brief Defines a schema for a dataset with 3 variables: X, Y, and Grid.
 * @return A JSON object representing the dataset schema.
 */
nlohmann::json get_dataset_schema() {
  std::string schemaStr = R"(
{
    "metadata": {
        "apiVersion": "1.0.0",
        "name": "Demo MDIO",
        "createdOn": "2024-08-01T15:50:00.000000Z"
    },
    "variables": [
        {
            "name": "X",
            "dataType": "uint32",
            "dimensions": [{"name": "X", "size": 10}]
        },
        {
            "name": "Y",
            "dataType": "uint32",
            "dimensions": [{"name": "Y", "size": 10}]
        },
        {
            "name": "Grid",
            "dataType": "float32",
            "dimensions": ["X", "Y"]
        }
    ]
}
    )";
  try {
    return nlohmann::json::parse(schemaStr);
  } catch (nlohmann::json::parse_error& e) {
    std::cerr << "Failed to parse schema: " << e.what() << std::endl;
  }
  return nlohmann::json();
}

#endif  // EXAMPLES_HELLO_MDIO_SRC_HELLO_MDIO_H_
