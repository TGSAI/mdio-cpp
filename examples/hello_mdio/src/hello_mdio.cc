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

/**
 * @file Provides a hello world example of using the MDIO library.
 * Goals:
 * 1. Provide a simple example of including the MDIO library in a CMake project.
 * 2. Provide a simple playground for users to experiment and learn the MDIO
 * library.
 */

#include "hello_mdio.h"  // NOLINT [build/include_subdir]

#include <string>
#include <vector>

/**
 * @brief Attempts to fetch a Variable from the dataset and print it to stdout.
 * @param ds The opened dataset
 * @param varName The name of the Variable in the Dataset
 * @return An ok() Result status if the Variable was successfully printed, or an
 * error Result status otherwise.
 */
mdio::Result<void> PrintVariable(const mdio::Dataset ds,
                                 const std::string& varName) {
  MDIO_ASSIGN_OR_RETURN(mdio::Variable var, ds.variables.at(varName));
  std::cout << var << std::endl;
  return absl::OkStatus();
}

int main() {
  nlohmann::json schema = get_dataset_schema();
  std::string path = "hello.mdio";

  mdio::Future<mdio::Dataset> dsRes =
      mdio::Dataset::from_json(schema, path, mdio::constants::kCreateClean);

  if (!dsRes.status().ok()) {
    std::cerr << dsRes.status() << std::endl;
    return 1;
  }
  mdio::Dataset ds = dsRes.value();

  std::vector<std::string> sortedNames = ds.variables.get_iterable_accessor();
  mdio::Result<void> printRes;

  for (std::string& varName : sortedNames) {
    printRes = PrintVariable(ds, varName);
    if (!printRes.status().ok()) {
      std::cerr << printRes.status() << std::endl;
      return 1;
    }
  }

  return 0;
}
