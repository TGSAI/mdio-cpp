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
 * @file The intention here is to complete an example where we read a dataset
 * that has been written by Python's xarray. In the example xarray is used to
 * initialize the "image" variable to all 1's.
 */
#include "absl/flags/flag.h"
#include "absl/flags/marshalling.h"
#include "absl/flags/parse.h"
#include "absl/status/status.h"
#include "mdio/dataset.h"

absl::Status Run(std::string dataset_path) {
  MDIO_ASSIGN_OR_RETURN(
      auto dataset,
      mdio::Dataset::Open(dataset_path, mdio::constants::kOpen).result())

  MDIO_ASSIGN_OR_RETURN(auto variable,
                        dataset.variables.get<float32_t>("image"))

  MDIO_ASSIGN_OR_RETURN(auto variable_data, variable.Read().result())

  auto image = variable_data.get_data_accessor();

  // scan the first row
  for (auto i = image.domain()[0].inclusive_min();
       i < image.domain()[0].exclusive_max(); ++i) {
    if (image({i, 0, 0}) != 1.0f) {
      std::string error_message = "Non-one value found at (" +
                                  std::to_string(i) + ", " + std::to_string(0) +
                                  ", " + std::to_string(0) +
                                  "): " + std::to_string(image({i, 0, 0}));
      return absl::InternalError(error_message);
    }
  }

  return absl::OkStatus();  // All elements are 1.0, return OK status.
}

ABSL_FLAG(std::string, PATH, "", "Provide the output path for the dataset.");
int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string path = absl::GetFlag(FLAGS_PATH);

  if (path.empty()) {
    std::cerr << "Error: --PATH is required.\n";
    return 1;  // Exit with error code
  }

  auto status = Run(path);

  if (!status.ok()) {
    std::cout << "Task failed.\n" << status << std::endl;
  } else {
    std::cout << "MDIO mdio from xarray example complete.\n";
  }
  return status.ok() ? 0 : 1;
}