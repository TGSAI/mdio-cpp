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
 * @file This is a part of an example where we round read/write with MDIO v1.0
 * and Python's Xarray. Here we create a MDIO v1.0 dataset.
 */

#include "xarray_integration.h"

#include <mdio/mdio.h>

#include "absl/flags/flag.h"
#include "absl/flags/marshalling.h"
#include "absl/status/status.h"

using Index = mdio::Index;

absl::Status Run(std::string dataset_path) {
  std::cout << dataset_path << std::endl;

  if (dataset_path.empty()) {
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path();
    dataset_path = temp_dir / "zarrs/seismic_3d/";
  }

  std::cout << dataset_path << std::endl;

  auto json_spec = XarrayExample();

  // This example round-trips through Python's xarray, which reads the store via
  // zarr-python 2.x (see pyproject.toml) using consolidated metadata. That is a
  // Zarr V2 concept, so we pin this dataset to V2 explicitly. Bumping to V3
  // would require zarr-python 3.x and a v3-capable xarray.
  MDIO_ASSIGN_OR_RETURN(auto dataset,
                        mdio::Dataset::from_json(json_spec, dataset_path,
                                                 mdio::zarr::ZarrVersion::kV2,
                                                 mdio::constants::kCreateClean)
                            .result());

  auto populate_inline = [](SharedArray<uint32_t>& data) {
    for (auto i = data.domain()[0].inclusive_min();
         i < data.domain()[0].exclusive_max(); ++i) {
      data(i) = i * 10 + 1000;
    }
  };

  RETURN_IF_NOT_OK(
      populate_and_write_variable<uint32_t>(dataset, "inline", populate_inline))

  auto populate_crossline = [](SharedArray<uint32_t>& data) {
    for (auto i = data.domain()[0].inclusive_min();
         i < data.domain()[0].exclusive_max(); ++i) {
      data(i) = (data.domain()[0].exclusive_max() + 100 - i) * 10;
    }
  };

  RETURN_IF_NOT_OK(populate_and_write_variable<uint32_t>(dataset, "crossline",
                                                         populate_crossline))
  auto populate_depth = [](SharedArray<uint32_t>& data) {
    for (auto i = data.domain()[0].inclusive_min();
         i < data.domain()[0].exclusive_max(); ++i) {
      data(i) = i * 10;
    }
  };

  RETURN_IF_NOT_OK(
      populate_and_write_variable<uint32_t>(dataset, "depth", populate_depth))

  return absl::OkStatus();
}

ABSL_FLAG(std::string, PATH, "", "Provide the output path for the dataset.");
int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  auto status = Run(absl::GetFlag(FLAGS_PATH));

  if (!status.ok()) {
    std::cout << "Task failed.\n" << status << std::endl;
  } else {
    std::cout << "MDIO to Xarray example complete.\n";
  }
  return status.ok() ? 0 : 1;
}