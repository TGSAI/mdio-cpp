// Copyright 2025 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdio/coordinate_selector.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "mdio/dataset.h"
#include "mdio/dataset_factory.h"
#include "mdio/zarr/zarr.h"
#include "tensorstore/driver/driver.h"
#include "tensorstore/driver/registry.h"
#include "tensorstore/index_space/dim_expression.h"
#include "tensorstore/index_space/index_domain_builder.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/open.h"
#include "tensorstore/tensorstore.h"
#include "tensorstore/util/future.h"
#include "tensorstore/util/status_testutil.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace {

/**
 * @brief Returns a string representation of the Zarr version for naming.
 */
std::string ZarrVersionToString(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "V3" : "V2";
}

/**
 * @brief Returns the base path for test data based on Zarr version.
 */
std::string GetBasePath(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3
             ? "generic_with_coords_v3.mdio"
             : "generic_with_coords.mdio";
}

mdio::Result<std::string> SetupDataset(
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  std::string ds_path = GetBasePath(version);
  std::string schema_str = R"(
    {
        "metadata": {
            "name": "generic_with_coords",
            "apiVersion": "1.0.0",
            "createdOn": "2025-05-13T12:00:00.000000-05:00",
            "attributes": {
                "generic type" : true
            }
        },
        "variables": [
            {
                "name": "DataVariable",
                "dataType": "float32",
                "dimensions": ["task", "trace", "sample"],
                "coordinates": ["inline", "crossline", "live_mask"],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [1, 64, 128] }
                    }
                }
            },
            {
                "name": "inline",
                "dataType": "int32",
                "dimensions": ["task", "trace"],
                "coordinates": ["live_mask"],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [1, 64] }
                    }
                }
            },
            {
                "name": "crossline",
                "dataType": "int32",
                "dimensions": ["task", "trace"],
                "coordinates": ["live_mask"],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [1, 64] }
                    }
                }
            },
            {
                "name": "live_mask",
                "dataType": "bool",
                "dimensions": ["task", "trace"],
                "coordinates": ["inline", "crossline"],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [1, 64] }
                    }
                }
            },
            {
                "name": "task",
                "dataType": "uint32",
                "dimensions": [{"name": "task", "size": 25}],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [1] }
                    }
                }
            },
            {
                "name": "trace",
                "dataType": "uint32",
                "dimensions": [{"name": "trace", "size": 256}],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [64] }
                    }
                }
            },
            {
                "name": "sample",
                "dataType": "uint32",
                "dimensions": [{"name": "sample", "size": 512}],
                "metadata": {
                    "chunkGrid": {
                        "name": "regular",
                        "configuration": { "chunkShape": [128] }
                    }
                }
            }
        ]
    })";

  auto schema = ::nlohmann::json::parse(schema_str);
  auto dsFut =
      mdio::Dataset::from_json(schema, ds_path, version, mdio::constants::kCreateClean);
  if (!dsFut.status().ok()) {
    return ds_path;
  }
  auto ds = dsFut.value();

  // Populate the dataset with data
  MDIO_ASSIGN_OR_RETURN(auto dataVar, ds.variables.get<float>("DataVariable"));
  MDIO_ASSIGN_OR_RETURN(auto inlineVar, ds.variables.get<int32_t>("inline"));
  MDIO_ASSIGN_OR_RETURN(auto crosslineVar,
                        ds.variables.get<int32_t>("crossline"));
  MDIO_ASSIGN_OR_RETURN(auto liveMaskVar, ds.variables.get<bool>("live_mask"));

  MDIO_ASSIGN_OR_RETURN(auto varData, mdio::from_variable<float>(dataVar));
  MDIO_ASSIGN_OR_RETURN(auto inlineData,
                        mdio::from_variable<int32_t>(inlineVar));
  MDIO_ASSIGN_OR_RETURN(auto crosslineData,
                        mdio::from_variable<int32_t>(crosslineVar));
  MDIO_ASSIGN_OR_RETURN(auto liveMaskData,
                        mdio::from_variable<bool>(liveMaskVar));

  auto varDataPtr = varData.get_data_accessor().data();
  auto inlineDataPtr = inlineData.get_data_accessor().data();
  auto crosslineDataPtr = crosslineData.get_data_accessor().data();
  auto liveMaskDataPtr = liveMaskData.get_data_accessor().data();

  auto varOffset = varData.get_flattened_offset();
  auto inlineOffset = inlineData.get_flattened_offset();
  auto crosslineOffset = crosslineData.get_flattened_offset();
  auto liveMaskOffset = liveMaskData.get_flattened_offset();

  std::size_t coords = 0;
  std::size_t var = 0;

  for (int i = 0; i < 15; ++i) {  // Only 15 of the 25 tasks were "assigned"
    for (int j = 0; j < 256; ++j) {
      inlineDataPtr[coords + inlineOffset] = coords;
      crosslineDataPtr[coords + crosslineOffset] = coords * 4;
      liveMaskDataPtr[coords + liveMaskOffset] = true;
      for (int k = 0; k < 512; ++k) {
        varDataPtr[var + varOffset] = i * 256 * 512 + j * 512 + k;
        var++;
      }
      coords++;
    }
  }

  auto varDataFut = dataVar.Write(varData);
  auto inlineDataFut = inlineVar.Write(inlineData);
  auto crosslineDataFut = crosslineVar.Write(crosslineData);
  auto liveMaskDataFut = liveMaskVar.Write(liveMaskData);

  if (!varDataFut.status().ok()) {
    return varDataFut.status();
  }
  if (!inlineDataFut.status().ok()) {
    return inlineDataFut.status();
  }
  if (!crosslineDataFut.status().ok()) {
    return crosslineDataFut.status();
  }
  if (!liveMaskDataFut.status().ok()) {
    return liveMaskDataFut.status();
  }

  return ds_path;
}

// ============================================================================
// Parameterized Coordinate Selector Tests
// ============================================================================

class CoordinateSelectorTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
  }

  void TearDown() override { std::filesystem::remove_all(base_path_); }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(CoordinateSelectorTest, SETUP) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
}

TEST_P(CoordinateSelectorTest, constructor) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
  auto path = pathResult.value();

  auto dsFut = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
  auto ds = dsFut.value();

  mdio::CoordinateSelector cs(ds);
}

TEST_P(CoordinateSelectorTest, add_selection) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
  auto path = pathResult.value();

  auto dsFut = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
  auto ds = dsFut.value();

  mdio::CoordinateSelector cs(ds);
  mdio::ValueDescriptor<bool> liveMaskDesc = {"live_mask", true};
  auto isFut = cs.filterByCoordinate(liveMaskDesc);
  ASSERT_TRUE(isFut.status().ok())
      << isFut
             .status();  // When this resolves, the selection object is updated.

  // auto selections = cs.selections();
  // ASSERT_EQ(selections.size(), 2) << "Expected 2 dimensions in the selection
  // map but got " << selections.size();
}

TEST_P(CoordinateSelectorTest, range_descriptors) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
  auto path = pathResult.value();

  auto dsFut = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
  auto ds = dsFut.value();

  mdio::CoordinateSelector cs(ds);
  mdio::ValueDescriptor<bool> liveMaskDesc = {"live_mask", true};
  auto isFut = cs.filterByCoordinate(liveMaskDesc);
  ASSERT_TRUE(isFut.status().ok())
      << isFut
             .status();  // When this resolves, the selection object is updated.

  // auto rangeDescriptors = cs.range_descriptors();

  // ASSERT_EQ(rangeDescriptors.size(), 2);
  // EXPECT_EQ(rangeDescriptors[0].label.label(), "task")  << "Expected first
  // RangeDescriptor to be for the 'task' dimension";
  // EXPECT_EQ(rangeDescriptors[1].label.label(), "trace") << "Expected second
  // RangeDescriptor to be for the 'trace' dimension";

  // EXPECT_EQ(rangeDescriptors[0].start, 0) << "Expected first RangeDescriptor
  // to start at index 0"; EXPECT_EQ(rangeDescriptors[0].stop, 15) << "Expected
  // first RangeDescriptor to stop at index 15";
  // EXPECT_EQ(rangeDescriptors[0].step, 1) << "Expected first RangeDescriptor
  // to have a step of 1";

  // EXPECT_EQ(rangeDescriptors[1].start, 0) << "Expected second RangeDescriptor
  // to start at index 0"; EXPECT_EQ(rangeDescriptors[1].stop, 256) << "Expected
  // second RangeDescriptor to stop at index 256";
  // EXPECT_EQ(rangeDescriptors[1].step, 1) << "Expected second RangeDescriptor
  // to have a step of 1";
}

TEST_P(CoordinateSelectorTest, get_inline_range) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
  auto path = pathResult.value();

  auto dsFut = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
  auto ds = dsFut.value();

  mdio::CoordinateSelector cs(ds);
  mdio::ValueDescriptor<bool> liveMaskDesc = {"live_mask", true};
  auto isFut = cs.filterByCoordinate(liveMaskDesc);
  ASSERT_TRUE(isFut.status().ok())
      << isFut
             .status();  // When this resolves, the selection object is updated.
  isFut = cs.filterByCoordinate(mdio::ValueDescriptor<int32_t>{"inline", 18});
  ASSERT_TRUE(isFut.status().ok()) << isFut.status();
  // auto rangeDescriptors = cs.range_descriptors();

  // for (const auto& desc : rangeDescriptors) {
  //     std::cout << "Dimension: " << desc.label.label() << " Start: " <<
  //     desc.start << " Stop: " << desc.stop << " Step: " << desc.step <<
  //     std::endl;
  // }
}

TEST_P(CoordinateSelectorTest, get_inline_range_dead) {
  auto pathResult = SetupDataset(version_);
  ASSERT_TRUE(pathResult.status().ok()) << pathResult.status();
  auto path = pathResult.value();

  auto dsFut = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
  auto ds = dsFut.value();

  mdio::CoordinateSelector cs(ds);
  mdio::ValueDescriptor<bool> liveMaskDesc = {"live_mask", true};
  auto isFut = cs.filterByCoordinate(liveMaskDesc);
  ASSERT_TRUE(isFut.status().ok())
      << isFut
             .status();  // When this resolves, the selection object is updated.
  isFut = cs.filterByCoordinate(mdio::ValueDescriptor<int32_t>{"inline", 5000});
  EXPECT_FALSE(isFut.status().ok()) << "Expected an error when adding a "
                                       "selection for an invalid inline index";

  // auto rangeDescriptors = is.range_descriptors();
  // for (const auto& desc : rangeDescriptors) {
  //     std::cout << "Dimension: " << desc.label.label() << " Start: " <<
  //     desc.start << " Stop: " << desc.stop << " Step: " << desc.step <<
  //     std::endl;
  // }
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, CoordinateSelectorTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

}  // namespace
