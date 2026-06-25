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

#include "mdio/utils/trim.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "mdio/zarr/zarr.h"

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
  return version == mdio::zarr::ZarrVersion::kV3 ? "zarrs/testing/trim_v3.mdio"
                                                 : "zarrs/testing/trim.mdio";
}

/**
 * @brief Returns a manifest exercising both scalar and struct-array variables.
 * Both V2 and V3 support the struct array (image_headers).
 */
std::string GetSimpleManifest() {
  return R"(
{
  "metadata": {
    "name": "campos_3d",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00",
    "attributes": {
      "textHeader": [
        "C01 .......................... ",
        "C02 .......................... ",
        "C03 .......................... "
      ],
      "foo": "bar"
    }
  },
  "variables": [
    {
      "name": "image",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 256},
        {"name": "crossline", "size": 512},
        {"name": "depth", "size": 384}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128, 128] }
        },
        "statsV1": {
          "count": 100,
          "sum": 1215.1,
          "sumSquares": 125.12,
          "min": 5.61,
          "max": 10.84,
          "histogram": {"binCenters":  [1, 2], "counts":  [10, 15]}
        },
        "attributes": {
          "fizz": "buzz"
        }
      },
      "coordinates": ["inline", "crossline", "depth", "cdp-x", "cdp-y"],
      "compressor": {"name": "blosc", "algorithm": "zstd"}
    },
    {
      "name": "velocity",
      "dataType": "float64",
      "dimensions": ["inline", "crossline", "depth"],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128, 128] }
        },
        "unitsV1": {"speed": "m/s"}
      },
      "coordinates": ["inline", "crossline", "depth", "cdp-x", "cdp-y"]
    },
    {
      "name": "image_inline",
      "dataType": "int16",
      "dimensions": ["inline", "crossline", "depth"],
      "longName": "inline optimized version of 3d_stack",
      "compressor": {"name": "blosc", "algorithm": "zstd"},
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128, 128] }
        }
      },
      "coordinates": ["inline", "crossline", "depth", "cdp-x", "cdp-y"]
    },
    {
      "name": "image_headers",
      "dataType": {
        "fields": [
          {"name": "cdp-x", "format": "int32"},
          {"name": "cdp-y", "format": "int32"},
          {"name": "elevation", "format": "float16"},
          {"name": "some_scalar", "format": "float16"}
        ]
      },
      "dimensions": ["inline", "crossline"],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128] }
        }
      },
      "coordinates": ["inline", "crossline", "cdp-x", "cdp-y"]
    },
    {
      "name": "inline",
      "dataType": "uint32",
      "dimensions": [{"name": "inline", "size": 256}]
    },
    {
      "name": "crossline",
      "dataType": "uint32",
      "dimensions": [{"name": "crossline", "size": 512}]
    },
    {
      "name": "depth",
      "dataType": "uint32",
      "dimensions": [{"name": "depth", "size": 384}],
      "metadata": {
        "unitsV1": { "length": "m" }
      }
    },
    {
      "name": "cdp-x",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 256},
        {"name": "crossline", "size": 512}
      ],
      "metadata": {
        "unitsV1": { "length": "m" }
      }
    },
    {
      "name": "cdp-y",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 256},
        {"name": "crossline", "size": 512}
      ],
      "metadata": {
        "unitsV1": { "length": "m" }
      }
    }
  ]
}
)";
}

/**
 * Sets up an inert dataset for testing destructive operations (V2/V3
 * compatible)
 */
mdio::Future<mdio::Dataset> SETUP(
    const std::string& path,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  auto j = nlohmann::json::parse(GetSimpleManifest());
  auto dsRes =
      mdio::Dataset::from_json(j, path, version, mdio::constants::kCreateClean);
  return dsRes;
}

// ============================================================================
// Parameterized Trim Tests for V2/V3
// ============================================================================

class TrimDatasetVersionTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
    std::filesystem::remove_all(base_path_);
  }

  void TearDown() override { std::filesystem::remove_all(base_path_); }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(TrimDatasetVersionTest, noop) {
  ASSERT_TRUE(SETUP(base_path_, version_).status().ok());
  auto res = mdio::utils::TrimDataset(base_path_, false);
  EXPECT_TRUE(res.status().ok()) << res.status();
}

TEST_P(TrimDatasetVersionTest, oneSlice) {
  ASSERT_TRUE(SETUP(base_path_, version_).status().ok());
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(base_path_, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
}

TEST_P(TrimDatasetVersionTest, oneSliceData) {
  // Set up the dataset
  ASSERT_TRUE(SETUP(base_path_, version_).status().ok());
  auto dsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto ds = dsRes.value();

  // Write some data to the inline variable
  auto inlineVarRes = ds.variables.get<mdio::dtypes::uint32_t>("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto inlineVar = inlineVarRes.value();

  auto inlineVarFuture = inlineVar.Read();
  ASSERT_TRUE(inlineVarFuture.status().ok()) << inlineVarFuture.status();
  auto inlineVarData = inlineVarFuture.value();

  auto inlineDataAccessor = inlineVarData.get_data_accessor();

  for (int i = 0; i < 256; ++i) {
    inlineDataAccessor({i}) = i + 256;
  }

  auto writeFuture = inlineVar.Write(inlineVarData);
  ASSERT_TRUE(writeFuture.status().ok()) << writeFuture.status();

  // Trim outside of a chunk boundary
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(base_path_, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();

  auto newDsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(newDsRes.status().ok()) << newDsRes.status();
  auto newDs = newDsRes.value();

  std::string name = "inline";
  auto varRes = newDs.get_variable(name);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();
  auto varFuture = var.Read();
  ASSERT_TRUE(varFuture.status().ok()) << varFuture.status();
  auto varData = varFuture.value();

  auto varDataAccessor = reinterpret_cast<mdio::dtypes::uint32_t*>(
      varData.get_data_accessor().data());
  for (int i = 0; i < 128; ++i) {
    EXPECT_EQ(varDataAccessor[i], i + 256) << "i: " << i;
  }
}

TEST_P(TrimDatasetVersionTest, oneSliceDataNoDelete) {
  // Set up the dataset
  ASSERT_TRUE(SETUP(base_path_, version_).status().ok());
  auto dsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto ds = dsRes.value();

  // Write some data to the inline variable
  auto inlineVarRes = ds.variables.get<mdio::dtypes::uint32_t>("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto inlineVar = inlineVarRes.value();

  auto inlineVarFuture = inlineVar.Read();
  ASSERT_TRUE(inlineVarFuture.status().ok()) << inlineVarFuture.status();
  auto inlineVarData = inlineVarFuture.value();

  auto inlineDataAccessor = inlineVarData.get_data_accessor();

  for (int i = 0; i < 256; ++i) {
    inlineDataAccessor({i}) = i + 256;
  }

  auto writeFuture = inlineVar.Write(inlineVarData);
  ASSERT_TRUE(writeFuture.status().ok()) << writeFuture.status();

  // Trim outside of a chunk boundary
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(base_path_, false, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();

  auto newDsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(newDsRes.status().ok()) << newDsRes.status();
  auto newDs = newDsRes.value();

  std::string name = "inline";
  auto varRes = newDs.get_variable(name);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();
  auto varFuture = var.Read();
  ASSERT_TRUE(varFuture.status().ok()) << varFuture.status();
  auto varData = varFuture.value();

  auto varDataAccessor = reinterpret_cast<mdio::dtypes::uint32_t*>(
      varData.get_data_accessor().data());
  for (int i = 0; i < 128; ++i) {
    EXPECT_EQ(varDataAccessor[i], i + 256) << "i: " << i;
  }
}

TEST_P(TrimDatasetVersionTest, metadataConsistency) {
  ASSERT_TRUE(SETUP(base_path_, version_).status().ok());
  nlohmann::json imageData;
  {
    auto dsFut = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
    ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
    auto ds = dsFut.value();
    auto imageVarRes = ds.variables.at("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    imageData = imageVarRes.value().getMetadata();
  }
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(base_path_, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto imageVarRes = dsRes.value().variables.at("image");
  ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
  EXPECT_EQ(imageVarRes.value().getMetadata(), imageData);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, TrimDatasetVersionTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

}  // namespace
