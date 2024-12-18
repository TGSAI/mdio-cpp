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

#include <string>

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace {

/*NOLINT*/ const std::string kTestPath = "zarrs/testing/utils.mdio";

/**
 * Sets up an inert dataset for testing destructive operations
 */
mdio::Future<mdio::Dataset> SETUP(const std::string& path) {
  std::string datasetManifest = R"(
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

  auto j = nlohmann::json::parse(datasetManifest);
  auto dsRes = mdio::Dataset::from_json(j, path, mdio::constants::kCreateClean);
  return dsRes;
}

TEST(TrimDataset, noop) {
  ASSERT_TRUE(SETUP(kTestPath).status().ok());
  auto res = mdio::utils::TrimDataset(kTestPath, false);
  EXPECT_TRUE(res.status().ok()) << res.status();
}

TEST(TrimDataset, oneSlice) {
  ASSERT_TRUE(SETUP(kTestPath).status().ok());
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(kTestPath, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
}

TEST(TrimDataset, oneSliceData) {
  // Set up the dataset
  ASSERT_TRUE(SETUP(kTestPath).status().ok());
  auto dsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
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

  // Trim outside of a chunk boundry
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(kTestPath, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();

  auto newDsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
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

TEST(TrimDataset, oneSliceDataNoDelete) {
  // Set up the dataset
  ASSERT_TRUE(SETUP(kTestPath).status().ok());
  auto dsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
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

  // Trim outside of a chunk boundry
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(kTestPath, false, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();

  auto newDsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
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

TEST(TrimDataset, metadataConsistency) {
  ASSERT_TRUE(SETUP(kTestPath).status().ok());
  nlohmann::json imageData;
  {
    auto dsFut = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
    ASSERT_TRUE(dsFut.status().ok()) << dsFut.status();
    auto ds = dsFut.value();
    auto imageVarRes = ds.variables.at("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    imageData = imageVarRes.value().getMetadata();
  }
  mdio::RangeDescriptor<mdio::Index> slice = {"inline", 0, 128, 1};
  auto res = mdio::utils::TrimDataset(kTestPath, true, slice);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto imageVarRes = dsRes.value().variables.at("image");
  ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
  EXPECT_EQ(imageVarRes.value().getMetadata(), imageData);
}

}  // namespace
