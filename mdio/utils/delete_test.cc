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

#include "mdio/utils/delete.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "mdio/zarr/zarr.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace {

// TODO(End user): User should point to their own GCS bucket here.
/*NOLINT*/ const std::string GCS_PATH = "gs://USER_BUCKET";

/*NOLINT*/ const std::string kTestPath = "zarrs/testing/utils.mdio";

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
             ? "zarrs/testing/utils_v3.mdio"
             : "zarrs/testing/utils.mdio";
}

/**
 * @brief Returns manifest without struct arrays (compatible with both V2 and V3).
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
 * Sets up an inert dataset for testing destructive operations (V2/V3 compatible)
 */
mdio::Future<mdio::Dataset> SETUP(
    const std::string& path,
    mdio::zarr::ZarrVersion version = mdio::zarr::ZarrVersion::kV2) {
  auto j = nlohmann::json::parse(GetSimpleManifest());
  auto dsRes = mdio::Dataset::from_json(j, path, version,
                                        mdio::constants::kCreateClean);
  return dsRes;
}

// V2-only setup with struct arrays
/**
 * Sets up an inert dataset for testing destructive operations
 */
mdio::Future<mdio::Dataset> SETUP_V2(const std::string& path) {
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

// V2-only tests (with struct arrays)
TEST(DeleteDatasetV2Only, delLocal) {
  ASSERT_TRUE(SETUP_V2(kTestPath).status().ok());
  auto res = mdio::utils::DeleteDataset(kTestPath);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(kTestPath, mdio::constants::kOpen);
  EXPECT_FALSE(dsRes.status().ok()) << dsRes.status();
}

TEST(DeleteDatasetV2Only, delGCS) {
  if (GCS_PATH == "gs://USER_BUCKET") {
    GTEST_SKIP() << "Skipping GCS deletion test.\nTo enable, please update the "
                    "GCS_PATH variable in the utils_test.cc file.";
  }
  auto setupStatus = SETUP_V2(GCS_PATH);
  ASSERT_TRUE(setupStatus.status().ok()) << setupStatus.status();
  auto res = mdio::utils::DeleteDataset(GCS_PATH);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(GCS_PATH, mdio::constants::kOpen);
  EXPECT_FALSE(dsRes.status().ok()) << dsRes.status();
}

// ============================================================================
// Parameterized Delete Tests for V2/V3
// ============================================================================

class DeleteDatasetVersionTest
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

TEST_P(DeleteDatasetVersionTest, delLocal) {
  auto setupRes = SETUP(base_path_, version_);
  ASSERT_TRUE(setupRes.status().ok()) << setupRes.status();
  auto res = mdio::utils::DeleteDataset(base_path_);
  ASSERT_TRUE(res.status().ok()) << res.status();
  auto dsRes = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  EXPECT_FALSE(dsRes.status().ok()) << dsRes.status();
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, DeleteDatasetVersionTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

}  // namespace
