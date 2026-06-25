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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/log/globals.h"
#include "absl/log/initialize.h"
#include "mdio/dataset.h"
#include "mdio/zarr/zarr.h"

namespace {

// Initialize Abseil logging before tests run to suppress verbose AWS logs
struct AbslLogInit {
  AbslLogInit() {
    absl::InitializeLog();
    // kError = show only errors, kInfinity = suppress all logs
    absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfinity);
  }
};
static AbslLogInit absl_log_init;

// TODO(End user): User should point to their own S3 bucket here.
// You may find the test dataset at: TODO: Upload the test dataset to a public
// object store
/*NOLINT*/ std::string const S3_PATH = "s3://USER_BUCKET/";

/**
 * @brief Returns a string representation of the Zarr version for naming.
 */
std::string ZarrVersionToString(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "V3" : "V2";
}

/**
 * @brief Returns the S3 path suffix for test data based on Zarr version.
 */
std::string GetS3PathSuffix(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "/test_v3" : "/test_v2";
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
    }]
}
)";
}

// ============================================================================
// Parameterized S3 Tests for V2/V3
// ============================================================================

class S3VersionTest : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    test_path_ = S3_PATH + GetS3PathSuffix(version_);
  }

  mdio::zarr::ZarrVersion version_;
  std::string test_path_;
};

TEST_P(S3VersionTest, create) {
  if (S3_PATH == "s3://USER_BUCKET/") {
    GTEST_SKIP() << "Please set the S3_PATH to your own bucket in the "
                    "s3_test.cc file.";
  }
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto dataset = mdio::Dataset::from_json(j, test_path_, version_,
                                          mdio::constants::kCreateClean);
  EXPECT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST_P(S3VersionTest, open) {
  if (S3_PATH == "s3://USER_BUCKET/") {
    GTEST_SKIP() << "Please set the S3_PATH to your own bucket in the "
                    "s3_test.cc file.";
  }
  // First create the dataset
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto createRes = mdio::Dataset::from_json(j, test_path_, version_,
                                            mdio::constants::kCreateClean);
  ASSERT_TRUE(createRes.status().ok()) << createRes.status();

  // Then open it
  auto dataset = mdio::Dataset::Open(test_path_, mdio::constants::kOpen);
  EXPECT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST_P(S3VersionTest, readWrite) {
  if (S3_PATH == "s3://USER_BUCKET/") {
    GTEST_SKIP() << "Please set the S3_PATH to your own bucket in the "
                    "s3_test.cc file.";
  }
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto createRes = mdio::Dataset::from_json(j, test_path_, version_,
                                            mdio::constants::kCreateClean);
  ASSERT_TRUE(createRes.status().ok()) << createRes.status();

  auto dataset = mdio::Dataset::Open(test_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();

  // Read and write to the image variable
  auto imageVarRes = ds.variables.get<mdio::dtypes::float32_t>("image");
  ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
  auto imageVar = imageVarRes.value();

  auto imageDataRes = imageVar.Read();
  ASSERT_TRUE(imageDataRes.status().ok()) << imageDataRes.status();
  auto imageData = imageDataRes.value();

  auto accessor = imageData.get_data_accessor().data();
  accessor[0] = 3.14f;
  accessor[1] = 2.71f;

  auto writeFut = imageVar.Write(imageData);
  ASSERT_TRUE(writeFut.status().ok()) << writeFut.status();

  // Re-read and verify
  auto rereadRes = imageVar.Read();
  ASSERT_TRUE(rereadRes.status().ok()) << rereadRes.status();
  auto rereadAccessor = rereadRes.value().get_data_accessor().data();

  EXPECT_FLOAT_EQ(rereadAccessor[0], 3.14f);
  EXPECT_FLOAT_EQ(rereadAccessor[1], 2.71f);
}

TEST_P(S3VersionTest, selectField) {
  if (S3_PATH == "s3://USER_BUCKET/") {
    GTEST_SKIP() << "Please set the S3_PATH to your own bucket in the "
                    "s3_test.cc file.";
  }
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto createRes = mdio::Dataset::from_json(j, test_path_, version_,
                                            mdio::constants::kCreateClean);
  ASSERT_TRUE(createRes.status().ok()) << createRes.status();

  auto dataset = mdio::Dataset::Open(test_path_, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  auto future = ds.SelectField("image_headers", "cdp-x");
  ASSERT_TRUE(future.status().ok()) << future.status();
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, S3VersionTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

}  // namespace
