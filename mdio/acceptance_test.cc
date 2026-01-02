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
 * @file acceptance_test.cc
 * @brief Unified acceptance tests for both Zarr V2 and V3 driver support.
 *
 * This file contains parameterized tests that verify the MDIO library works
 * correctly with both Zarr V2 and Zarr V3 formats, including variable creation,
 * reading, writing, and dataset operations.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <nlohmann/json.hpp>  // NOLINT

#include "mdio/dataset.h"
#include "mdio/dataset_factory.h"
#include "mdio/zarr/zarr.h"

namespace {

constexpr char PYTHON_EXECUTABLE[] = "python3";
constexpr char PROJECT_BASE_PATH_ENV[] = "PROJECT_BASE_PATH";
constexpr char DEFAULT_BASE_PATH[] = "../..";
constexpr char ZARR_SCRIPT_RELATIVE_PATH[] =
    "/mdio/regression_tests/zarr_compatibility.py";
constexpr char XARRAY_SCRIPT_RELATIVE_PATH[] =
    "/mdio/regression_tests/xarray_compatibility_test.py";
constexpr int ERROR_CODE = EXIT_FAILURE;
constexpr int SUCCESS_CODE = EXIT_SUCCESS;

using float16_t = mdio::dtypes::float_16_t;

/**
 * @brief Returns a string representation of the Zarr version for naming.
 */
std::string ZarrVersionToString(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "V3" : "V2";
}

/**
 * @brief Returns the base path for test data based on Zarr version.
 * Note: Does NOT include trailing slash.
 */
std::string GetBasePath(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "zarrs/acceptance_v3"
                                                 : "zarrs/acceptance";
}

/**
 * @brief Returns the TensorStore driver name for the given version.
 */
std::string GetTestDriverName(mdio::zarr::ZarrVersion version) {
  return version == mdio::zarr::ZarrVersion::kV3 ? "zarr3" : "zarr";
}

/**
 * @brief Creates a variable spec for testing based on Zarr version.
 */
nlohmann::json CreateVariableSpec(mdio::zarr::ZarrVersion version,
                                  const std::string& name,
                                  const std::string& base_path) {
  nlohmann::json spec;
  spec["driver"] = GetTestDriverName(version);
  spec["kvstore"]["driver"] = "file";
  spec["kvstore"]["path"] = base_path + "/" + name;

  if (version == mdio::zarr::ZarrVersion::kV3) {
    spec["metadata"]["data_type"] = "int16";
    spec["metadata"]["shape"] = nlohmann::json::array({10, 10});
    spec["metadata"]["chunk_grid"]["name"] = "regular";
    spec["metadata"]["chunk_grid"]["configuration"]["chunk_shape"] =
        nlohmann::json::array({5, 5});
    spec["metadata"]["chunk_key_encoding"]["name"] = "default";
    spec["metadata"]["chunk_key_encoding"]["configuration"]["separator"] = "/";
    nlohmann::json bytes_codec;
    bytes_codec["name"] = "bytes";
    spec["metadata"]["codecs"] = nlohmann::json::array({bytes_codec});
    spec["attributes"]["metadata"]["attributes"]["foo"] = "bar";
    spec["attributes"]["long_name"] = "2-byte integer test";
    spec["attributes"]["dimension_names"] =
        nlohmann::json::array({"inline", "crossline"});
    spec["attributes"]["dimension_units"] = nlohmann::json::array({"m", "m"});
  } else {
    spec["metadata"]["dtype"] = "<i2";
    spec["metadata"]["shape"] = nlohmann::json::array({10, 10});
    spec["metadata"]["chunks"] = nlohmann::json::array({5, 5});
    spec["metadata"]["dimension_separator"] = "/";
    spec["metadata"]["compressor"]["id"] = "blosc";
    spec["attributes"]["metadata"]["attributes"]["foo"] = "bar";
    spec["attributes"]["long_name"] = "2-byte integer test";
    spec["attributes"]["dimension_names"] =
        nlohmann::json::array({"inline", "crossline"});
    spec["attributes"]["dimension_units"] = nlohmann::json::array({"m", "m"});
  }

  return spec;
}

/**
 * @brief Creates a typed variable spec based on version and dtype.
 */
nlohmann::json CreateTypedVariableSpec(mdio::zarr::ZarrVersion version,
                                       const std::string& name,
                                       const std::string& base_path,
                                       const std::string& dtype_v2,
                                       const std::string& dtype_v3,
                                       const std::string& long_name) {
  nlohmann::json spec;
  spec["driver"] = GetTestDriverName(version);
  spec["kvstore"]["driver"] = "file";
  spec["kvstore"]["path"] = base_path + "/" + name;

  if (version == mdio::zarr::ZarrVersion::kV3) {
    spec["metadata"]["data_type"] = dtype_v3;
    spec["metadata"]["shape"] = nlohmann::json::array({10, 10});
    spec["metadata"]["chunk_grid"]["name"] = "regular";
    spec["metadata"]["chunk_grid"]["configuration"]["chunk_shape"] =
        nlohmann::json::array({5, 5});
    spec["metadata"]["chunk_key_encoding"]["name"] = "default";
    spec["metadata"]["chunk_key_encoding"]["configuration"]["separator"] = "/";
    nlohmann::json bytes_codec;
    bytes_codec["name"] = "bytes";
    spec["metadata"]["codecs"] = nlohmann::json::array({bytes_codec});
    spec["attributes"]["metadata"]["attributes"]["foo"] = "bar";
    spec["attributes"]["long_name"] = long_name;
    spec["attributes"]["dimension_names"] =
        nlohmann::json::array({"inline", "crossline"});
    spec["attributes"]["dimension_units"] = nlohmann::json::array({"m", "m"});
  } else {
    spec["metadata"]["dtype"] = dtype_v2;
    spec["metadata"]["shape"] = nlohmann::json::array({10, 10});
    spec["metadata"]["chunks"] = nlohmann::json::array({5, 5});
    spec["metadata"]["dimension_separator"] = "/";
    spec["metadata"]["compressor"]["id"] = "blosc";
    spec["attributes"]["metadata"]["attributes"]["foo"] = "bar";
    spec["attributes"]["long_name"] = long_name;
    spec["attributes"]["dimension_names"] =
        nlohmann::json::array({"inline", "crossline"});
    spec["attributes"]["dimension_units"] = nlohmann::json::array({"m", "m"});
  }

  return spec;
}

/**
 * @brief Creates a base spec (for opening) based on version.
 */
nlohmann::json CreateBaseSpec(mdio::zarr::ZarrVersion version,
                              const std::string& name,
                              const std::string& base_path) {
  nlohmann::json spec;
  spec["driver"] = GetTestDriverName(version);
  spec["kvstore"]["driver"] = "file";
  spec["kvstore"]["path"] = base_path + "/" + name;
  return spec;
}

/**
 * @brief Returns the dataset manifest JSON for the given version.
 */
std::string GetDatasetManifest(mdio::zarr::ZarrVersion version) {
  std::string name =
      version == mdio::zarr::ZarrVersion::kV3 ? "campos_3d_v3" : "campos_3d";

  // Note: V3 doesn't support struct arrays yet, so we use a slightly different
  // manifest
  if (version == mdio::zarr::ZarrVersion::kV3) {
    return R"(
{
  "metadata": {
    "name": "campos_3d_v3",
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
          "configuration": { "chunkShape": [4, 512, 512] }
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
  } else {
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
}

/**
 * @brief Executes the Python regression tests
 */
int executePythonScript(const std::string& scriptPath,
                        const std::vector<std::string>& args) {
  if (scriptPath.empty() || args.empty()) {
    std::cerr << "Error: scriptPath or args are empty." << std::endl;
    return ERROR_CODE;
  }

  if (scriptPath[0] != '/' && scriptPath.find(DEFAULT_BASE_PATH) != 0) {
    std::cerr
        << "Error: PROJECT_BASE_PATH must be an absolute path or not be set."
        << std::endl;
    return ERROR_CODE;
  }

  std::vector<const char*> execArgs = {PYTHON_EXECUTABLE, scriptPath.c_str()};
  for (const auto& arg : args) {
    execArgs.push_back(arg.c_str());
  }
  execArgs.push_back(nullptr);

  if (execvp(execArgs[0], const_cast<char* const*>(execArgs.data())) == -1) {
    perror("execvp failed");
    return ERROR_CODE;
  }

  return SUCCESS_CODE;
}

// ============================================================================
// Parameterized Variable Tests
// ============================================================================

class VariableTest : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
    driver_ = GetTestDriverName(version_);
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
  std::string driver_;
};

TEST_P(VariableTest, SETUP) {
  // Ensure directory exists
  std::filesystem::create_directories(base_path_);

  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  // Create i2 (int16)
  auto i2Spec = CreateTypedVariableSpec(version_, "i2", base_path_, "<i2",
                                        "int16", "2-byte integer test");
  auto i2Schema = mdio::internal::ValidateAndProcessJson(i2Spec).value();
  auto [i2Store, i2Metadata] = i2Schema;
  auto i2 =
      mdio::internal::CreateVariable(i2Store, i2Metadata, std::move(options));
  ASSERT_TRUE(i2.status().ok()) << i2.status();

  // Create i4 (int32)
  auto i4Spec = CreateTypedVariableSpec(version_, "i4", base_path_, "<i4",
                                        "int32", "4-byte integer test");
  auto i4Schema = mdio::internal::ValidateAndProcessJson(i4Spec).value();
  auto [i4Store, i4Metadata] = i4Schema;
  auto i4 =
      mdio::internal::CreateVariable(i4Store, i4Metadata, std::move(options));
  ASSERT_TRUE(i4.status().ok()) << i4.status();

  // Create i8 (int64)
  auto i8Spec = CreateTypedVariableSpec(version_, "i8", base_path_, "<i8",
                                        "int64", "8-byte integer test");
  auto i8Schema = mdio::internal::ValidateAndProcessJson(i8Spec).value();
  auto [i8Store, i8Metadata] = i8Schema;
  auto i8 =
      mdio::internal::CreateVariable(i8Store, i8Metadata, std::move(options));
  ASSERT_TRUE(i8.status().ok()) << i8.status();

  // Create f2 (float16)
  auto f2Spec = CreateTypedVariableSpec(version_, "f2", base_path_, "<f2",
                                        "float16", "2-byte float test");
  auto f2Schema = mdio::internal::ValidateAndProcessJson(f2Spec).value();
  auto [f2Store, f2Metadata] = f2Schema;
  auto f2 =
      mdio::internal::CreateVariable(f2Store, f2Metadata, std::move(options));
  ASSERT_TRUE(f2.status().ok()) << f2.status();

  // Create f4 (float32)
  auto f4Spec = CreateTypedVariableSpec(version_, "f4", base_path_, "<f4",
                                        "float32", "4-byte float test");
  auto f4Schema = mdio::internal::ValidateAndProcessJson(f4Spec).value();
  auto [f4Store, f4Metadata] = f4Schema;
  auto f4 =
      mdio::internal::CreateVariable(f4Store, f4Metadata, std::move(options));
  ASSERT_TRUE(f4.status().ok()) << f4.status();

  // Create f8 (float64)
  auto f8Spec = CreateTypedVariableSpec(version_, "f8", base_path_, "<f8",
                                        "float64", "8-byte float test");
  auto f8Schema = mdio::internal::ValidateAndProcessJson(f8Spec).value();
  auto [f8Store, f8Metadata] = f8Schema;
  auto f8 =
      mdio::internal::CreateVariable(f8Store, f8Metadata, std::move(options));
  ASSERT_TRUE(f8.status().ok()) << f8.status();

  // Create u1 (uint8)
  auto u1Spec =
      CreateTypedVariableSpec(version_, "u1", base_path_, "<u1", "uint8",
                              "1-byte unsigned integer test");
  auto u1Schema = mdio::internal::ValidateAndProcessJson(u1Spec).value();
  auto [u1Store, u1Metadata] = u1Schema;
  auto u1 =
      mdio::internal::CreateVariable(u1Store, u1Metadata, std::move(options));
  ASSERT_TRUE(u1.status().ok()) << u1.status();

  // Create u2 (uint16)
  auto u2Spec =
      CreateTypedVariableSpec(version_, "u2", base_path_, "<u2", "uint16",
                              "2-byte unsigned integer test");
  auto u2Schema = mdio::internal::ValidateAndProcessJson(u2Spec).value();
  auto [u2Store, u2Metadata] = u2Schema;
  auto u2 =
      mdio::internal::CreateVariable(u2Store, u2Metadata, std::move(options));
  ASSERT_TRUE(u2.status().ok()) << u2.status();

  // Create u8 (uint64)
  auto u8Spec =
      CreateTypedVariableSpec(version_, "u8", base_path_, "<u8", "uint64",
                              "8-byte unsigned integer test");
  auto u8Schema = mdio::internal::ValidateAndProcessJson(u8Spec).value();
  auto [u8Store, u8Metadata] = u8Schema;
  auto u8 =
      mdio::internal::CreateVariable(u8Store, u8Metadata, std::move(options));
  ASSERT_TRUE(u8.status().ok()) << u8.status();

  // Create b1 (bool)
  auto b1Spec = CreateTypedVariableSpec(version_, "b1", base_path_, "|b1",
                                        "bool", "boolean test");
  auto b1Schema = mdio::internal::ValidateAndProcessJson(b1Spec).value();
  auto [b1Store, b1Metadata] = b1Schema;
  auto b1 =
      mdio::internal::CreateVariable(b1Store, b1Metadata, std::move(options));
  ASSERT_TRUE(b1.status().ok()) << b1.status();
}

TEST_P(VariableTest, open) {
  auto i2Base = CreateBaseSpec(version_, "i2", base_path_);
  auto i4Base = CreateBaseSpec(version_, "i4", base_path_);
  auto i8Base = CreateBaseSpec(version_, "i8", base_path_);
  auto f2Base = CreateBaseSpec(version_, "f2", base_path_);
  auto f4Base = CreateBaseSpec(version_, "f4", base_path_);
  auto f8Base = CreateBaseSpec(version_, "f8", base_path_);

  EXPECT_TRUE(
      mdio::Variable<>::Open(i2Base, mdio::constants::kOpen).status().ok());
  EXPECT_TRUE(
      mdio::Variable<>::Open(i4Base, mdio::constants::kOpen).status().ok());
  EXPECT_TRUE(
      mdio::Variable<>::Open(i8Base, mdio::constants::kOpen).status().ok());
  EXPECT_TRUE(
      mdio::Variable<>::Open(f2Base, mdio::constants::kOpen).status().ok());
  EXPECT_TRUE(
      mdio::Variable<>::Open(f4Base, mdio::constants::kOpen).status().ok());
  EXPECT_TRUE(
      mdio::Variable<>::Open(f8Base, mdio::constants::kOpen).status().ok());
}

TEST_P(VariableTest, name) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  auto i4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i4", base_path_),
                                   mdio::constants::kOpen);
  auto i8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i8", base_path_),
                                   mdio::constants::kOpen);
  auto f2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f2", base_path_),
                                   mdio::constants::kOpen);
  auto f4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f4", base_path_),
                                   mdio::constants::kOpen);
  auto f8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f8", base_path_),
                                   mdio::constants::kOpen);

  ASSERT_TRUE(i2.status().ok()) << i2.status();
  ASSERT_TRUE(i4.status().ok()) << i4.status();
  ASSERT_TRUE(i8.status().ok()) << i8.status();
  ASSERT_TRUE(f2.status().ok()) << f2.status();
  ASSERT_TRUE(f4.status().ok()) << f4.status();
  ASSERT_TRUE(f8.status().ok()) << f8.status();

  EXPECT_EQ(i2.value().get_variable_name(), "i2");
  EXPECT_EQ(i4.value().get_variable_name(), "i4");
  EXPECT_EQ(i8.value().get_variable_name(), "i8");
  EXPECT_EQ(f2.value().get_variable_name(), "f2");
  EXPECT_EQ(f4.value().get_variable_name(), "f4");
  EXPECT_EQ(f8.value().get_variable_name(), "f8");
}

TEST_P(VariableTest, longName) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  auto i4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i4", base_path_),
                                   mdio::constants::kOpen);
  auto i8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i8", base_path_),
                                   mdio::constants::kOpen);
  auto f2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f2", base_path_),
                                   mdio::constants::kOpen);
  auto f4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f4", base_path_),
                                   mdio::constants::kOpen);
  auto f8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f8", base_path_),
                                   mdio::constants::kOpen);

  ASSERT_TRUE(i2.status().ok()) << i2.status();
  ASSERT_TRUE(i4.status().ok()) << i4.status();
  ASSERT_TRUE(i8.status().ok()) << i8.status();
  ASSERT_TRUE(f2.status().ok()) << f2.status();
  ASSERT_TRUE(f4.status().ok()) << f4.status();
  ASSERT_TRUE(f8.status().ok()) << f8.status();

  EXPECT_EQ(i2.value().get_long_name(), "2-byte integer test");
  EXPECT_EQ(i4.value().get_long_name(), "4-byte integer test");
  EXPECT_EQ(i8.value().get_long_name(), "8-byte integer test");
  EXPECT_EQ(f2.value().get_long_name(), "2-byte float test");
  EXPECT_EQ(f4.value().get_long_name(), "4-byte float test");
  EXPECT_EQ(f8.value().get_long_name(), "8-byte float test");
}

TEST_P(VariableTest, optionalAttrs) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_EQ(i2.value().GetAttributes()["attributes"]["foo"], "bar");
}

TEST_P(VariableTest, namedDimensions) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_EQ(i2.value().getMetadata()["dimension_names"].size(), 2);
}

TEST_P(VariableTest, sliceByDimIdx) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  auto f4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f4", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  ASSERT_TRUE(f4.status().ok()) << f4.status();

  mdio::RangeDescriptor<mdio::Index> zeroIdxSlice = {0, 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> oneIdxSlice = {1, 0, 5, 1};

  auto i2Slice = i2.value().slice(zeroIdxSlice, oneIdxSlice);
  auto f4Slice = f4.value().slice(zeroIdxSlice, oneIdxSlice);

  EXPECT_TRUE(i2Slice.status().ok()) << i2Slice.status();
  EXPECT_TRUE(f4Slice.status().ok()) << f4Slice.status();
  EXPECT_THAT(i2Slice.value().dimensions().shape(),
              ::testing::ElementsAre(5, 5));
  EXPECT_THAT(f4Slice.value().dimensions().shape(),
              ::testing::ElementsAre(5, 5));
}

TEST_P(VariableTest, sliceByDimName) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();

  mdio::RangeDescriptor<mdio::Index> inlineSlice = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> crosslineSpec = {"crossline", 0, 5, 1};
  auto i2Slice = i2.value().slice(inlineSlice, crosslineSpec);

  EXPECT_TRUE(i2Slice.status().ok()) << i2Slice.status();
  EXPECT_THAT(i2Slice.value().dimensions().shape(),
              ::testing::ElementsAre(5, 5));
}

TEST_P(VariableTest, dimensionUnits) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_TRUE(i2.value().getMetadata().contains("dimension_units"));
}

TEST_P(VariableTest, chunkSize) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_TRUE(i2.value().get_chunk_shape().status().ok());
}

TEST_P(VariableTest, shape) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_THAT(i2.value().dimensions().shape(), ::testing::ElementsAre(10, 10));
}

TEST_P(VariableTest, dtype) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  auto i4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i4", base_path_),
                                   mdio::constants::kOpen);
  auto i8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i8", base_path_),
                                   mdio::constants::kOpen);
  auto f2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f2", base_path_),
                                   mdio::constants::kOpen);
  auto f4 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f4", base_path_),
                                   mdio::constants::kOpen);
  auto f8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "f8", base_path_),
                                   mdio::constants::kOpen);
  auto u1 = mdio::Variable<>::Open(CreateBaseSpec(version_, "u1", base_path_),
                                   mdio::constants::kOpen);
  auto u2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "u2", base_path_),
                                   mdio::constants::kOpen);
  auto u8 = mdio::Variable<>::Open(CreateBaseSpec(version_, "u8", base_path_),
                                   mdio::constants::kOpen);
  auto b1 = mdio::Variable<>::Open(CreateBaseSpec(version_, "b1", base_path_),
                                   mdio::constants::kOpen);

  ASSERT_TRUE(i2.status().ok()) << i2.status();
  ASSERT_TRUE(i4.status().ok()) << i4.status();
  ASSERT_TRUE(i8.status().ok()) << i8.status();
  ASSERT_TRUE(f2.status().ok()) << f2.status();
  ASSERT_TRUE(f4.status().ok()) << f4.status();
  ASSERT_TRUE(f8.status().ok()) << f8.status();
  ASSERT_TRUE(u1.status().ok()) << u1.status();
  ASSERT_TRUE(u2.status().ok()) << u2.status();
  ASSERT_TRUE(u8.status().ok()) << u8.status();
  ASSERT_TRUE(b1.status().ok()) << b1.status();

  EXPECT_EQ(i2.value().dtype(), mdio::constants::kInt16);
  EXPECT_EQ(i4.value().dtype(), mdio::constants::kInt32);
  EXPECT_EQ(i8.value().dtype(), mdio::constants::kInt64);
  EXPECT_EQ(f2.value().dtype(), mdio::constants::kFloat16);
  EXPECT_EQ(f4.value().dtype(), mdio::constants::kFloat32);
  EXPECT_EQ(f8.value().dtype(), mdio::constants::kFloat64);
  EXPECT_EQ(u1.value().dtype(), mdio::constants::kUint8);
  EXPECT_EQ(u2.value().dtype(), mdio::constants::kUint16);
  EXPECT_EQ(u8.value().dtype(), mdio::constants::kUint64);
  EXPECT_EQ(b1.value().dtype(), mdio::constants::kBool);
}

TEST_P(VariableTest, domain) {
  auto i2 = mdio::Variable<>::Open(CreateBaseSpec(version_, "i2", base_path_),
                                   mdio::constants::kOpen);
  ASSERT_TRUE(i2.status().ok()) << i2.status();

  const mdio::Index EXPECTED_SHAPE = 10;
  EXPECT_EQ(i2.value().dimensions().shape()[0], EXPECTED_SHAPE);
  EXPECT_EQ(i2.value().dimensions().rank(), 2);
}

TEST_P(VariableTest, TEARDOWN) {
  std::filesystem::remove_all(base_path_ + "/i2");
  std::filesystem::remove_all(base_path_ + "/i4");
  std::filesystem::remove_all(base_path_ + "/i8");
  std::filesystem::remove_all(base_path_ + "/f2");
  std::filesystem::remove_all(base_path_ + "/f4");
  std::filesystem::remove_all(base_path_ + "/f8");
  std::filesystem::remove_all(base_path_ + "/u1");
  std::filesystem::remove_all(base_path_ + "/u2");
  std::filesystem::remove_all(base_path_ + "/u8");
  std::filesystem::remove_all(base_path_ + "/b1");
  ASSERT_TRUE(true);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, VariableTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

// ============================================================================
// Parameterized VariableData Tests
// ============================================================================

class VariableDataTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
  }

  mdio::Variable<> getVariable() {
    auto spec = CreateBaseSpec(version_, "i2", base_path_);
    auto var = mdio::Variable<>::Open(spec, mdio::constants::kOpen);
    if (!var.status().ok()) {
      std::cout << "Error opening i2: " << var.status() << std::endl;
      return mdio::Variable<>();
    }
    return var.value();
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(VariableDataTest, SETUP) {
  // Ensure directory exists
  std::filesystem::create_directories(base_path_);

  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  auto i2Spec = CreateTypedVariableSpec(version_, "i2", base_path_, "<i2",
                                        "int16", "2-byte integer test");
  auto i2Schema = mdio::internal::ValidateAndProcessJson(i2Spec).value();
  auto [i2Store, i2Metadata] = i2Schema;
  auto i2 =
      mdio::internal::CreateVariable(i2Store, i2Metadata, std::move(options));
  ASSERT_TRUE(i2.status().ok()) << i2.status();
}

TEST_P(VariableDataTest, name) {
  auto variableData = getVariable().Read().value();
  EXPECT_EQ(variableData.variableName, "i2");
}

TEST_P(VariableDataTest, longName) {
  auto variableData = getVariable().Read().value();
  EXPECT_EQ(variableData.longName, "2-byte integer test");
}

TEST_P(VariableDataTest, optionalAttrs) {
  auto variableData = getVariable().Read().value();
  EXPECT_EQ(variableData.metadata["metadata"]["attributes"]["foo"], "bar");
}

TEST_P(VariableDataTest, namedDimensions) {
  auto variableData = getVariable().Read().value();
  EXPECT_EQ(variableData.metadata["dimension_names"].size(), 2);
}

TEST_P(VariableDataTest, sliceByDimIdx) {
  auto variableData = getVariable().Read().value();
  mdio::RangeDescriptor<mdio::Index> zeroIdxSlice = {0, 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> oneIdxSlice = {1, 0, 5, 1};
  auto slicedVariableData = variableData.slice(zeroIdxSlice, oneIdxSlice);
  ASSERT_TRUE(slicedVariableData.status().ok()) << slicedVariableData.status();
  EXPECT_THAT(slicedVariableData.value().domain().shape(),
              ::testing::ElementsAre(5, 5));
}

TEST_P(VariableDataTest, sliceByDimName) {
  auto variableData = getVariable().Read().value();
  mdio::RangeDescriptor<mdio::Index> inlineSlice = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> crosslineSpec = {"crossline", 0, 5, 1};
  auto slicedVariableData = variableData.slice(inlineSlice, crosslineSpec);
  ASSERT_TRUE(slicedVariableData.status().ok()) << slicedVariableData.status();
  EXPECT_THAT(slicedVariableData.value().domain().shape(),
              ::testing::ElementsAre(5, 5));
}

TEST_P(VariableDataTest, writeToStore) {
  auto variable = getVariable();
  auto variableData = variable.Read().value();
  auto data =
      reinterpret_cast<int16_t*>(variableData.get_data_accessor().data());
  data[0] = 0xff;
  auto writeFuture = variable.Write(variableData);
  writeFuture.result();
  EXPECT_TRUE(writeFuture.status().ok()) << writeFuture.status();

  auto variableCheck = getVariable().Read().value();
  auto dataCheck =
      reinterpret_cast<int16_t*>(variableCheck.get_data_accessor().data());
  EXPECT_EQ(dataCheck[0], 0xff);
}

TEST_P(VariableDataTest, dimensionUnits) {
  auto variableData = getVariable().Read().value();
  EXPECT_EQ(variableData.metadata["dimension_units"].size(), 2);
}

TEST_P(VariableDataTest, TEARDOWN) {
  std::filesystem::remove_all(base_path_ + "/i2");
  ASSERT_TRUE(true);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, VariableDataTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

// ============================================================================
// Parameterized Dataset Tests
// ============================================================================

class DatasetTest : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
    dataset_manifest_ = GetDatasetManifest(version_);
    expected_var_count_ = version_ == mdio::zarr::ZarrVersion::kV3
                              ? 8
                              : 9;  // V2 has struct array
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
  std::string dataset_manifest_;
  size_t expected_var_count_;
};

TEST_P(DatasetTest, specValid) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto res = Construct(j, base_path_, version_);
  ASSERT_TRUE(res.status().ok()) << res.status();
  std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = res.value();
  std::vector<nlohmann::json> variables = std::get<1>(parsed);
  EXPECT_EQ(variables.size(), expected_var_count_);

  // Verify driver name
  std::string expected_driver = GetTestDriverName(version_);
  for (const auto& var : variables) {
    EXPECT_EQ(var["driver"], expected_driver);
  }
}

TEST_P(DatasetTest, open) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST_P(DatasetTest, condensed) {
  // First create the dataset
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto createDs =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kCreateClean);
  ASSERT_TRUE(createDs.status().ok()) << createDs.status();

  // Now test opening with just the path
  auto ds = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(ds.status().ok())
      << "Failed to open with trailing slash: " << ds.status();

  // Test without trailing slash
  std::string path_no_slash = base_path_;
  if (!path_no_slash.empty() && path_no_slash.back() == '/') {
    path_no_slash.pop_back();
  }
  auto ds2 = mdio::Dataset::Open(path_no_slash, mdio::constants::kOpen);
  ASSERT_TRUE(ds2.status().ok())
      << "Failed to open without trailing slash: " << ds2.status();
}

TEST_P(DatasetTest, read) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  for (auto& kv : ds.coordinates) {
    std::string key = kv.first;
    auto var = ds.get_variable(key);
    ASSERT_TRUE(var.status().ok()) << var.status();
  }
}

TEST_P(DatasetTest, write) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  std::vector<std::string> names = ds.variables.get_keys();
  std::vector<mdio::Variable<>> openVariables;
  for (auto& key : names) {
    auto var = ds.get_variable(key);
    openVariables.emplace_back(var.value());
  }

  std::vector<mdio::Future<mdio::VariableData<>>> readVariablesFutures;
  for (auto& v : openVariables) {
    readVariablesFutures.emplace_back(v.Read());
  }

  std::vector<mdio::VariableData<>> readVariables;
  for (auto& v : readVariablesFutures) {
    ASSERT_TRUE(v.status().ok()) << v.status();
    readVariables.emplace_back(v.value());
  }

  // Modify some data
  for (auto& variable : readVariables) {
    std::string name = variable.variableName;
    mdio::DataType dtype = variable.dtype();
    if (dtype == mdio::constants::kFloat32 && name == "image") {
      auto data = reinterpret_cast<float*>(variable.get_data_accessor().data());
      data[0] = 3.14f;
    } else if (dtype == mdio::constants::kFloat64 && name == "velocity") {
      auto data =
          reinterpret_cast<double*>(variable.get_data_accessor().data());
      data[0] = 2.71828;
    } else if (dtype == mdio::constants::kInt16 && name == "image_inline") {
      auto data =
          reinterpret_cast<int16_t*>(variable.get_data_accessor().data());
      data[0] = 0xff;
    } else if (name == "inline") {
      auto data =
          reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
      for (uint32_t i = 0; i < 256; ++i) {
        data[i] = i;
      }
    } else if (name == "crossline") {
      auto data =
          reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
      for (uint32_t i = 0; i < 512; ++i) {
        data[i] = i;
      }
    } else if (name == "depth") {
      auto data =
          reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
      for (uint32_t i = 0; i < 384; ++i) {
        data[i] = i;
      }
    }
  }

  // Pair and write
  std::map<std::size_t, std::size_t> variableIdxPair;
  for (std::size_t i = 0; i < openVariables.size(); i++) {
    for (std::size_t j = 0; j < readVariables.size(); j++) {
      if (openVariables[i].get_variable_name() ==
          readVariables[j].variableName) {
        variableIdxPair[i] = j;
        break;
      }
    }
  }

  std::vector<mdio::WriteFutures> writeFutures;
  for (auto& idxPair : variableIdxPair) {
    writeFutures.emplace_back(
        openVariables[idxPair.second].Write(readVariables[idxPair.first]));
  }

  for (auto& w : writeFutures) {
    ASSERT_TRUE(w.status().ok()) << w.status();
  }

  // Verify writes
  std::string driver = GetTestDriverName(version_);
  nlohmann::json imageJson;
  imageJson["driver"] = driver;
  imageJson["kvstore"]["driver"] = "file";
  imageJson["kvstore"]["path"] = base_path_ + "/image";

  auto image = mdio::Variable<>::Open(imageJson, mdio::constants::kOpen);
  ASSERT_TRUE(image.status().ok()) << image.status();

  auto imageData = image.result()->Read();
  ASSERT_TRUE(imageData.status().ok()) << imageData.status();
  auto castedImage =
      reinterpret_cast<float*>(imageData.value().get_data_accessor().data());
  EXPECT_EQ(castedImage[0], 3.14f);
}

TEST_P(DatasetTest, name) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  std::string expected_name =
      version_ == mdio::zarr::ZarrVersion::kV3 ? "campos_3d_v3" : "campos_3d";
  EXPECT_EQ(dataset.value().getMetadata()["name"], expected_name);
}

TEST_P(DatasetTest, optionalAttrs) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  EXPECT_TRUE(dataset.value().getMetadata().contains("name"));
}

TEST_P(DatasetTest, isel) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 5, 1};
  auto slice = ds.isel(desc1);
  ASSERT_TRUE(slice.status().ok()) << slice.status();

  auto domain = slice->domain;
  ASSERT_EQ(domain.rank(), 3) << "This should have a rank of 3...";

  auto depthRange = domain[1];
  EXPECT_EQ(depthRange.interval().inclusive_min(), 0);
  EXPECT_EQ(depthRange.interval().exclusive_max(), 384);

  auto crosslineRange = domain[0];
  EXPECT_EQ(crosslineRange.interval().inclusive_min(), 0);
  EXPECT_EQ(crosslineRange.interval().exclusive_max(), 512);

  auto inlineRange = domain[2];
  EXPECT_EQ(inlineRange.interval().inclusive_min(), 0);
  EXPECT_EQ(inlineRange.interval().exclusive_max(), 5);
}

TEST_P(DatasetTest, listVars) {
  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto construct = Construct(j, base_path_, version_);
  ASSERT_TRUE(construct.status().ok()) << construct.status();

  auto [metadata, variables] = construct.value();
  auto dataset =
      mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  std::vector<std::string> varList = dataset.value().variables.get_keys();
  EXPECT_EQ(varList.size(), expected_var_count_);
}

TEST_P(DatasetTest, fromJson) {
  std::filesystem::remove_all(base_path_ + "/from_json_test");

  nlohmann::json j = nlohmann::json::parse(dataset_manifest_);
  auto dataset =
      mdio::Dataset::from_json(j, base_path_ + "/from_json_test", version_,
                               mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  std::vector<std::string> varList = dataset.value().variables.get_keys();
  EXPECT_EQ(varList.size(), expected_var_count_);

  std::filesystem::remove_all(base_path_ + "/from_json_test");
}

TEST_P(DatasetTest, TEARDOWN) {
  std::filesystem::remove_all(base_path_);
  ASSERT_TRUE(true);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, DatasetTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

// ============================================================================
// V2-Only Tests (Struct Arrays)
// ============================================================================

namespace V2OnlyTests {

TEST(VariableV2Only, structArraySetup) {
  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  nlohmann::json voidedSpec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/voided"
            },
            "field": "integer16",
            "metadata": {
                "dtype": [["integer16", "<i2"], ["float32", "<f4"], ["double", "<f8"]],
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "struct array test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
  auto voidedSchema =
      mdio::internal::ValidateAndProcessJson(voidedSpec).value();
  auto [voidedStore, voidedMetadata] = voidedSchema;
  auto voided = mdio::internal::CreateVariable(voidedStore, voidedMetadata,
                                               std::move(options));
  ASSERT_TRUE(voided.status().ok()) << voided.status();
}

TEST(VariableV2Only, structArrayOpen) {
  nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

  EXPECT_TRUE(
      mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen).status().ok());
}

TEST(VariableV2Only, structArrayName) {
  nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

  auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
  ASSERT_TRUE(voided.status().ok()) << voided.status();
  EXPECT_EQ(voided.value().get_variable_name(), "voided");
  EXPECT_EQ(voided.value().get_long_name(), "struct array test");
}

TEST(VariableV2Only, structArrayDtype) {
  nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

  auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
  ASSERT_TRUE(voided.status().ok()) << voided.status();
  EXPECT_EQ(voided.value().dtype(), mdio::constants::kByte);
}

TEST(VariableV2Only, structArrayShape) {
  nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

  auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
  ASSERT_TRUE(voided.status().ok()) << voided.status();
  EXPECT_THAT(voided.value().dimensions().shape(),
              ::testing::ElementsAre(10, 10, 14));
  EXPECT_EQ(voided.value().dimensions().rank(), 3);
}

TEST(VariableV2Only, structArraySlice) {
  nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

  auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
  ASSERT_TRUE(voided.status().ok()) << voided.status();

  mdio::RangeDescriptor<mdio::Index> zeroIdxSlice = {0, 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> oneIdxSlice = {1, 0, 5, 1};
  auto voidedSlice = voided.value().slice(zeroIdxSlice, oneIdxSlice);
  EXPECT_TRUE(voidedSlice.status().ok()) << voidedSlice.status();
  EXPECT_THAT(voidedSlice.value().dimensions().shape(),
              ::testing::ElementsAre(5, 5, 14));
}

TEST(VariableV2Only, structArrayTeardown) {
  std::filesystem::remove_all("zarrs/acceptance/voided");
  ASSERT_TRUE(true);
}

TEST(DatasetV2Only, selectField) {
  // Create dataset first
  std::string manifest = R"(
{
  "metadata": {
    "name": "select_field_test",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00"
  },
  "variables": [
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
      "dimensions": [
        {"name": "inline", "size": 128},
        {"name": "crossline", "size": 128}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [64, 64] }
        }
      },
      "coordinates": ["inline", "crossline"]
    },
    {
      "name": "inline",
      "dataType": "uint32",
      "dimensions": [{"name": "inline", "size": 128}]
    },
    {
      "name": "crossline",
      "dataType": "uint32",
      "dimensions": [{"name": "crossline", "size": 128}]
    }
  ]
}
  )";

  std::filesystem::remove_all("zarrs/select_field_test");
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto dataset = mdio::Dataset::from_json(j, "zarrs/select_field_test",
                                          mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();
  std::string name = "image_headers";

  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kByte);
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 3);

  EXPECT_TRUE(ds.SelectField("image_headers", "cdp-x").status().ok());
  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kInt32);
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 2);

  EXPECT_TRUE(ds.SelectField("image_headers", "elevation").status().ok());
  EXPECT_TRUE(ds.get_variable(name).value().dtype() ==
              mdio::constants::kFloat16);
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 2);

  EXPECT_TRUE(ds.SelectField("image_headers", "").status().ok());
  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kByte);
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 3);

  EXPECT_FALSE(ds.SelectField("image_headers", "NotAField").status().ok());
  EXPECT_FALSE(ds.SelectField("NotAVariable", "NotAField").status().ok());

  std::filesystem::remove_all("zarrs/select_field_test");
}

TEST(DatasetV2Only, fillValue) {
  std::string manifest = R"(
{
  "metadata": {
    "name": "fill_value_test",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00"
  },
  "variables": [
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
      "dimensions": [
        {"name": "inline", "size": 256},
        {"name": "crossline", "size": 512}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128] }
        }
      }
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
    }
  ]
}
  )";

  std::filesystem::remove_all("zarrs/fill_value_test");
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto ds = mdio::Dataset::from_json(j, "zarrs/fill_value_test",
                                     mdio::constants::kCreateClean);
  ASSERT_TRUE(ds.status().ok()) << ds.status();

  std::string key = "image_headers";
  auto var = ds.value().get_variable(key);
  ASSERT_TRUE(var.status().ok()) << var.status();
  auto vdf = var.value().Read();
  ASSERT_TRUE(vdf.status().ok()) << vdf.status();
  auto vd = vdf.value();

  auto data =
      reinterpret_cast<mdio::dtypes::byte_t*>(vd.get_data_accessor().data());
  std::byte zero = std::byte(0);
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(data[i], zero) << "Expected 0 at byte " << i;
  }

  std::filesystem::remove_all("zarrs/fill_value_test");
}

}  // namespace V2OnlyTests

// ============================================================================
// V2 Compatibility Tests (Python)
// ============================================================================

namespace V2CompatibilityTests {

TEST(VariableV2Compat, zarrCompatibility) {
  // First setup variables for testing
  std::filesystem::remove_all("zarrs/compat_test");
  std::filesystem::create_directories("zarrs/compat_test");

  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  std::vector<std::tuple<std::string, std::string, std::string>> vars = {
      {"i2", "<i2", "2-byte integer"}, {"i4", "<i4", "4-byte integer"},
      {"i8", "<i8", "8-byte integer"}, {"f2", "<f2", "2-byte float"},
      {"f4", "<f4", "4-byte float"},   {"f8", "<f8", "8-byte float"},
  };

  for (const auto& [name, dtype, long_name] : vars) {
    nlohmann::json spec;
    spec["driver"] = "zarr";
    spec["kvstore"]["driver"] = "file";
    spec["kvstore"]["path"] = "zarrs/compat_test/" + name;
    spec["metadata"]["dtype"] = dtype;
    spec["metadata"]["shape"] = nlohmann::json::array({10, 10});
    spec["metadata"]["chunks"] = nlohmann::json::array({5, 5});
    spec["metadata"]["dimension_separator"] = "/";
    spec["metadata"]["compressor"]["id"] = "blosc";
    spec["attributes"]["metadata"]["attributes"]["foo"] = "bar";
    spec["attributes"]["long_name"] = long_name;
    spec["attributes"]["dimension_names"] =
        nlohmann::json::array({"inline", "crossline"});
    spec["attributes"]["dimension_units"] = nlohmann::json::array({"m", "m"});

    auto schema = mdio::internal::ValidateAndProcessJson(spec).value();
    auto [store, metadata] = schema;
    auto var =
        mdio::internal::CreateVariable(store, metadata, std::move(options));
    ASSERT_TRUE(var.status().ok()) << var.status();
  }

  const char* basePath = std::getenv(PROJECT_BASE_PATH_ENV);
  if (!basePath) {
    std::cout << "PROJECT_BASE_PATH environment variable not set. Expecting to "
                 "be in the 'build/mdio' directory."
              << std::endl;
    basePath = DEFAULT_BASE_PATH;
  }

  std::string srcPath = std::string(basePath) + ZARR_SCRIPT_RELATIVE_PATH;

  if (access(srcPath.c_str(), F_OK) == -1) {
    std::cerr << "Error: Python script not found at " << srcPath << std::endl;
    FAIL() << "Script not found: " << srcPath;
  }

  std::vector<std::string> args = {"i2", "i4", "i8", "f2", "f4", "f8"};
  std::vector<pid_t> pids;

  for (const auto& arg : args) {
    pid_t pid = fork();
    if (pid == 0) {
      int result = executePythonScript(srcPath, {"zarrs/compat_test/" + arg});
      if (result == 0xfd00) {
        GTEST_SKIP() << "Zarr compatibility skipped due to import error";
        exit(SUCCESS_CODE);
      }
      exit(result);
    } else if (pid > 0) {
      pids.push_back(pid);
    } else {
      perror("fork failed");
      FAIL() << "fork failed";
    }
  }

  for (pid_t pid : pids) {
    int status;
    if (waitpid(pid, &status, 0) == -1) {
      perror("waitpid failed");
      FAIL() << "waitpid failed";
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0xfd00) {
      GTEST_SKIP() << "Zarr compatibility skipped due to import error";
    }
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "Failed to read one of the arguments";
  }

  std::filesystem::remove_all("zarrs/compat_test");
}

TEST(DatasetV2Compat, xarrayCompatible) {
  // First create a dataset
  std::string manifest = R"(
{
  "metadata": {
    "name": "xarray_compat_test",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00"
  },
  "variables": [
    {
      "name": "data",
      "dataType": "float32",
      "dimensions": [
        {"name": "x", "size": 10},
        {"name": "y", "size": 10}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [5, 5] }
        }
      }
    },
    {
      "name": "x",
      "dataType": "int32",
      "dimensions": [{"name": "x", "size": 10}]
    },
    {
      "name": "y",
      "dataType": "int32",
      "dimensions": [{"name": "y", "size": 10}]
    }
  ]
}
  )";

  std::filesystem::remove_all("zarrs/xarray_compat");
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto ds = mdio::Dataset::from_json(j, "zarrs/xarray_compat",
                                     mdio::constants::kCreateClean);
  ASSERT_TRUE(ds.status().ok()) << ds.status();

  const char* basePath = std::getenv(PROJECT_BASE_PATH_ENV);
  if (!basePath) {
    std::cout << "PROJECT_BASE_PATH environment variable not set. Expecting to "
                 "be in the 'build/mdio' directory."
              << std::endl;
    basePath = DEFAULT_BASE_PATH;
  }

  std::string srcPath = std::string(basePath) + XARRAY_SCRIPT_RELATIVE_PATH;

  if (access(srcPath.c_str(), F_OK) == -1) {
    std::cerr << "Error: Python script not found at " << srcPath << std::endl;
    std::filesystem::remove_all("zarrs/xarray_compat");
    FAIL() << "Script not found: " << srcPath;
  }

  std::vector<std::string> metadataOptions = {"False", "True"};
  std::vector<pid_t> pids;

  for (const auto& option : metadataOptions) {
    pid_t pid = fork();
    if (pid == 0) {
      int result =
          executePythonScript(srcPath, {"zarrs/xarray_compat/", option});
      if (result == 0xfd00) {
        GTEST_SKIP()
            << "Xarray compatibility skipped due to import error for xarray";
        exit(SUCCESS_CODE);
      }
      exit(result);
    } else if (pid > 0) {
      pids.push_back(pid);
    } else {
      perror("fork failed");
      FAIL() << "fork failed";
    }
  }

  for (pid_t pid : pids) {
    int status;
    if (waitpid(pid, &status, 0) == -1) {
      perror("waitpid failed");
      FAIL() << "waitpid failed";
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0xfd00) {
      GTEST_SKIP()
          << "Xarray compatibility skipped due to import error for xarray";
    }
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "xarray compatibility test failed";
  }

  std::filesystem::remove_all("zarrs/xarray_compat");
}

}  // namespace V2CompatibilityTests

// ============================================================================
// Dataset::from_json with Version Parameter Tests
// ============================================================================

class DatasetFromJsonTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_) + "/from_json";
    std::filesystem::remove_all(base_path_);
  }

  void TearDown() override { std::filesystem::remove_all(base_path_); }

  std::string GetSimpleManifest() {
    return R"(
{
  "metadata": {
    "name": "from_json_test",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00"
  },
  "variables": [
    {
      "name": "data",
      "dataType": "float32",
      "dimensions": [
        {"name": "x", "size": 32},
        {"name": "y", "size": 32}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [16, 16] }
        }
      }
    },
    {
      "name": "x",
      "dataType": "int32",
      "dimensions": [{"name": "x", "size": 32}]
    },
    {
      "name": "y",
      "dataType": "int32",
      "dimensions": [{"name": "y", "size": 32}]
    }
  ]
}
    )";
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(DatasetFromJsonTest, createWithExplicitVersion) {
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto dataset = mdio::Dataset::from_json(j, base_path_, version_,
                                          mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();
  EXPECT_EQ(ds.getMetadata()["name"], "from_json_test");

  std::vector<std::string> varList = ds.variables.get_keys();
  EXPECT_EQ(varList.size(), 3);
}

TEST_P(DatasetFromJsonTest, createWithOptionalVersion) {
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto dataset = mdio::Dataset::from_json(
      j, base_path_, std::optional<mdio::zarr::ZarrVersion>(version_),
      mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();
  EXPECT_EQ(ds.getMetadata()["name"], "from_json_test");
}

TEST_P(DatasetFromJsonTest, readWrite) {
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto datasetRes = mdio::Dataset::from_json(j, base_path_, version_,
                                             mdio::constants::kCreateClean);
  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto ds = datasetRes.value();

  auto dataVarRes = ds.variables.get<mdio::dtypes::float32_t>("data");
  ASSERT_TRUE(dataVarRes.status().ok()) << dataVarRes.status();
  auto dataVar = dataVarRes.value();

  auto dataRes = dataVar.Read();
  ASSERT_TRUE(dataRes.status().ok()) << dataRes.status();
  auto data = dataRes.value();

  auto accessor = data.get_data_accessor().data();
  accessor[0] = 42.0f;
  accessor[1] = 43.0f;

  auto writeFut = dataVar.Write(data);
  ASSERT_TRUE(writeFut.status().ok()) << writeFut.status();

  auto rereadFut = dataVar.Read();
  ASSERT_TRUE(rereadFut.status().ok()) << rereadFut.status();
  auto rereadData = rereadFut.value();
  auto rereadAccessor = rereadData.get_data_accessor().data();

  EXPECT_FLOAT_EQ(rereadAccessor[0], 42.0f);
  EXPECT_FLOAT_EQ(rereadAccessor[1], 43.0f);
}

TEST_P(DatasetFromJsonTest, isel) {
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto datasetRes = mdio::Dataset::from_json(j, base_path_, version_,
                                             mdio::constants::kCreateClean);
  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto ds = datasetRes.value();

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 10, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 15, 1};
  auto sliceRes = ds.isel(desc1, desc2);

  ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
  auto slice = sliceRes.value();

  auto domain = slice.domain;
  ASSERT_EQ(domain.rank(), 2);
}

TEST_P(DatasetFromJsonTest, intervals) {
  nlohmann::json j = nlohmann::json::parse(GetSimpleManifest());
  auto datasetRes = mdio::Dataset::from_json(j, base_path_, version_,
                                             mdio::constants::kCreateClean);
  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto ds = datasetRes.value();

  auto intervalRes = ds.get_intervals();
  ASSERT_TRUE(intervalRes.ok()) << intervalRes.status();
  auto intervals = intervalRes.value();

  EXPECT_GE(intervals.size(), 2);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, DatasetFromJsonTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

// Test nullopt version (should default to V2)
TEST(DatasetFromJsonNullopt, createWithNulloptVersion) {
  std::filesystem::remove_all("zarrs/from_json_nullopt");

  std::string manifest = R"(
{
  "metadata": {
    "name": "nullopt_test",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00"
  },
  "variables": [
    {
      "name": "data",
      "dataType": "float32",
      "dimensions": [{"name": "x", "size": 10}]
    },
    {
      "name": "x",
      "dataType": "int32",
      "dimensions": [{"name": "x", "size": 10}]
    }
  ]
}
  )";

  nlohmann::json j = nlohmann::json::parse(manifest);
  std::optional<mdio::zarr::ZarrVersion> version = std::nullopt;
  auto dataset = mdio::Dataset::from_json(j, "zarrs/from_json_nullopt", version,
                                          mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();
  EXPECT_EQ(ds.getMetadata()["name"], "nullopt_test");

  std::filesystem::remove_all("zarrs/from_json_nullopt");
}

}  // namespace
