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
#include <map>
#include <sstream>
#include <nlohmann/json.hpp>  // NOLINT

#include "mdio/dataset.h"
#include "mdio/dataset_factory.h"
#include "mdio/zarr/zarr.h"

namespace {

constexpr char PYTHON_EXECUTABLE[] = "python3";
constexpr char PROJECT_BASE_PATH_ENV[] = "PROJECT_BASE_PATH";
constexpr char DEFAULT_BASE_PATH[] = "../..";
constexpr char XARRAY_SCRIPT_RELATIVE_PATH[] =
    "/mdio/regression_tests/xarray_compatibility_test.py";
constexpr char MULTIDIMIO_SCRIPT_RELATIVE_PATH[] =
    "/mdio/regression_tests/multidimio_compatibility_test.py";
constexpr char FILL_VALUE_PARITY_SCRIPT_RELATIVE_PATH[] =
    "/mdio/regression_tests/fill_value_parity_test.py";
constexpr int ERROR_CODE = EXIT_FAILURE;
constexpr int SUCCESS_CODE = EXIT_SUCCESS;

using float16_t = mdio::dtypes::float_16_t;

/**
 * @brief Test variable definition with dtype info for V2 and V3.
 */
struct TestVariableDef {
  std::string name;
  std::string dtype_v2;
  std::string dtype_v3;
  std::string long_name;
  mdio::DataType expected_dtype;
};

// Common test variable definitions used across multiple tests
const std::vector<TestVariableDef> kTestVariables = {
    {"i2", "<i2", "int16", "2-byte integer test", mdio::constants::kInt16},
    {"i4", "<i4", "int32", "4-byte integer test", mdio::constants::kInt32},
    {"i8", "<i8", "int64", "8-byte integer test", mdio::constants::kInt64},
    {"f2", "<f2", "float16", "2-byte float test", mdio::constants::kFloat16},
    {"f4", "<f4", "float32", "4-byte float test", mdio::constants::kFloat32},
    {"f8", "<f8", "float64", "8-byte float test", mdio::constants::kFloat64},
    {"u1", "<u1", "uint8", "1-byte unsigned integer test",
     mdio::constants::kUint8},
    {"u2", "<u2", "uint16", "2-byte unsigned integer test",
     mdio::constants::kUint16},
    {"u8", "<u8", "uint64", "8-byte unsigned integer test",
     mdio::constants::kUint64},
    {"b1", "|b1", "bool", "boolean test", mdio::constants::kBool},
};

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
 * @brief Creates and asserts a test variable from a definition.
 */
void CreateTestVariable(const TestVariableDef& def,
                        mdio::zarr::ZarrVersion version,
                        const std::string& base_path) {
  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  auto spec = CreateTypedVariableSpec(
      version, def.name, base_path, def.dtype_v2, def.dtype_v3, def.long_name);
  auto schema = mdio::internal::ValidateAndProcessJson(spec).value();
  auto [store, metadata] = schema;
  auto var =
      mdio::internal::CreateVariable(store, metadata, std::move(options));
  ASSERT_TRUE(var.status().ok())
      << "Failed to create " << def.name << ": " << var.status();
}

/**
 * @brief Opens a variable by name and returns its Future.
 */
mdio::Future<mdio::Variable<>> OpenTestVariable(const std::string& name,
                                                mdio::zarr::ZarrVersion version,
                                                const std::string& base_path) {
  return mdio::Variable<>::Open(CreateBaseSpec(version, name, base_path),
                                mdio::constants::kOpen);
}

/**
 * @brief Gets the Python script base path from environment.
 */
const char* GetPythonBasePath() {
  const char* basePath = std::getenv(PROJECT_BASE_PATH_ENV);
  if (!basePath) {
    std::cout << "PROJECT_BASE_PATH environment variable not set. Expecting to "
                 "be in the 'build/mdio' directory."
              << std::endl;
    basePath = DEFAULT_BASE_PATH;
  }
  return basePath;
}

/**
 * @brief Returns the dataset manifest JSON for the given version.
 * Both versions use the same manifest structure including struct arrays.
 */
std::string GetDatasetManifest(mdio::zarr::ZarrVersion version) {
  std::string name =
      version == mdio::zarr::ZarrVersion::kV3 ? "campos_3d_v3" : "campos_3d";

  // clang-format off
  std::string manifest = R"(
{
  "metadata": {
    "name": ")" + name + R"(",
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
  // clang-format on
  return manifest;
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

/**
 * @brief Runs Python scripts with fork/wait and checks results.
 * @return true if all scripts passed, false otherwise.
 */
bool RunPythonScripts(const std::string& script_path,
                      const std::vector<std::vector<std::string>>& arg_sets,
                      const std::string& skip_message) {
  std::vector<pid_t> pids;

  for (const auto& args : arg_sets) {
    pid_t pid = fork();
    if (pid == 0) {
      int result = executePythonScript(script_path, args);
      if (result == 0xfd00) {
        exit(SUCCESS_CODE);
      }
      exit(result);
    } else if (pid > 0) {
      pids.push_back(pid);
    } else {
      perror("fork failed");
      return false;
    }
  }

  bool all_passed = true;
  for (pid_t pid : pids) {
    int status;
    if (waitpid(pid, &status, 0) == -1) {
      perror("waitpid failed");
      return false;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0xfd00) {
        // Import error - will be handled by caller
        continue;
      }
      all_passed = false;
    }
  }
  return all_passed;
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
  std::filesystem::create_directories(base_path_);

  for (const auto& def : kTestVariables) {
    CreateTestVariable(def, version_, base_path_);
  }
}

TEST_P(VariableTest, open) {
  for (const auto& def : kTestVariables) {
    auto var = OpenTestVariable(def.name, version_, base_path_);
    EXPECT_TRUE(var.status().ok())
        << "Failed to open " << def.name << ": " << var.status();
  }
}

TEST_P(VariableTest, name) {
  for (const auto& def : kTestVariables) {
    auto var = OpenTestVariable(def.name, version_, base_path_);
    ASSERT_TRUE(var.status().ok()) << var.status();
    EXPECT_EQ(var.value().get_variable_name(), def.name);
  }
}

TEST_P(VariableTest, longName) {
  for (const auto& def : kTestVariables) {
    auto var = OpenTestVariable(def.name, version_, base_path_);
    ASSERT_TRUE(var.status().ok()) << var.status();
    EXPECT_EQ(var.value().get_long_name(), def.long_name);
  }
}

TEST_P(VariableTest, optionalAttrs) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_EQ(i2.value().GetAttributes()["attributes"]["foo"], "bar");
}

TEST_P(VariableTest, namedDimensions) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_EQ(i2.value().getMetadata()["dimension_names"].size(), 2);
}

TEST_P(VariableTest, sliceByDimIdx) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  auto f4 = OpenTestVariable("f4", version_, base_path_);
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
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();

  mdio::RangeDescriptor<mdio::Index> inlineSlice = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> crosslineSpec = {"crossline", 0, 5, 1};
  auto i2Slice = i2.value().slice(inlineSlice, crosslineSpec);

  EXPECT_TRUE(i2Slice.status().ok()) << i2Slice.status();
  EXPECT_THAT(i2Slice.value().dimensions().shape(),
              ::testing::ElementsAre(5, 5));
}

TEST_P(VariableTest, dimensionUnits) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_TRUE(i2.value().getMetadata().contains("dimension_units"));
}

TEST_P(VariableTest, chunkSize) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_TRUE(i2.value().get_chunk_shape().status().ok());
}

TEST_P(VariableTest, shape) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();
  EXPECT_THAT(i2.value().dimensions().shape(), ::testing::ElementsAre(10, 10));
}

TEST_P(VariableTest, dtype) {
  for (const auto& def : kTestVariables) {
    auto var = OpenTestVariable(def.name, version_, base_path_);
    ASSERT_TRUE(var.status().ok()) << var.status();
    EXPECT_EQ(var.value().dtype(), def.expected_dtype)
        << "dtype mismatch for " << def.name;
  }
}

TEST_P(VariableTest, domain) {
  auto i2 = OpenTestVariable("i2", version_, base_path_);
  ASSERT_TRUE(i2.status().ok()) << i2.status();

  const mdio::Index EXPECTED_SHAPE = 10;
  EXPECT_EQ(i2.value().dimensions().shape()[0], EXPECTED_SHAPE);
  EXPECT_EQ(i2.value().dimensions().rank(), 2);
}

TEST_P(VariableTest, TEARDOWN) {
  for (const auto& def : kTestVariables) {
    std::filesystem::remove_all(base_path_ + "/" + def.name);
  }
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
    auto var = OpenTestVariable("i2", version_, base_path_);
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
  std::filesystem::create_directories(base_path_);
  // Only create i2 for this test class
  CreateTestVariable(kTestVariables[0], version_, base_path_);
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
    expected_var_count_ = 9;  // Both versions support struct arrays
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

  // Construct a vector of Variables to work with
  std::vector<mdio::Variable<>> openVariables;
  for (auto& key : names) {
    auto var = ds.get_variable(key);
    openVariables.emplace_back(var.value());
  }

  // Now we can start opening all the Variables
  std::vector<mdio::Future<mdio::VariableData<>>> readVariablesFutures;
  for (auto& v : openVariables) {
    auto read = v.Read();
    readVariablesFutures.emplace_back(read);
  }

  // Now we make sure all the reads were successful
  std::vector<mdio::VariableData<>> readVariables;
  for (auto& v : readVariablesFutures) {
    ASSERT_TRUE(v.status().ok()) << v.status();
    readVariables.emplace_back(v.value());
  }

  for (auto variable : readVariables) {
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
    } else if (dtype == mdio::constants::kByte && name == "image_headers") {
      auto data = reinterpret_cast<mdio::dtypes::byte_t*>(
          variable.get_data_accessor().data());
      for (int i = 0; i < 12; i++) {
        data[i] = std::byte(0xff);
      }
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

  // Pair the Variables to the VariableData objects via name matching so we can
  // write them out correctly This makes an assumption that the vectors are 1-1
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

  // Now we can write the Variables back to the store
  std::vector<mdio::WriteFutures> writeFutures;
  for (auto& idxPair : variableIdxPair) {
    auto write =
        openVariables[idxPair.second].Write(readVariables[idxPair.first]);
    writeFutures.emplace_back(write);
  }

  // Now we make sure all the writes were successful
  for (auto& w : writeFutures) {
    ASSERT_TRUE(w.status().ok()) << w.status();
  }

  // Test SelectField and negative case for struct arrays
  std::string fielded = "image_headers";
  ASSERT_TRUE(ds.SelectField(fielded, "cdp-x").status().ok());
  auto wf = ds.get_variable(fielded).value().Write(readVariables[4]);
  ASSERT_FALSE(wf.status().ok()) << wf.status();

  std::string driver = GetTestDriverName(version_);
  nlohmann::json imageJson;
  imageJson["driver"] = driver;
  imageJson["kvstore"]["driver"] = "file";
  imageJson["kvstore"]["path"] = base_path_ + "/image";

  nlohmann::json velocityJson;
  velocityJson["driver"] = driver;
  velocityJson["kvstore"]["driver"] = "file";
  velocityJson["kvstore"]["path"] = base_path_ + "/velocity";

  nlohmann::json imageInlineJson;
  imageInlineJson["driver"] = driver;
  imageInlineJson["kvstore"]["driver"] = "file";
  imageInlineJson["kvstore"]["path"] = base_path_ + "/image_inline";

  nlohmann::json imageHeadersJson;
  imageHeadersJson["driver"] = driver;
  imageHeadersJson["kvstore"]["driver"] = "file";
  imageHeadersJson["kvstore"]["path"] = base_path_ + "/image_headers";

  auto image = mdio::Variable<>::Open(imageJson, mdio::constants::kOpen);
  auto velocity = mdio::Variable<>::Open(velocityJson, mdio::constants::kOpen);
  auto imageInline =
      mdio::Variable<>::Open(imageInlineJson, mdio::constants::kOpen);
  auto imageHeaders =
      mdio::Variable<>::Open(imageHeadersJson, mdio::constants::kOpen);

  ASSERT_TRUE(image.status().ok()) << image.status();
  ASSERT_TRUE(velocity.status().ok()) << velocity.status();
  ASSERT_TRUE(imageInline.status().ok()) << imageInline.status();
  ASSERT_TRUE(imageHeaders.status().ok()) << imageHeaders.status();

  auto imageData = image.result()->Read();
  auto velocityData = velocity.result()->Read();
  auto imageInlineData = imageInline.result()->Read();
  auto imageHeadersData = imageHeaders.result()->Read();

  ASSERT_TRUE(imageData.status().ok()) << imageData.status();
  ASSERT_TRUE(velocityData.status().ok()) << velocityData.status();
  ASSERT_TRUE(imageInlineData.status().ok()) << imageInlineData.status();
  ASSERT_TRUE(imageHeadersData.status().ok()) << imageHeadersData.status();

  auto castedImage =
      reinterpret_cast<float*>(imageData.value().get_data_accessor().data());
  auto castedVelociy = reinterpret_cast<double*>(
      velocityData.value().get_data_accessor().data());
  auto castedImageInline = reinterpret_cast<int16_t*>(
      imageInlineData.value().get_data_accessor().data());

  EXPECT_EQ(castedImage[0], 3.14f) << castedImage[0];
  EXPECT_EQ(castedVelociy[0], 2.71828) << castedVelociy[0];
  EXPECT_EQ(castedImageInline[0], 0xff) << castedImageInline[0];
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

TEST_P(DatasetTest, selectField) {
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

  std::string test_path = base_path_ + "/select_field_test";
  std::filesystem::remove_all(test_path);
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto dataset = mdio::Dataset::from_json(j, test_path, version_,
                                          mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  auto ds = dataset.value();
  std::string name = "image_headers";

  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kByte)
      << "Failed to pull byte array from image_headers";
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 3)
      << "Expected structarray to be of rank 3";

  EXPECT_TRUE(ds.SelectField("image_headers", "cdp-x").status().ok())
      << "Failed to pull cdp-x from image_headers";
  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kInt32)
      << "Failed to pull int32 from image_headers";
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 2)
      << "Expected cdp-x to be of rank 2";

  EXPECT_TRUE(ds.SelectField("image_headers", "elevation").status().ok())
      << "Failed to pull elevation from image_headers";
  EXPECT_TRUE(ds.get_variable(name).value().dtype() ==
              mdio::constants::kFloat16)
      << "Failed to pull float16 from image_headers";
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 2)
      << "Expected elevation to be of rank 2";

  EXPECT_TRUE(ds.SelectField("image_headers", "").status().ok())
      << "Failed to set to byte array";
  EXPECT_TRUE(ds.get_variable(name).value().dtype() == mdio::constants::kByte)
      << "Failed to pull byte array from image_headers";
  EXPECT_EQ(ds.get_variable(name).value().get_store().rank(), 3)
      << "Expected structarray to be of rank 3";

  EXPECT_FALSE(ds.SelectField("image", "NotAField").status().ok())
      << "Somehow pulled NotAField from image";

  EXPECT_FALSE(ds.SelectField("NotAVariable", "NotAField").status().ok())
      << "Somehow pulled NotAField from NotAVariable";

  EXPECT_FALSE(ds.SelectField("image_headers", "NotAField").status().ok())
      << "Somehow pulled NotAField from image_headers";

  std::filesystem::remove_all(test_path);
}

TEST_P(DatasetTest, fillValue) {
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

  std::string test_path = base_path_ + "/fill_value_test";
  std::filesystem::remove_all(test_path);
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto ds = mdio::Dataset::from_json(j, test_path, version_,
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
    ASSERT_EQ(data[i], zero) << "Expected 0 at byte " << i << " but got "
                             << static_cast<int>(data[i]);
  }

  std::filesystem::remove_all(test_path);
}

namespace {

// Reads the "fill_value" from a Variable through the mdio API (its TensorStore
// spec) rather than poking at the Zarr metadata files directly.
nlohmann::json GetFillValueViaApi(const mdio::Variable<>& variable) {
  auto spec_result = variable.spec();
  if (!spec_result.status().ok()) {
    return nlohmann::json("__SPEC_ERROR__");
  }
  auto json_result = spec_result.value().ToJson();
  if (!json_result.ok()) {
    return nlohmann::json("__TOJSON_ERROR__");
  }
  const nlohmann::json& spec_json = json_result.value();
  if (spec_json.contains("metadata") &&
      spec_json["metadata"].contains("fill_value")) {
    return spec_json["metadata"]["fill_value"];
  }
  return nlohmann::json("__NO_FILL_VALUE__");
}

// Normalizes fill values so semantically-equal encodings compare equal.
// mdio-python materializes bool as int8, so its V3 bool fill is the integer 0,
// while mdio-cpp keeps a native bool fill (false). Mapping booleans to integers
// lets the two representations match.
nlohmann::json NormalizeFillValue(nlohmann::json fill_value) {
  if (fill_value.is_boolean()) {
    return fill_value.get<bool>() ? 1 : 0;
  }
  return fill_value;
}

}  // namespace

// Integration test: mdio-python is the source of truth for fill values. We ask
// mdio-python to build a dataset covering every supported scalar dtype, then
// build the equivalent dataset with mdio-cpp and assert the on-disk fill values
// match. Expectations are derived from mdio-python at runtime (never
// hardcoded), so the test tracks mdio-python automatically.
TEST_P(DatasetTest, fillValueParityWithPython) {
  const bool is_v3 = version_ == mdio::zarr::ZarrVersion::kV3;

  std::string srcPath =
      std::string(GetPythonBasePath()) + FILL_VALUE_PARITY_SCRIPT_RELATIVE_PATH;
  if (access(srcPath.c_str(), F_OK) == -1) {
    FAIL() << "Script not found: " << srcPath;
  }

  // 1. Generate the reference dataset with mdio-python.
  std::string python_path = base_path_ + "/fill_value_python";
  std::filesystem::remove_all(python_path);
  setenv("ZARR_DEFAULT_ZARR_FORMAT", is_v3 ? "3" : "2", /*overwrite=*/1);
  bool scripts_passed =
      RunPythonScripts(srcPath, {{python_path}},
                       "Fill value parity skipped due to import error");
  unsetenv("ZARR_DEFAULT_ZARR_FORMAT");
  ASSERT_TRUE(scripts_passed)
      << "mdio-python failed to generate the reference dataset";

  // 2. Open the reference dataset through the mdio API and read each dtype's
  //    fill value from its spec. Variables are named "v_<dtype>", so the dtype
  //    set is driven entirely by mdio-python.
  auto python_ds_future =
      mdio::Dataset::Open(python_path + "/", mdio::constants::kOpen);
  ASSERT_TRUE(python_ds_future.status().ok()) << python_ds_future.status();
  auto python_ds = python_ds_future.value();

  std::map<std::string, nlohmann::json> python_fill;  // dtype -> fill_value
  for (const auto& key : python_ds.variables.get_keys()) {
    if (key.rfind("v_", 0) != 0) {
      continue;
    }
    auto var = python_ds.variables.at(key);
    ASSERT_TRUE(var.status().ok()) << key << ": " << var.status();
    python_fill[key.substr(2)] = GetFillValueViaApi(var.value());
  }
  ASSERT_FALSE(python_fill.empty())
      << "mdio-python produced no dtype variables to compare against";

  // 3. Build an equivalent dataset with mdio-cpp covering the same dtypes.
  nlohmann::json manifest;
  manifest["metadata"] = {{"name", "fill_value_cpp"},
                          {"apiVersion", "1.0.0"},
                          {"createdOn", "2023-12-12T15:02:06.413469-06:00"}};
  manifest["variables"] = nlohmann::json::array();
  for (const auto& [dtype, _] : python_fill) {
    nlohmann::json var;
    var["name"] = "v_" + dtype;
    var["dataType"] = dtype;
    var["dimensions"] = nlohmann::json::array(
        {nlohmann::json{{"name", "v_" + dtype}, {"size", 4}}});
    manifest["variables"].push_back(var);
  }

  std::string cpp_path = base_path_ + "/fill_value_cpp";
  std::filesystem::remove_all(cpp_path);
  auto cpp_ds_future = mdio::Dataset::from_json(
      manifest, cpp_path, version_, mdio::constants::kCreateClean);
  ASSERT_TRUE(cpp_ds_future.status().ok()) << cpp_ds_future.status();
  auto cpp_ds = cpp_ds_future.value();

  // 4. Compare mdio-cpp's fill values against mdio-python's, dtype by dtype,
  //    reading both through the same API so encodings are consistent.
  for (const auto& [dtype, py_fill] : python_fill) {
    auto var = cpp_ds.variables.at("v_" + dtype);
    ASSERT_TRUE(var.status().ok()) << dtype << ": " << var.status();
    nlohmann::json cpp_fill = GetFillValueViaApi(var.value());
    EXPECT_EQ(NormalizeFillValue(cpp_fill), NormalizeFillValue(py_fill))
        << "dtype=" << dtype << " version=" << (is_v3 ? "V3" : "V2")
        << " cpp=" << cpp_fill.dump() << " python=" << py_fill.dump();
  }

  std::filesystem::remove_all(python_path);
  std::filesystem::remove_all(cpp_path);
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
// Parameterized Python/Xarray Dataset Compatibility Tests
// ============================================================================

class XarrayCompatibilityTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(XarrayCompatibilityTest, datasetCompatible) {
  // This test verifies that a Dataset created by MDIO can be opened by xarray.
  // The dataset is created fresh for this test to ensure isolation.
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

  std::string test_path = base_path_ + "/xarray_compat";
  std::filesystem::remove_all(test_path);

  nlohmann::json j = nlohmann::json::parse(manifest);
  auto ds = mdio::Dataset::from_json(j, test_path, version_,
                                     mdio::constants::kCreateClean);
  ASSERT_TRUE(ds.status().ok()) << ds.status();

  std::string srcPath =
      std::string(GetPythonBasePath()) + XARRAY_SCRIPT_RELATIVE_PATH;

  if (access(srcPath.c_str(), F_OK) == -1) {
    std::cerr << "Error: Python script not found at " << srcPath << std::endl;
    std::filesystem::remove_all(test_path);
    FAIL() << "Script not found: " << srcPath;
  }

  // Test without consolidated metadata (both versions)
  // Note: Consolidated metadata is only supported for V2
  std::vector<std::vector<std::string>> arg_sets = {
      {test_path + "/", "False"},
  };
  if (version_ == mdio::zarr::ZarrVersion::kV2) {
    // Also test with consolidated metadata for V2
    arg_sets.push_back({test_path + "/", "True"});
  }

  std::string version_name = ZarrVersionToString(version_);
  EXPECT_TRUE(RunPythonScripts(
      srcPath, arg_sets, "Xarray compatibility skipped due to import error"))
      << "xarray " << version_name << " compatibility test failed";

  // std::filesystem::remove_all(test_path);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, XarrayCompatibilityTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

// ============================================================================
// Parameterized Python/Multidimio Dataset Compatibility Tests
// ============================================================================

class MultidimioCompatibilityTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  void SetUp() override {
    version_ = GetParam();
    base_path_ = GetBasePath(version_);
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(MultidimioCompatibilityTest, datasetCompatible) {
  // This test verifies that a Dataset created by multidimio can be opened by
  // C++, for both Zarr V2 and V3. It runs the python script to ingest a real
  // SEG-Y dataset to an MDIO dataset. The python ingestion honors the requested
  // Zarr format via the ZARR_DEFAULT_ZARR_FORMAT environment variable, which
  // the forked interpreter inherits.
  std::string test_path = base_path_ + "/multidimio_compat";
  std::filesystem::remove_all(test_path);

  std::string srcPath =
      std::string(GetPythonBasePath()) + MULTIDIMIO_SCRIPT_RELATIVE_PATH;

  if (access(srcPath.c_str(), F_OK) == -1) {
    std::cerr << "Error: Python script not found at " << srcPath << std::endl;
    std::filesystem::remove_all(test_path);
    FAIL() << "Script not found: " << srcPath;
  }

  const std::string zarr_format =
      version_ == mdio::zarr::ZarrVersion::kV3 ? "3" : "2";
  setenv("ZARR_DEFAULT_ZARR_FORMAT", zarr_format.c_str(), /*overwrite=*/1);

  std::vector<std::vector<std::string>> arg_sets = {
      {test_path + "/"},
  };

  bool scripts_passed =
      RunPythonScripts(srcPath, arg_sets,
                       "Multidimio compatibility skipped due to import error");
  unsetenv("ZARR_DEFAULT_ZARR_FORMAT");

  EXPECT_TRUE(scripts_passed) << "multidimio compatibility test failed";

  // Now try to open the ingested dataset with C++
  std::cout << "Attempting to open the ingested dataset with C++..."
            << std::endl;
  auto ds = mdio::Dataset::Open(test_path, mdio::constants::kOpen);

  EXPECT_TRUE(ds.status().ok()) << ds.status();

  auto dataset = ds.value();
  EXPECT_TRUE(dataset.header_variables.contains_key("segy_file_header"));

  auto header_var = dataset.get_header_variable("segy_file_header");
  ASSERT_TRUE(header_var.status().ok()) << header_var.status();
  EXPECT_EQ(header_var.value().get_variable_name(), "segy_file_header");
  EXPECT_EQ(header_var.value().rank(), 0);

  auto read_future = header_var.value().Read();
  EXPECT_FALSE(read_future.status().ok());

  std::stringstream printed;
  printed << dataset;
  EXPECT_THAT(printed.str(),
              ::testing::HasSubstr("Header Variable: segy_file_header"));

  auto attrs = header_var.value().GetAttributes();
  ASSERT_TRUE(attrs.contains("attributes"));
  ASSERT_TRUE(attrs["attributes"].contains("textHeader"));
  nlohmann::json updated_attrs = attrs;
  updated_attrs["attributes"]["testMarker"] = "cpp-mdio";
  ASSERT_TRUE(header_var.value().UpdateAttributes(updated_attrs).ok());

  // Persist through the Dataset commit path, which rewrites the consolidated
  // metadata and republishes the variable attributes together, keeping them in
  // sync. This is the canonical way to persist metadata changes.
  auto commit_future = dataset.CommitMetadata();
  ASSERT_TRUE(commit_future.status().ok()) << commit_future.status();

  auto reopened = mdio::Dataset::Open(test_path, mdio::constants::kOpen);
  ASSERT_TRUE(reopened.status().ok()) << reopened.status();
  auto reopened_header =
      reopened.value().get_header_variable("segy_file_header");
  ASSERT_TRUE(reopened_header.status().ok()) << reopened_header.status();
  EXPECT_EQ(reopened_header.value().GetAttributes()["attributes"]["testMarker"],
            "cpp-mdio");

  // Clean up
  std::filesystem::remove_all(test_path);
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, MultidimioCompatibilityTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return ZarrVersionToString(info.param);
    });

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
