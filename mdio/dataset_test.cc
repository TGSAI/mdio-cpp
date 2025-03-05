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

#include "mdio/dataset.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "mdio/dataset_factory.h"
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
::nlohmann::json GetToyExample() {
  std::string schema = R"(
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
      "dataType": "float16",
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
      "dataType": "float32",
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
  return ::nlohmann::json::parse(schema);
}

::nlohmann::json base_variable = {
    {"driver", "zarr"},
    {"kvstore", {{"driver", "file"}, {"path", "name"}}},
    {"attributes",
     {
         {"long_name", "foooooo ....."},   // required
         {"dimension_names", {"x", "y"}},  // required
     }},
    {"metadata",
     {
         {"compressor", {{"id", "blosc"}}},
         {"dtype", "<i2"},
         {"shape", {500, 500}},
         {"dimension_separator", "/"},
     }}};

std::vector<::nlohmann::json> make_vars() {
  auto jvar1 = base_variable;
  jvar1["kvstore"]["path"] = "dataset/var1";
  jvar1["attributes"]["long_name"] = "variable_1";
  jvar1["metadata"]["shape"] = {100, 200, 300};
  jvar1["attributes"]["dimension_names"] = {"inline", "crossline", "sample"};
  jvar1["attributes"]["coordinates"] = {"var2"};

  auto jvar2 = base_variable;
  jvar2["kvstore"]["path"] = "dataset/var2";
  jvar2["attributes"]["long_name"] = "variable_2";
  jvar2["metadata"]["shape"] = {100, 200};
  jvar2["metadata"]["chunks"] = {20, 20};
  jvar2["attributes"]["dimension_names"] = {"inline", "crossline"};

  return {jvar1, jvar2};
}

mdio::Dataset make() {
  auto json_vars = make_vars();

  auto var1 =
      mdio::Variable<>::Open(json_vars[0], mdio::constants::kCreateClean)
          .value();

  auto var2 =
      mdio::Variable<>::Open(json_vars[1], mdio::constants::kCreateClean)
          .value();

  ::nlohmann::json metadata = {{"name", "my dataset"}};

  mdio::VariableCollection variables;
  variables.add("var1", var1);
  variables.add("var2", var2);

  mdio::coordinate_map coords = {
      {"var1", {"inline", "crossline", "sample"}},
      {"var2", {"inline", "crossline"}},
  };

  // there is an implied sorting for dimensions here.
  auto domain_result = tensorstore::IndexDomainBuilder<>(3)
                           .origin({0, 0, 0})
                           .shape({100, 200, 300})
                           .labels({"inline", "crossline", "sample"})
                           .Finalize();

  return {metadata, variables, coords, domain_result.value()};
}

mdio::Result<mdio::Dataset> makePopulated(const std::string& path) {
  std::string schema = R"(
{
  "metadata": {
    "name": "selTester",
    "apiVersion": "1.0.0",
    "createdOn": "2024-08-23T08:56:00.000000-06:00"
  },
  "variables": [
    {
      "name": "data",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 10},
        {"name": "crossline", "size": 15},
        {"name": "depth", "size": 20}
      ]
    },
    {
      "name": "inline",
      "dataType": "int32",
      "dimensions": [{"name": "inline", "size": 10}]
    },
    {
      "name": "crossline",
      "dataType": "int32",
      "dimensions": [{"name": "crossline", "size": 15}]
    },
    {
      "name": "depth",
      "dataType": "int32",
      "dimensions": [{"name": "depth", "size": 20}]
    }
  ]
})";
  nlohmann::json j = nlohmann::json::parse(schema);
  auto dsFut = mdio::Dataset::from_json(j, path, mdio::constants::kCreateClean);
  if (!dsFut.status().ok()) {
    return dsFut.status();
  }
  auto ds = dsFut.value();
  MDIO_ASSIGN_OR_RETURN(auto dataVar,
                        ds.variables.get<mdio::dtypes::float32_t>("data"));
  MDIO_ASSIGN_OR_RETURN(auto inlineVar,
                        ds.variables.get<mdio::dtypes::int32_t>("inline"));
  MDIO_ASSIGN_OR_RETURN(auto crosslineVar,
                        ds.variables.get<mdio::dtypes::int32_t>("crossline"));
  MDIO_ASSIGN_OR_RETURN(auto depthVar,
                        ds.variables.get<mdio::dtypes::int32_t>("depth"));

  MDIO_ASSIGN_OR_RETURN(auto dataData,
                        mdio::from_variable<mdio::dtypes::float32_t>(dataVar));
  MDIO_ASSIGN_OR_RETURN(auto inlineData,
                        mdio::from_variable<mdio::dtypes::int32_t>(inlineVar));
  MDIO_ASSIGN_OR_RETURN(
      auto crosslineData,
      mdio::from_variable<mdio::dtypes::int32_t>(crosslineVar));
  MDIO_ASSIGN_OR_RETURN(auto depthData,
                        mdio::from_variable<mdio::dtypes::int32_t>(depthVar));

  auto dataAccessor = dataData.get_data_accessor();
  auto inlineAccessor = inlineData.get_data_accessor();
  auto crosslineAccessor = crosslineData.get_data_accessor();
  auto depthAccessor = depthData.get_data_accessor();

  // Inline has some repeated values to show possible conditions for slicing
  std::vector<mdio::dtypes::int32_t> inlineCoords(
      {1, 2, 3, 4, 3, 5, 6, 7, 8, 8});

  for (int i = 0; i < 10; ++i) {
    inlineAccessor({i}) = inlineCoords[i];
  }

  // Assign some coordinate values
  for (int i = 0; i < 15; ++i) {
    crosslineAccessor({i}) = i + 18;
  }
  for (int i = 0; i < 20; ++i) {
    depthAccessor({i}) = i + 50;
  }

  auto inlineFut = inlineVar.Write(inlineData);
  auto crosslineFut = crosslineVar.Write(crosslineData);
  auto depthFut = depthVar.Write(depthData);

  // Assign some data to the data variable
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 15; ++j) {
      for (int k = 0; k < 20; ++k) {
        dataAccessor({i, j, k}) =
            static_cast<float>(inlineCoords[i]) +
            static_cast<float>(j) / 100;  // Do nothing special for depth
      }
    }
  }

  auto dataFut = dataVar.Write(dataData);

  if (!inlineFut.status().ok()) {
    return inlineFut.status();
  }
  if (!crosslineFut.status().ok()) {
    return crosslineFut.status();
  }
  if (!depthFut.status().ok()) {
    return depthFut.status();
  }
  if (!dataFut.status().ok()) {
    return dataFut.status();
  }

  return ds;
}

TEST(DatasetSpec, valid) {
  auto dataset = make();

  // Use the iterable accessor to get sorted keys
  auto keys = dataset.variables.get_iterable_accessor();
  ASSERT_TRUE(keys.size() == 2) << "Expected 2 variables in the dataset";
  EXPECT_TRUE(keys[0] == "var1") << "Expected first variable to be var1";
  EXPECT_TRUE(keys[1] == "var2") << "Expected second variable to be var2";

  auto result = dataset.variables.get("var1");
  ASSERT_TRUE(result.status().ok()) << result.status();
}

TEST(Dataset, isel) {
  auto json_vars = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                          mdio::constants::kCreateClean)
                     .result();

  ASSERT_TRUE(dataset.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 5, 1};

  auto slice = dataset->isel(desc1);

  ASSERT_TRUE(slice.ok());

  auto domain = slice->domain;

  ASSERT_EQ(domain.rank(), 3) << "Tensorstore should have 3 dimensions";

  // Check depth range
  auto depthRange = domain[1];
  EXPECT_EQ(depthRange.interval().inclusive_min(), 0)
      << "Depth range should start at 0";
  EXPECT_EQ(depthRange.interval().exclusive_max(), 384)
      << "Depth range should end at 384";

  // Check crossline range
  auto crosslineRange = domain[0];
  EXPECT_EQ(crosslineRange.interval().inclusive_min(), 0)
      << "Crossline range should start at 0";
  EXPECT_EQ(crosslineRange.interval().exclusive_max(), 512)
      << "Crossline range should end at 512";

  // Check inline range
  auto inlineRange = domain[2];
  EXPECT_EQ(inlineRange.interval().inclusive_min(), 0)
      << "Inline range should start at 0";
  EXPECT_EQ(inlineRange.interval().exclusive_max(), 5)
      << "Inline range should end at 5";
}

TEST(Dataset, iselWithStride) {
  // Tests the integrity of data that is written with a strided slice.
  std::string iselPath = "zarrs/acceptance";
  {  // Scoping the dataset creation to ensure the variables are cleaned up
     // before the testing.
    auto json_vars = GetToyExample();
    auto dataset = mdio::Dataset::from_json(json_vars, iselPath,
                                            mdio::constants::kCreateClean);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
    auto ds = dataset.value();

    mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 256, 2};
    auto sliceRes = ds.isel(desc1);
    ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
    ds = sliceRes.value();

    auto ilVarRes = ds.variables.get<mdio::dtypes::uint32_t>("inline");
    ASSERT_TRUE(ilVarRes.status().ok()) << ilVarRes.status();
    auto ilVar = ilVarRes.value();

    auto ilDataRes = mdio::from_variable<mdio::dtypes::uint32_t>(ilVar);
    ASSERT_TRUE(ilDataRes.status().ok()) << ilDataRes.status();
    auto ilData = ilDataRes.value();

    auto ilAccessor = ilData.get_data_accessor().data();
    for (uint32_t i = 0; i < 128; i++) {
      ilAccessor[i] = i * 2;
    }

    auto ilFut = ilVar.Write(ilData);

    ASSERT_TRUE(ilFut.status().ok()) << ilFut.status();

    // --- Begin new QC data generation for the "image" variable (float32) ---
    auto imageVarRes = ds.variables.get<mdio::dtypes::float32_t>("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    auto imageVar = imageVarRes.value();

    auto imageDataRes = mdio::from_variable<mdio::dtypes::float32_t>(imageVar);
    ASSERT_TRUE(imageDataRes.status().ok()) << imageDataRes.status();
    auto imageData = imageDataRes.value();

    auto imageAccessor = imageData.get_data_accessor().data();
    for (uint32_t i = 0; i < 128; i++) {
      for (uint32_t j = 0; j < 512; j++) {
        for (uint32_t k = 0; k < 384; k++) {
          imageAccessor[i * (512 * 384) + j * 384 + k] =
              static_cast<float>(i * 2) + j * 0.1f + k * 0.01f;
        }
      }
    }

    auto imageWriteFut = imageVar.Write(imageData);
    ASSERT_TRUE(imageWriteFut.status().ok()) << imageWriteFut.status();
  }  // end of scoping the dataset creation to ensure the variables are cleaned
     // up before the testing.

  auto reopenedDsFut = mdio::Dataset::Open(iselPath, mdio::constants::kOpen);
  ASSERT_TRUE(reopenedDsFut.status().ok()) << reopenedDsFut.status();
  auto reopenedDs = reopenedDsFut.value();

  auto inlineVarRes =
      reopenedDs.variables.get<mdio::dtypes::uint32_t>("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto inlineVar = inlineVarRes.value();

  auto inlineDataFut = inlineVar.Read();
  ASSERT_TRUE(inlineDataFut.status().ok()) << inlineDataFut.status();
  auto inlineData = inlineDataFut.value();
  auto inlineAccessor = inlineData.get_data_accessor().data();
  for (uint32_t i = 0; i < 256; i++) {
    if (i % 2 == 0) {
      ASSERT_EQ(inlineAccessor[i], i) << "Expected inline value to be " << i
                                      << " but got " << inlineAccessor[i];
    } else {
      ASSERT_EQ(inlineAccessor[i], 0)
          << "Expected inline value to be 0 but got " << inlineAccessor[i];
    }
  }

  auto imageVarResReopen =
      reopenedDs.variables.get<mdio::dtypes::float32_t>("image");
  ASSERT_TRUE(imageVarResReopen.status().ok()) << imageVarResReopen.status();
  auto imageVarReopen = imageVarResReopen.value();

  auto imageDataFut = imageVarReopen.Read();
  ASSERT_TRUE(imageDataFut.status().ok()) << imageDataFut.status();
  auto imageDataFull = imageDataFut.value();
  auto imageAccessorFull = imageDataFull.get_data_accessor().data();

  // Instead of checking all 256x512x384 elements (which can be very time
  // consuming), we check a few sample indices. For full "image" variable, for
  // every full inline index i: if (i % 2 == 0): the expected value is i +
  // j*0.1f + k*0.01f, otherwise NaN.
  std::vector<uint32_t> sample_i = {0, 1, 2,
                                    255};  // mix of even and odd indices
  std::vector<uint32_t> sample_j = {0, 256, 511};
  std::vector<uint32_t> sample_k = {0, 100, 383};

  for (auto i : sample_i) {
    for (auto j : sample_j) {
      for (auto k : sample_k) {
        size_t index = i * (512 * 384) + j * 384 + k;
        float actual = imageAccessorFull[index];

        if (i % 2 == 0) {
          // For even indices, we expect a specific value
          float expected = static_cast<float>(i) + j * 0.1f + k * 0.01f;
          ASSERT_FLOAT_EQ(actual, expected)
              << "QC mismatch in image variable at (" << i << ", " << j << ", "
              << k << ")";
        } else {
          // For odd indices, we expect NaN
          ASSERT_TRUE(std::isnan(actual))
              << "Expected NaN at (" << i << ", " << j << ", " << k
              << ") but got " << actual;
        }
      }
    }
  }
  // --- End new QC check for the "image" variable ---
}

TEST(Dataset, iselWithStrideAndExistingData) {
  std::string testPath = "zarrs/slice_scale_test";
  float scaleFactor = 2.5f;

  // --- Step 1: Initialize the entire image variable with QC values and Write
  // it ---
  {
    // Create a new dataset
    auto json_vars = GetToyExample();
    auto dataset = mdio::Dataset::from_json(json_vars, testPath,
                                            mdio::constants::kCreateClean);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
    auto ds = dataset.value();

    // Get the "image" variable (expected to be float32_t type)
    auto imageVarRes = ds.variables.get<mdio::dtypes::float32_t>("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    auto imageVar = imageVarRes.value();

    auto imageDataRes = mdio::from_variable<mdio::dtypes::float32_t>(imageVar);
    ASSERT_TRUE(imageDataRes.status().ok()) << imageDataRes.status();
    auto imageData = imageDataRes.value();
    auto imageAccessor = imageData.get_data_accessor().data();

    // Initialize the entire "image" variable with QC values.
    // For this test, we assume dimensions 256 x 512 x 384.
    for (uint32_t i = 0; i < 256; i++) {
      for (uint32_t j = 0; j < 512; j++) {
        for (uint32_t k = 0; k < 384; k++) {
          imageAccessor[i * (512 * 384) + j * 384 + k] =
              static_cast<float>(i) + j * 0.1f + k * 0.01f;
        }
      }
    }

    auto writeFut = imageVar.Write(imageData);
    ASSERT_TRUE(writeFut.status().ok()) << writeFut.status();
  }  // End of Step 1

  // --- Step 2: Slice with stride of 2 and scale the values of the "image"
  // variable ---
  {
    // Re-open the dataset for modifications.
    auto reopenedDsFut = mdio::Dataset::Open(testPath, mdio::constants::kOpen);
    ASSERT_TRUE(reopenedDsFut.status().ok()) << reopenedDsFut.status();
    auto ds = reopenedDsFut.value();

    // Slice the dataset along the "inline" dimension using a stride of 2.
    mdio::RangeDescriptor<mdio::Index> desc = {"inline", 0, 256, 2};
    auto sliceRes = ds.isel(desc);
    ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
    auto ds_slice = sliceRes.value();

    // Get the "image" variable from the sliced dataset.
    auto imageVarRes = ds_slice.variables.get<mdio::dtypes::float32_t>("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    auto imageVar = imageVarRes.value();

    auto imageDataFut = imageVar.Read();
    ASSERT_TRUE(imageDataFut.status().ok()) << imageDataFut.status();
    auto imageData = imageDataFut.value();
    auto imageAccessor = imageData.get_data_accessor().data();

    // The sliced "image" now has dimensions 128 x 512 x 384 because we selected
    // every 2nd index. Scale each element in the slice by 'scaleFactor'
    for (uint32_t ii = 0; ii < 128;
         ii++) {  // 'ii' corresponds to original index i = ii * 2.
      for (uint32_t j = 0; j < 512; j++) {
        for (uint32_t k = 0; k < 384; k++) {
          size_t index = ii * (512 * 384) + j * 384 + k;
          imageAccessor[index] *= scaleFactor;
        }
      }
    }
    // Write the updated (scaled) data back to the dataset.
    auto writeFut = imageVar.Write(imageData);
    ASSERT_TRUE(writeFut.status().ok()) << writeFut.status();
  }  // End of Step 2

  // --- Step 3: Read the entire image variable and validate QC values ---
  {
    // Re-open the dataset for the final validation.
    auto reopenedDsFut = mdio::Dataset::Open(testPath, mdio::constants::kOpen);
    ASSERT_TRUE(reopenedDsFut.status().ok()) << reopenedDsFut.status();
    auto ds = reopenedDsFut.value();

    auto imageVarRes = ds.variables.get<mdio::dtypes::float32_t>("image");
    ASSERT_TRUE(imageVarRes.status().ok()) << imageVarRes.status();
    auto imageVar = imageVarRes.value();

    auto imageReadFut = imageVar.Read();
    ASSERT_TRUE(imageReadFut.status().ok()) << imageReadFut.status();
    auto imageData = imageReadFut.value();
    auto imageAccessor = imageData.get_data_accessor().data();

    // Validate the values over the entire "image" variable.
    // For even inline indices (i % 2 == 0) we expect the initial QC value
    // scaled by 'scaleFactor'. For odd inline indices, the original QC values
    // should remain.
    for (uint32_t i = 0; i < 256; i++) {
      for (uint32_t j = 0; j < 512; j++) {
        for (uint32_t k = 0; k < 384; k++) {
          size_t index = i * (512 * 384) + j * 384 + k;
          float baseValue = static_cast<float>(i) + j * 0.1f + k * 0.01f;
          float expected = (i % 2 == 0) ? baseValue * scaleFactor : baseValue;
          auto val = imageAccessor[index];
          ASSERT_FLOAT_EQ(val, expected)
              << "Mismatch at (" << i << ", " << j << ", " << k
              << "): expected " << expected << ", but got " << val;
        }
      }
    }
  }  // End of Step 3
}

TEST(Dataset, selValue) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ValueDescriptor<mdio::dtypes::int32_t> ilValue = {"inline", 1};

  auto sliceRes = ds.sel(ilValue);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
  auto slicedDs = sliceRes.value();

  auto inlineVarRes = slicedDs.variables.at("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto samples = inlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 1) << "Expected 1 sample for inline but got " << samples;

  auto xlineVarRes = slicedDs.variables.at("crossline");
  ASSERT_TRUE(xlineVarRes.status().ok()) << xlineVarRes.status();
  samples = xlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 15) << "Expected 15 samples for crossline but got "
                         << samples;

  auto depthVarRes = slicedDs.variables.at("depth");
  ASSERT_TRUE(depthVarRes.status().ok()) << depthVarRes.status();
  samples = depthVarRes.value().num_samples();
  EXPECT_EQ(samples, 20) << "Expected 20 samples for depth but got " << samples;

  auto dataVarRes = slicedDs.variables.at("data");
  ASSERT_TRUE(dataVarRes.status().ok()) << dataVarRes.status();
  samples = dataVarRes.value().num_samples();
  EXPECT_EQ(samples, 1 * 15 * 20)
      << "Expected 1*15*20 samples for data but got " << samples;
}

TEST(Dataset, selIndex) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  // Attempt to get the 0th dimension from the dataset (illegal)
  mdio::RangeDescriptor<mdio::dtypes::int32_t> ilRange = {0, 2, 5, 1};

  auto sliceRes = ds.sel(ilRange);
  ASSERT_FALSE(sliceRes.ok());
}

TEST(Dataset, selRepeatedValue) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ValueDescriptor<mdio::dtypes::int32_t> ilValue = {"inline", 3};

  auto sliceRes = ds.sel(ilValue);
  ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
  auto slicedDs = sliceRes.value();

  auto inlineVarRes = slicedDs.variables.at("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto samples = inlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 2) << "Expected 1 sample for inline but got " << samples;

  auto xlineVarRes = slicedDs.variables.at("crossline");
  ASSERT_TRUE(xlineVarRes.status().ok()) << xlineVarRes.status();
  samples = xlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 15) << "Expected 15 samples for crossline but got "
                         << samples;

  auto depthVarRes = slicedDs.variables.at("depth");
  ASSERT_TRUE(depthVarRes.status().ok()) << depthVarRes.status();
  samples = depthVarRes.value().num_samples();
  EXPECT_EQ(samples, 20) << "Expected 20 samples for depth but got " << samples;

  auto dataVarRes = slicedDs.variables.at("data");
  ASSERT_TRUE(dataVarRes.status().ok()) << dataVarRes.status();
  samples = dataVarRes.value().num_samples();
  EXPECT_EQ(samples, 2 * 15 * 20)
      << "Expected 2*15*20 samples for data but got " << samples;
}

TEST(Dataset, selMultipleValues) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ValueDescriptor<mdio::dtypes::int32_t> ilValue = {"inline", 1};
  mdio::ValueDescriptor<mdio::dtypes::int32_t> xlValue = {"crossline", 20};

  auto sliceRes = ds.sel(ilValue, xlValue);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
  auto slicedDs = sliceRes.value();

  auto inlineVarRes = slicedDs.variables.at("inline");
  ASSERT_TRUE(inlineVarRes.status().ok()) << inlineVarRes.status();
  auto samples = inlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 1) << "Expected 1 sample for inline but got " << samples;

  auto xlineVarRes = slicedDs.variables.at("crossline");
  ASSERT_TRUE(xlineVarRes.status().ok()) << xlineVarRes.status();
  samples = xlineVarRes.value().num_samples();
  EXPECT_EQ(samples, 1) << "Expected 1 sample for crossline but got "
                        << samples;

  auto depthVarRes = slicedDs.variables.at("depth");
  ASSERT_TRUE(depthVarRes.status().ok()) << depthVarRes.status();
  samples = depthVarRes.value().num_samples();
  EXPECT_EQ(samples, 20) << "Expected 20 sample for depth but got " << samples;

  auto dataVarRes = slicedDs.variables.at("data");
  ASSERT_TRUE(dataVarRes.status().ok()) << dataVarRes.status();
  samples = dataVarRes.value().num_samples();
  EXPECT_EQ(samples, 1 * 1 * 20)
      << "Expected 1*1*20 samples for data but got " << samples;
}

TEST(Dataset, selRepeated) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ValueDescriptor<mdio::dtypes::int32_t> ilValue1 = {"inline", 3};
  mdio::ValueDescriptor<mdio::dtypes::int32_t> ilValue2 = {"inline", 4};

  auto sliceRes = ds.sel(ilValue1, ilValue2);
  EXPECT_FALSE(sliceRes.status().ok())
      << "Descriptor with repeated labels are expressly forbidden, but passed "
         "anyway";
}

TEST(Dataset, selList) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  std::vector<mdio::dtypes::int32_t> selCoords = {2, 1, 5, 7};
  mdio::ListDescriptor<mdio::dtypes::int32_t> ilValues = {"inline", selCoords};

  auto sliceRes = ds.sel(ilValues);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, selListMissingCoord) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  std::vector<mdio::dtypes::int32_t> selCoords = {2, 1, 5, 7, 9};
  mdio::ListDescriptor<mdio::dtypes::int32_t> ilValues = {"inline", selCoords};

  auto sliceRes = ds.sel(ilValues);
  ASSERT_FALSE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, selRange) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::RangeDescriptor<mdio::dtypes::int32_t> ilRange = {"inline", 2, 5, 1};

  auto sliceRes = ds.sel(ilRange);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, legacySliceDescriptor) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::SliceDescriptor ilRange = {"inline", 2, 5, 1};

  // ds.sel does not, and will not, support the legacy slice descriptor
  auto sliceRes = ds.isel(ilRange);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, selRangeFlippedStartStop) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::RangeDescriptor<mdio::dtypes::int32_t> ilRange = {"inline", 5, 2, 1};

  auto sliceRes = ds.sel(ilRange);
  ASSERT_FALSE(sliceRes.status().ok());
}

TEST(Dataset, selRepeatedListSingleton) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ListDescriptor<mdio::dtypes::int32_t> ilValues = {"inline", {3}};

  auto sliceRes = ds.sel(ilValues);
  ASSERT_FALSE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, selRepeatedListMulti) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::ListDescriptor<mdio::dtypes::int32_t> ilValues = {"inline", {3, 4, 5}};

  auto sliceRes = ds.sel(ilValues);
  ASSERT_FALSE(sliceRes.ok()) << sliceRes.status();
}

TEST(Dataset, selRepeatedRangeStart) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::RangeDescriptor<mdio::dtypes::int32_t> ilRange = {"inline", 3, 5, 1};

  auto sliceRes = ds.sel(ilRange);
  ASSERT_FALSE(sliceRes.status().ok());
}

TEST(Dataset, selRepeatedRangeStop) {
  std::string path = "zarrs/selTester.mdio";
  auto dsRes = makePopulated(path);
  ASSERT_TRUE(dsRes.ok()) << dsRes.status();
  auto ds = dsRes.value();

  mdio::RangeDescriptor<mdio::dtypes::int32_t> ilRange = {"inline", 5, 3, 1};

  auto sliceRes = ds.sel(ilRange);
  ASSERT_FALSE(sliceRes.status().ok());
}

TEST(Dataset, selectField) {
  auto json_var = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_var, "zarrs/acceptance",
                                          mdio::constants::kCreateClean);

  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"crossline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc3 = {"depth", 0, 5, 1};

  auto sliceRes = ds.isel(desc1, desc2, desc3);
  ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
  auto slicedDs = sliceRes.value();

  auto structImageHeadersRes =
      slicedDs.variables.get<mdio::dtypes::byte_t>("image_headers");
  ASSERT_TRUE(structImageHeadersRes.status().ok())
      << structImageHeadersRes.status();
  auto structImageHeaders = structImageHeadersRes.value();
  auto asdf = structImageHeaders.get_intervals();
  ASSERT_TRUE(asdf.status().ok()) << asdf.status();
  auto structIntervals = asdf.value();
  ASSERT_EQ(structIntervals.size(), 3);

  auto selectedVarFut =
      slicedDs.SelectField<mdio::dtypes::int32_t>("image_headers", "cdp-x");
  ASSERT_TRUE(selectedVarFut.status().ok()) << selectedVarFut.status();

  auto typedInervalsRes = selectedVarFut.value().get_intervals();
  ASSERT_TRUE(typedInervalsRes.status().ok()) << typedInervalsRes.status();
  auto typedIntervals = typedInervalsRes.value();
  ASSERT_EQ(typedIntervals.size(), 2);

  EXPECT_EQ(typedIntervals[0].label, structIntervals[0].label)
      << "Dimension 0 labels did not match";
  EXPECT_EQ(typedIntervals[1].label, structIntervals[1].label)
      << "Dimension 1 labels did not match";
  EXPECT_EQ(typedIntervals[0].inclusive_min, structIntervals[0].inclusive_min)
      << "Dimension 0 min did not match";
  EXPECT_EQ(typedIntervals[1].inclusive_min, structIntervals[1].inclusive_min)
      << "Dimension 1 min did not match";
  EXPECT_EQ(typedIntervals[0].exclusive_max, structIntervals[0].exclusive_max)
      << "Dimension 0 max did not match";
  EXPECT_EQ(typedIntervals[1].exclusive_max, structIntervals[1].exclusive_max)
      << "Dimension 1 max did not match";
}

TEST(Dataset, selectFieldName) {
  auto json_var = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_var, "zarrs/acceptance",
                                          mdio::constants::kCreateClean);

  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  auto ds = dataset.value();

  auto selectedVarFut =
      ds.SelectField<mdio::dtypes::int32_t>("image_headers", "cdp-x");
  ASSERT_TRUE(selectedVarFut.status().ok()) << selectedVarFut.status();
  EXPECT_EQ(selectedVarFut.value().get_variable_name(), "image_headers")
      << "Expected selected variable to be named image_headers";
}

TEST(Dataset, fromConsolidatedMeta) {
  auto json_vars = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                          mdio::constants::kCreateClean)
                     .result();

  ASSERT_TRUE(dataset.ok());

  std::string dataset_path = "zarrs/acceptance/";

  auto new_dataset =
      mdio::Dataset::Open(dataset_path, mdio::constants::kOpen).result();
  ASSERT_TRUE(new_dataset.status().ok()) << new_dataset.status();
}

TEST(Dataset, open) {
  auto json_schema = GetToyExample();

  auto validated_schema = Construct(json_schema, "zarrs/acceptance");

  ASSERT_TRUE(validated_schema.ok());

  auto [metadata, json_vars] = validated_schema.value();

  auto result =
      mdio::Dataset::Open(metadata, json_vars, mdio::constants::kCreateClean)
          .result();

  ASSERT_TRUE(result.ok());
}

TEST(Dataset, openWithContext) {
  // tests the struct array on creation
  auto concurrency_json =
      ::nlohmann::json::parse(R"({"data_copy_concurrency": {"limit": 2}})");

  auto spec = mdio::Context::Spec::FromJson(concurrency_json);
  ASSERT_TRUE(spec.ok());

  auto context = mdio::Context(spec.value());

  auto json_schema = GetToyExample();

  auto validated_schema = Construct(json_schema, "zarrs/acceptance");

  ASSERT_TRUE(validated_schema.ok());

  auto [metadata, json_vars] = validated_schema.value();

  auto result = mdio::Dataset::Open(metadata, json_vars,
                                    mdio::constants::kCreateClean, context)
                    .result();

  ASSERT_TRUE(result.ok());
}

TEST(Dataset, fromJson) {
  auto json_vars = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                          mdio::constants::kCreateClean)
                     .result();

  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST(Dataset, multiFuture) {
  auto json_vars = make_vars();

  std::vector<mdio::Future<mdio::Variable<>>> variables;
  std::vector<tensorstore::Promise<void>> promises;
  std::vector<tensorstore::AnyFuture> futures;

  for (const auto& json : json_vars) {
    auto pair = tensorstore::PromiseFuturePair<void>::Make();

    auto var = mdio::Variable<>::Open(json, mdio::constants::kCreateClean);

    // Attach a continuation to the first future
    var.ExecuteWhenReady(
        [promise = std::move(pair.promise)](
            tensorstore::ReadyFuture<mdio::Variable<>> readyVar) {
          // When firstFuture is ready, fulfill the second promise
          promise.SetResult(
              absl::OkStatus());  // Set appropriate result or status
        });

    variables.push_back(std::move(var));
    promises.push_back(std::move(pair.promise));
    futures.push_back(std::move(pair.future));
  }

  auto all_done_future = tensorstore::WaitAllFuture(futures);

  // It may be the case that some futures become ready before this.
  // for (auto var : variables) {
  //   EXPECT_FALSE(var.ready());
  // }

  all_done_future.Wait();
  ASSERT_TRUE(all_done_future.result().status().ok())
      << all_done_future.result().status();

  for (auto var : variables) {
    ASSERT_TRUE(var.result().status().ok()) << var.result().status();
  }
}

TEST(Dataset, coordinates) {
  auto json_vars = GetToyExample();

  auto dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                          mdio::constants::kCreateClean);

  ASSERT_TRUE(dataset.status().ok()) << dataset.status();
  EXPECT_TRUE(dataset.value().coordinates.size() > 0)
      << "Dataset expected to have coordinates but none were present!";

  std::string path = "zarrs/acceptance";
  auto dataset_open = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(dataset_open.status().ok()) << dataset_open.status();
  EXPECT_TRUE(dataset_open.value().coordinates.size() > 0)
      << "Dataset expected to have coordinates but none were present!";
}

TEST(Dataset, noIntervals) {
  auto json_vars = GetToyExample();

  auto datasetFut = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetFut.status().ok()) << datasetFut.status();
  auto dataset = datasetFut.value();
  auto intervalRes = dataset.get_intervals();
  ASSERT_TRUE(intervalRes.ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 4);
  EXPECT_EQ(intervals[0].label, "inline");
  EXPECT_EQ(intervals[1].label, "crossline");
  EXPECT_EQ(intervals[2].label, "depth");
  EXPECT_EQ(intervals[3].label, "");
}

TEST(Dataset, oneInterval) {
  auto json_vars = GetToyExample();

  auto datasetFut = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetFut.status().ok()) << datasetFut.status();
  auto dataset = datasetFut.value();
  auto intervalRes = dataset.get_intervals("depth");
  ASSERT_TRUE(intervalRes.ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 1);
  EXPECT_EQ(intervals[0].label, "depth");
}

TEST(Dataset, mixedIntervals) {
  auto json_vars = GetToyExample();

  auto datasetFut = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetFut.status().ok()) << datasetFut.status();
  auto dataset = datasetFut.value();
  auto intervalRes = dataset.get_intervals("inline", "crossline", "time");
  ASSERT_TRUE(intervalRes.ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 2);
  EXPECT_EQ(intervals[0].label, "inline");
  EXPECT_EQ(intervals[1].label, "crossline");
}

TEST(Dataset, wrongIntervals) {
  auto json_vars = GetToyExample();

  auto datasetFut = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetFut.status().ok()) << datasetFut.status();
  auto dataset = datasetFut.value();
  auto intervalRes =
      dataset.get_intervals("offset", "azmiuth", "source_x", "source_y");
  ASSERT_FALSE(intervalRes.ok()) << intervalRes.status();
}

TEST(Dataset, slicedIntervals) {
  auto json_vars = GetToyExample();

  auto datasetFut = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetFut.status().ok()) << datasetFut.status();
  auto dataset = datasetFut.value();
  mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"crossline", 0, 5, 1};
  auto sliceRes = dataset.isel(desc1, desc2);
  ASSERT_TRUE(sliceRes.ok()) << sliceRes.status();
  auto slice = sliceRes.value();
  auto intervalRes = slice.get_intervals();
  ASSERT_TRUE(intervalRes.ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 4);
  EXPECT_EQ(intervals[0].label, "inline");
  EXPECT_EQ(intervals[1].label, "crossline");
  EXPECT_EQ(intervals[2].label, "depth");
  EXPECT_EQ(intervals[3].label, "");
  EXPECT_EQ(intervals[0].inclusive_min, 0);
  EXPECT_EQ(intervals[0].exclusive_max, 5);
  EXPECT_EQ(intervals[1].inclusive_min, 0);
  EXPECT_EQ(intervals[1].exclusive_max, 5);
  EXPECT_EQ(intervals[2].inclusive_min, 0);
  EXPECT_EQ(intervals[2].exclusive_max, 384);
  EXPECT_EQ(intervals[3].inclusive_min, 0);
  EXPECT_EQ(intervals[3].exclusive_max, 12);
}

TEST(Dataset, create) {
  std::filesystem::remove_all("zarrs/acceptance");

  // Create with no pre-existing Dataset
  auto json_vars = GetToyExample();
  auto dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                          mdio::constants::kCreate);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  // Create with pre-existing Dataset
  dataset = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                     mdio::constants::kCreateClean);
  ASSERT_TRUE(dataset.status().ok()) << dataset.status();

  // Simulate trying to create the same dataset but with slightly different
  // metadata
  json_vars["variables"][0]["metadata"]["statsV1"]["count"] = 222;
  json_vars["metadata"]["foo"] = "nobar";
  auto dataset_overwrite = mdio::Dataset::from_json(
      json_vars, "zarrs/acceptance", mdio::constants::kCreate);

  EXPECT_FALSE(dataset_overwrite.status().ok())
      << "Dataset successfully overwrote an existing dataset!";
}

TEST(Dataset, commitMetadata) {
  const std::string path = "zarrs/acceptance";
  std::filesystem::remove_all("zarrs/acceptance");
  auto json_vars = GetToyExample();

  auto datasetRes = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto dataset = datasetRes.value();

  auto imageRes = dataset.variables.at("image");
  ASSERT_TRUE(imageRes.ok()) << imageRes.status();
  auto image = imageRes.value();

  auto attrs = image.GetAttributes();
  attrs["statsV1"]["histogram"]["binCenters"] = {2, 4, 6};
  attrs["statsV1"]["histogram"]["counts"] = {10, 15, 20};
  auto attrsUpdateRes = image.UpdateAttributes<float>(attrs);
  ASSERT_TRUE(attrsUpdateRes.status().ok()) << attrsUpdateRes.status();

  auto commitRes = dataset.CommitMetadata();

  ASSERT_TRUE(commitRes.status().ok()) << commitRes.status();

  auto newDataset = mdio::Dataset::Open(path, mdio::constants::kOpen);
  ASSERT_TRUE(newDataset.status().ok()) << newDataset.status();

  auto newImageRes = newDataset.value().variables.at("image");
  ASSERT_TRUE(newImageRes.ok()) << newImageRes.status();

  nlohmann::json metadata = newImageRes.value().getMetadata();
  ASSERT_TRUE(metadata.contains("metadata")) << metadata;
  ASSERT_TRUE(metadata["metadata"].contains("statsV1"))
      << "Did not find statsV1 in metadata";
  ASSERT_TRUE(metadata["metadata"]["statsV1"].contains("histogram"))
      << "Did not find histogram in statsV1";
  ASSERT_TRUE(
      metadata["metadata"]["statsV1"]["histogram"].contains("binCenters"))
      << "Did not find binCenters in histogram";
  EXPECT_TRUE(metadata["metadata"]["statsV1"]["histogram"]["binCenters"] ==
              std::vector<float>({2, 4, 6}))
      << "Expected binCenters to be [2, 4, 6] but got "
      << metadata["metadata"]["statsV1"]["histogram"]["binCenters"];
  EXPECT_TRUE(metadata["metadata"]["statsV1"]["histogram"]["counts"] ==
              std::vector<float>({10, 15, 20}))
      << "Expected counts to be [10, 15, 20] but got "
      << metadata["metadata"]["statsV1"]["histogram"]["counts"];
}

TEST(Dataset, commitSlicedMetadata) {
  std::filesystem::remove_all("zarrs/acceptance");
  auto json_vars = GetToyExample();

  auto datasetRes = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto dataset = datasetRes.value();

  mdio::RangeDescriptor<mdio::Index> desc1 = {"inline", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"crossline", 0, 5, 1};

  auto sliceRes = dataset.isel(desc1, desc2);

  ASSERT_TRUE(sliceRes.status().ok()) << sliceRes.status();
  auto slice = sliceRes.value();

  auto imageRes = slice.variables.at("image");
  ASSERT_TRUE(imageRes.ok()) << imageRes.status();
  auto image = imageRes.value();

  auto attrs = image.GetAttributes();
  attrs["statsV1"]["histogram"]["binCenters"] = {2, 4, 6};
  attrs["statsV1"]["histogram"]["counts"] = {10, 15, 20};

  auto attrsUpdateRes = image.UpdateAttributes<float>(attrs);
  ASSERT_TRUE(attrsUpdateRes.status().ok()) << attrsUpdateRes.status();

  auto commitRes = dataset.CommitMetadata();

  EXPECT_TRUE(commitRes.status().ok()) << commitRes.status();
}

TEST(Dataset, openNonExistent) {
  auto json_vars = GetToyExample();

  auto datasetRes =
      mdio::Dataset::from_json(json_vars, "zarrs/DNE", mdio::constants::kOpen);
  ASSERT_FALSE(datasetRes.status().ok())
      << "Opened a non-existent dataset without error!";
}

TEST(Dataset, kCreateOverExisting) {
  auto json_vars = GetToyExample();

  auto datasetRes = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);
  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();

  datasetRes = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                        mdio::constants::kCreate);
  ASSERT_FALSE(datasetRes.status().ok())
      << "Created a dataset over an existing "
         "one without error!";
}

}  // namespace
