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

  mdio::SliceDescriptor desc1 = {"inline", 0, 5, 1};

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
  ASSERT_TRUE(metadata.contains("statsV1"))
      << "Did not find statsV1 in metadata";
  ASSERT_TRUE(metadata["statsV1"].contains("histogram"))
      << "Did not find histogram in statsV1";
  ASSERT_TRUE(metadata["statsV1"]["histogram"].contains("binCenters"))
      << "Did not find binCenters in histogram";
  EXPECT_TRUE(metadata["statsV1"]["histogram"]["binCenters"] ==
              std::vector<float>({2, 4, 6}))
      << "Expected binCenters to be [2, 4, 6] but got "
      << metadata["statsV1"]["histogram"]["binCenters"];
  EXPECT_TRUE(metadata["statsV1"]["histogram"]["counts"] ==
              std::vector<float>({10, 15, 20}))
      << "Expected counts to be [10, 15, 20] but got "
      << metadata["statsV1"]["histogram"]["counts"];
}

TEST(Dataset, commitSlicedMetadata) {
  std::filesystem::remove_all("zarrs/acceptance");
  auto json_vars = GetToyExample();

  auto datasetRes = mdio::Dataset::from_json(json_vars, "zarrs/acceptance",
                                             mdio::constants::kCreateClean);

  ASSERT_TRUE(datasetRes.status().ok()) << datasetRes.status();
  auto dataset = datasetRes.value();

  mdio::SliceDescriptor desc1 = {"inline", 0, 5, 1};
  mdio::SliceDescriptor desc2 = {"crossline", 0, 5, 1};

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
