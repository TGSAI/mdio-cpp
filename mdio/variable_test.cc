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

#include "mdio/variable.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>  // For checking NaN values
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include "tensorstore/driver/zarr/compressor.h"
#include "tensorstore/driver/zarr/driver_impl.h"
#include "tensorstore/driver/zarr/dtype.h"
#include "tensorstore/driver/zarr/metadata.h"
#include "tensorstore/util/result.h"
#include "tensorstore/util/status_testutil.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

// clang-format off
::nlohmann::json json_good = ::nlohmann::json::object({
    {"driver", "zarr"},
    {"kvstore",
        {
            {"driver", "file"},
            {"path", "name"}
        }
    },
    {"attributes",
        {{"metadata",
            {{"attributes",  // optional misc attributes.
                {
                    {"job status", "win"},  // can be anything
                    {"project code", "fail"}
                }
            }}
        },
        {"long_name", "foooooo ....."},  // required
        {"dimension_names", {"x", "y"} },  // required
        {"dimension_units", {"m", "ft"} },  // optional (if coord).
    }},
    {"metadata",
        {
            {"compressor", {{"id", "blosc"}}},
            {"dtype", "<i2"},
            {"shape", {500, 500}},
            {"chunks", {100, 50}},
            {"dimension_separator", "/"},
        }
    }
});


::nlohmann::json json_bad_1 = {
    {"driver", "zarr"},
    {"kvstore",
        {
            {"driver", "file"},
            {"path", "name"}
        }
    },
    {"attributes",
        {{"dimension_units", {"m", "ft"} },  // optional (if coord).
            {"metadata",
                {{"attributes",  // optional misc attributes.
                    {
                        {"job status", "win"},
                        {"project code", "fail"}
                    }
                }}
            },
        }
    },
    {"metadata",
        {
            {"compressor", {{"id", "blosc"}}},
            {"dtype", "<i2"},
            {"shape", {100, 100}},
            {"chunks", {3, 2}},
            {"dimension_separator", "/"},
        }
    }
};


::nlohmann::json json_bad_2 = {
    {"driver", "zarr"},
    {"kvstore",
        {
            {"driver", "file"},
            {"path", "name"}
        }
    },
    {"metadata",
        {
            {"compressor", {{"id", "blosc"}}},
            {"dtype", "<i2"},
            {"shape", {100, 100}},
            {"chunks", {3, 2}},
            {"dimension_separator", "/"},
        }
    }
};

::nlohmann::json GetJsonSpecStruct() {
    std::string new_spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "test.zarr"
            },
            "metadata": {
                "compressor": {"id": "blosc"},
                "dtype": [["a", "<i2"], ["b", "<i4"], ["c", "|V2"]],
                "shape": [100, 100],
                "chunks": [3, 2],
                "dimension_separator": "/",
                "zarr_format": 2
            },
            "attributes": {
                "dimension_names": ["x", "y"],
                "long_name": "foooooo ....."
            }
        }
    )";
    // Parse the string to a JSON object
    return nlohmann::json::parse(new_spec);
}

// clang-format on

namespace {
mdio::Result<::nlohmann::json> PopulateStore(const nlohmann::json& json_good) {
  MDIO_ASSIGN_OR_RETURN(
      auto result,
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean)
          .result());

  MDIO_ASSIGN_OR_RETURN(auto variableDataFuture, result.Read().result())

  // Create a 500x500 array with the data to write
  auto data = tensorstore::AllocateArray<int16_t>({500, 500});
  for (int i = 0; i < 500; i++) {
    for (int j = 0; j < 500; j++) {
      data(i, j) = int16_t(j + (i * 500));
    }
  }

  // Write the data to the store
  auto write_future = tensorstore::Write(data, result.get_store());
  write_future.commit_future.Wait();
  write_future.copy_future.Wait();

  auto output_json = json_good;
  output_json.erase("attributes");
  output_json.erase("metadata");

  return output_json;
}

TEST(VariableData, conversion) {
  using int16_t = mdio::dtypes::int16_t;

  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();
  ASSERT_TRUE(variable.ok()) << variable.status();

  auto variable_a = mdio::from_variable<int16_t>(variable.value());
  ASSERT_TRUE(variable_a.ok()) << variable_a.status();
  // we cant convert from dtype to a void though

  // we need to be able to cast to void
  mdio::VariableData variable_b(variable_a.value());
  mdio::VariableData<int16_t> variable_c(variable_b);

  // FIXME - complete test
  EXPECT_EQ(250000, variable_b.num_samples());
  EXPECT_EQ(250000, variable_c.num_samples());

  EXPECT_EQ("name", variable_b.variableName);
  EXPECT_EQ("name", variable_c.variableName);
}

TEST(VariableData, fromVariable) {
  using int16_t = mdio::dtypes::int16_t;

  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(variable.ok());

  auto variable_data = mdio::from_variable<int16_t>(variable.value());

  ASSERT_TRUE(variable_data.ok());

  ASSERT_TRUE(variable_data->variableName == variable->get_variable_name());
  ASSERT_TRUE(variable_data->longName == variable->get_long_name());

  auto data = variable_data->get_data_accessor();

  data({0, 0}) = 100;
  EXPECT_EQ(data({0, 0}), 100);
  EXPECT_EQ(data({1, 0}), 0);

  auto _variable_data = mdio::from_variable(variable.value());

  ASSERT_TRUE(_variable_data.ok());
  // you can't do this data({0, 0}) because it's void, you would need to memcpy
}

TEST(VariableData, fromVariable2) {
  using int16_t = mdio::dtypes::int16_t;

  auto variable =
      mdio::Variable<int16_t>::Open(json_good, mdio::constants::kCreateClean)
          .result();

  ASSERT_TRUE(variable.ok());

  auto variable_data = mdio::from_variable<int16_t>(variable.value());

  ASSERT_TRUE(variable_data.ok());

  ASSERT_TRUE(variable_data->variableName == variable->get_variable_name());
  ASSERT_TRUE(variable_data->longName == variable->get_long_name());

  auto data = variable_data->get_data_accessor();

  data({0, 0}) = 100;
  EXPECT_EQ(data({0, 0}), 100)
      << "Data at 0,0 should be 100 but was " << data({0, 0});
  EXPECT_EQ(data({1, 0}), 0)
      << "Data at 1,0 should be 0 but was " << data({1, 0});
}

TEST(Variable, context) {
  // tests the struct array on creation
  auto concurrency_json =
      ::nlohmann::json::parse(R"({"data_copy_concurrency": {"limit": 2}})");

  auto spec = mdio::Context::Spec::FromJson(concurrency_json);
  ASSERT_TRUE(spec.ok());

  auto context = mdio::Context(spec.value());

  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean, context)
          .result();
}

TEST(Variable, structArray) {
  // tests the struct array on creation
  auto json_spec = GetJsonSpecStruct();

  auto variable =
      mdio::Variable<>::Open(json_spec, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(variable.ok());

  auto domain = variable->dimensions();
  // TODO(BrianMichell) - we might assign a "byte" label
  // This is a feature of Tensorstore
  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y", ""));

  auto bytes = mdio::constants::kByte;

  // check it should have the right dtype
  ASSERT_TRUE(variable->dtype() == bytes);

  std::filesystem::remove_all("test.zarr");
}

TEST(Variable, structArrayField) {
  auto json_spec = GetJsonSpecStruct();
  json_spec["field"] = "a";

  auto variableA =
      mdio::Variable<>::Open(json_spec, mdio::constants::kCreateClean).result();

  json_spec["field"] = "b";
  auto variableB =
      mdio::Variable<>::Open(json_spec, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(variableA.ok());
  ASSERT_TRUE(variableA.value().dtype() == mdio::constants::kInt16);

  ASSERT_TRUE(variableB.ok());
  ASSERT_TRUE(variableB.value().dtype() == mdio::constants::kInt32);
  std::filesystem::remove_all("test.zarr");
}

TEST(Variable, structArrayOpen) {
  // tests the struct array on open
  auto json_spec = GetJsonSpecStruct();

  // writes the file ....
  auto variable =
      mdio::Variable<>::Open(json_spec, mdio::constants::kCreateClean).result();

  auto open_json_spec = json_spec;

  // just supply the driver and kvstore:
  open_json_spec.erase("metadata");
  open_json_spec.erase("attributes");

  // opens the file ...
  variable = mdio::Variable<>::Open(open_json_spec).result();

  ASSERT_TRUE(variable.ok());

  auto domain = variable->dimensions();
  // TODO(BrianMichell) - we might assign a "byte" label
  // This is a feature of Tensorstore
  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y", ""));

  auto bytes = mdio::constants::kByte;

  // check it should have the right dtype
  ASSERT_TRUE(variable->dtype() == bytes);

  std::filesystem::remove_all("test.zarr");
}

TEST(Variable, open) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 11, 1};

  auto result = variable.slice(desc2, desc1);
  ASSERT_TRUE(result.ok());

  auto domain = result->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  EXPECT_THAT(domain.origin(), ::testing::ElementsAre(0, 5));

  EXPECT_THAT(domain.shape(), ::testing::ElementsAre(5, 6));

  std::filesystem::remove_all("name");
}

TEST(Variable, rank) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(variable.status().ok()) << variable.status();
  auto rank = variable.value().rank();
  EXPECT_EQ(rank, 2);
  std::filesystem::remove_all("name");
}

TEST(Variable, openMetadata) {
  auto json = json_good["metadata"];

  /*
  ZarrPartialMetadata partial_metadata;

  if (json.contains("shape")) {
      partial_metadata.shape = json["shape"].get<std::vector<Index>>();
  }

  if (json.contains("dimension_separator")) {
      if (json["dimension_separator"] == "/"){
          partial_metadata.dimension_separator =
  DimensionSeparator::kSlashSeparated; } else if (json["dimension_separator"] ==
  "."){
          // should not be possible with the current conventions ...
          partial_metadata.dimension_separator =
  DimensionSeparator::kDotSeparated; } else {
          // return an runtime error, dimension_separator is invalid
      }
  }

  // Extract chunks
  if (json.contains("chunks")) {
      partial_metadata.chunks = json["chunks"].get<std::vector<Index>>();
  }

  // Extract dtype
  if (json.contains("dtype")) {
      auto dtype_result = tensorstore::internal_zarr::ParseDType(json["dtype"]);
      if (dtype_result.ok()) {
          partial_metadata.dtype = *dtype_result;
      } else {
          // Handle error: dtype_result.status()
      }
  }

  // Extract compressor (if present)
  if (json.contains("compressor")) {
      auto compressor_result = Compressor::FromJson(json["compressor"]);
      if (compressor_result.ok()) {
          partial_metadata.compressor = *compressor_result;
      } else {
          // Handle error: compressor_result.status()
      }
  }
  */

  json["order"] = "C";
  json["filters"] = nullptr;
  json["fill_value"] = nullptr;
  json["zarr_format"] = 2;

  auto zarr_metadata = tensorstore::internal_zarr::ZarrMetadata::FromJson(json);

  std::string expectedString = R"(
  {
    "chunks":[100,50],
    "compressor": {
      "blocksize":0,
      "clevel":5,
      "cname":"lz4",
      "id":"blosc",
      "shuffle":-1
      },
    "dimension_separator":"/",
    "dtype":"<i2",
    "fill_value":null,
    "filters":null,
    "order":"C",
    "shape":[500,500],
    "zarr_format":2
  })";
  nlohmann::json expected = nlohmann::json::parse(expectedString);
  nlohmann::json actual = nlohmann::json(*zarr_metadata);
  EXPECT_EQ(actual, expected) << "Metadata did not match expected";
}

TEST(Variable, openExisting) {
  auto _variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  ::nlohmann::json open_json_spec = json_good;
  // just supply the driver and kvstore:
  open_json_spec.erase("metadata");
  open_json_spec.erase("attributes");

  // opens the file ...
  auto variable = mdio::Variable<>::Open(open_json_spec).result();

  ASSERT_TRUE(variable.ok());

  ASSERT_TRUE(variable->dtype() == mdio::constants::kInt16);

  auto domain = variable->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  std::filesystem::remove_all("name");
}

TEST(Variable, mismatchAttrs) {
  nlohmann::json json = GetJsonSpecStruct();
  auto variable = mdio::Variable<>::Open(json, mdio::constants::kCreateClean);
  ASSERT_TRUE(variable.status().ok()) << variable.status();

  // Mutate an element of attributes to mismatch the saved metadata
  json["attributes"]["dimension_names"] = {"x", "y", "z"};
  auto variable2 = mdio::Variable<>::Open(json);
  ASSERT_FALSE(variable2.status().ok()) << variable2.status();

  // Remove an element of attributes to mismatch the saved metadata
  json = GetJsonSpecStruct();
  json["attributes"].erase("dimension_names");
  auto variable3 = mdio::Variable<>::Open(json);
  ASSERT_FALSE(variable3.status().ok()) << variable3.status();

  std::filesystem::remove_all("test.zarr");
}

TEST(Variable, updateMetadata) {
  nlohmann::json json = GetJsonSpecStruct();
  auto variableRes =
      mdio::Variable<>::Open(json, mdio::constants::kCreateClean);
  ASSERT_TRUE(variableRes.status().ok()) << variableRes.status();
  auto variable = variableRes.value();

  // Update the metadata
  nlohmann::json attrs = {
      {"count", 100},
      {"min", -1000.0},
      {"max", 1000.0},
      {"sum", 0.0},
      {"sumSquares", 0.0},
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  auto attrsUpdateRes = variable.UpdateAttributes<float>(attrs);
  ASSERT_TRUE(attrsUpdateRes.status().ok()) << attrsUpdateRes.status();

  auto publishFuture = variable.PublishMetadata();
  EXPECT_TRUE(publishFuture.status().ok()) << publishFuture.status();
}

TEST(Variable, publishNoStats) {
  nlohmann::json json = GetJsonSpecStruct();
  auto variableRes =
      mdio::Variable<>::Open(json, mdio::constants::kCreateClean);
  ASSERT_TRUE(variableRes.status().ok()) << variableRes.status();
  auto variable = variableRes.value();

  auto publishFuture = variable.PublishMetadata();
  EXPECT_TRUE(publishFuture.status().ok()) << publishFuture.status();
}

TEST(Variable, publishNoAttrs) {
  nlohmann::json json = GetJsonSpecStruct();
  json["attributes"].erase("long_name");
  auto variableRes =
      mdio::Variable<>::Open(json, mdio::constants::kCreateClean);
  ASSERT_TRUE(variableRes.status().ok()) << variableRes.status();
  auto variable = variableRes.value();

  auto publishFuture = variable.PublishMetadata();
  EXPECT_TRUE(publishFuture.status().ok()) << publishFuture.status();
}

TEST(Variable, slice) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 11, 1};

  auto result = variable.slice(desc2, desc1);
  ASSERT_TRUE(result.ok());

  auto domain = result->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  EXPECT_THAT(domain.origin(), ::testing::ElementsAre(0, 5));

  EXPECT_THAT(domain.shape(), ::testing::ElementsAre(5, 6));

  std::filesystem::remove_all("name");
}

TEST(Variable, legacySliceDescriptor) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  // compile time example
  mdio::SliceDescriptor desc1 = {"x", 0, 5, 1};
  mdio::SliceDescriptor desc2 = {"y", 5, 11, 1};

  auto result = variable.slice(desc2, desc1);
  ASSERT_TRUE(result.ok());

  auto domain = result->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  EXPECT_THAT(domain.origin(), ::testing::ElementsAre(0, 5));

  EXPECT_THAT(domain.shape(), ::testing::ElementsAre(5, 6));

  std::filesystem::remove_all("name");
}

TEST(Variable, sliceTyped) {
  auto variable = mdio::Variable<mdio::dtypes::int16_t>::Open(
                      json_good, mdio::constants::kCreateClean)
                      .value();

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 11, 1};

  auto result = variable.slice(desc2, desc1);
  ASSERT_TRUE(result.ok());

  auto domain = result->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  EXPECT_THAT(domain.origin(), ::testing::ElementsAre(0, 5));

  EXPECT_THAT(domain.shape(), ::testing::ElementsAre(5, 6));

  std::filesystem::remove_all("name");
}

TEST(Variable, sliceVector) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 11, 1};
  std::vector<mdio::RangeDescriptor<mdio::Index>> descriptors = {desc1, desc2};

  auto result = variable.slice(descriptors);
  ASSERT_TRUE(result.ok());

  auto domain = result->dimensions();

  EXPECT_THAT(domain.labels(), ::testing::ElementsAre("x", "y"));

  EXPECT_THAT(domain.origin(), ::testing::ElementsAre(0, 5));

  EXPECT_THAT(domain.shape(), ::testing::ElementsAre(5, 6));

  std::filesystem::remove_all("name");
}

TEST(Variable, sliceEmptyVector) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  std::vector<mdio::RangeDescriptor<mdio::Index>> descriptors;

  auto result = variable.slice(descriptors);
  ASSERT_FALSE(result.ok());

  std::filesystem::remove_all("name");
}

TEST(Variable, sliceOverfullVector) {
  auto variable =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).value();

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  std::vector<mdio::RangeDescriptor<mdio::Index>> descriptors;
  for (int i = 0; i < 33; ++i) {
    descriptors.push_back(desc1);
  }

  auto result = variable.slice(descriptors);
  ASSERT_FALSE(result.ok());

  std::filesystem::remove_all("name");
}

TEST(Variable, labeledArray) {
  auto dimensions = tensorstore::Box({0, 0, 0}, {100, 200, 300});

  auto array = tensorstore::AllocateArray<mdio::dtypes::float32_t>(
      dimensions, mdio::ContiguousLayoutOrder::c, tensorstore::value_init,
      mdio::constants::kFloat32);

  array({0, 5, 0}) = 1337;

  auto domain = tensorstore::IndexDomainBuilder<3>(array.rank())
                    .origin(array.origin())
                    .shape(array.shape())
                    .labels({"x", "y", "z"})
                    .Finalize()
                    .value();

  auto labeled_array = mdio::LabeledArray{domain, array};

  // compile time example
  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 10, 1};

  auto result = labeled_array.slice(desc1, desc2).value();

  // have the same address in memory
  // std::cout << &result({0, 0, 0}) << std::endl;
  // std::cout << &array({0, 5, 0}) << std::endl;
  // //
  // std::cout << &result({1, 0, 0}) << std::endl;
  // std::cout << &array({1, 5, 0}) << std::endl;
  // //
  // std::cout << &result({0, 0, 200}) << std::endl;
  // std::cout << &array({0, 5, 200}) << std::endl;
  // you can do const_cast to discard the const qualifier.

  // if we really want to write ... then safety's are off, let's go ...
  *const_cast<float*>(&result({0, 1, 0})) = 42.0;
  // std::cout << array({0, 6, 0}) << std::endl;
}

TEST(Variable, userAttributes) {
  auto var1Res = mdio::Variable<>::Open(GetJsonSpecStruct(),
                                        mdio::constants::kCreateClean);
  ASSERT_TRUE(var1Res.status().ok()) << var1Res.status();
  auto var1 = var1Res.value();

  ASSERT_EQ(var1.GetAttributes(), nlohmann::json::object())
      << var1.GetAttributes().dump(4);

  nlohmann::json goodCpy = GetJsonSpecStruct();
  goodCpy["attributes"]["metadata"]["attributes"]["new_attr"] = "new_value";

  auto var2Res = mdio::Variable<>::Open(goodCpy, mdio::constants::kCreateClean);
  ASSERT_TRUE(var2Res.status().ok()) << var2Res.status();
  auto var2 = var2Res.value();
  EXPECT_EQ(var2.GetAttributes()["attributes"]["new_attr"], "new_value")
      << var2.getMetadata().dump(4);

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 10, 1};
  auto var2Sliced = var2.slice(desc1, desc2).value();

  EXPECT_FALSE(var2.was_updated())
      << "An update to the UserAttributes was detected but not solicited";
  EXPECT_FALSE(var2Sliced.was_updated())
      << "An update to the UserAttributes was detected but not solicited";

  auto toUpdate = var2.GetAttributes();
  toUpdate["attributes"]["new_attr"] = "updated_value";
  auto s = var2.UpdateAttributes(toUpdate);

  ASSERT_TRUE(s.status().ok()) << s.status();
  EXPECT_TRUE(var2.was_updated())
      << "An update to the UserAttributes was not detected";
  EXPECT_TRUE(var2Sliced.was_updated())
      << "An update to the UserAttributes was not detected";
}

// If a slice is "out of bounds" it should automatically get resized to the
// proper bounds.
TEST(Variable, outOfBoundsSlice) {
  auto var =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(var.ok());
  EXPECT_THAT(var->dimensions().shape(), ::testing::ElementsAre(500, 500));

  mdio::RangeDescriptor<mdio::Index> x_inbounds = {"x", 100, 250, 1};
  mdio::RangeDescriptor<mdio::Index> y_inbounds = {"y", 0, 500, 1};
  mdio::RangeDescriptor<mdio::Index> x_outbounds = {"x", 250, 1000, 1};

  auto inbounds = var.value().slice(x_inbounds, y_inbounds);
  EXPECT_TRUE(inbounds.status().ok()) << inbounds.status();

  auto outbounds = var.value().slice(x_outbounds, y_inbounds);
  EXPECT_TRUE(outbounds.status().ok()) << outbounds.status();

  auto badDomain = outbounds.value();
  EXPECT_THAT(badDomain.dimensions().shape(), ::testing::ElementsAre(250, 500))
      << badDomain.dimensions();

  auto var1 =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  mdio::RangeDescriptor<mdio::Index> illegal_start_stop = {"x", 500, 0, 1};
  auto illegal = var1.value().slice(illegal_start_stop);
  EXPECT_FALSE(illegal.status().ok())
      << "Start stop precondition was violated but still sliced";

  mdio::RangeDescriptor<mdio::Index> same_idx = {"x", 1, 1, 1};
  auto legal = var1.value().slice(same_idx);
  EXPECT_TRUE(legal.status().ok()) << legal.status();
}

TEST(Variable, noIntervals) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();

  auto intervalRes = var.get_intervals();
  ASSERT_TRUE(intervalRes.status().ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 2);
}

TEST(Variable, oneInterval) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();

  auto intervalRes = var.get_intervals("y");
  ASSERT_TRUE(intervalRes.status().ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 1);
}

TEST(Variable, mixedIntervals) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();

  auto intervalRes = var.get_intervals("x", "y", "z");
  ASSERT_TRUE(intervalRes.status().ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  ASSERT_EQ(intervals.size(), 2);
}

TEST(Variable, wrongIntervals) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();

  auto intervalRes = var.get_intervals("z");
  ASSERT_FALSE(intervalRes.status().ok()) << intervalRes.status();
}

TEST(Variable, slicedIntervals) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(varRes.status().ok()) << varRes.status();
  auto var = varRes.value();

  auto intervalRes = var.get_intervals();
  ASSERT_TRUE(intervalRes.status().ok()) << intervalRes.status();
  auto intervals = intervalRes.value();
  mdio::RangeDescriptor<mdio::Index> desc1, desc2;

  for (auto& interval : intervals) {
    if (interval.label.label() == "x") {
      desc1.label = interval.label;
      desc1.start = interval.inclusive_min + 100;
      desc1.stop = interval.exclusive_max - 100;
      desc1.step = 1;
    } else if (interval.label.label() == "y") {
      desc2.label = interval.label;
      desc2.start = interval.inclusive_min + 200;
      desc2.stop = interval.exclusive_max - 200;
      desc2.step = 1;
    } else {
      FAIL() << "Unexpected interval label: " << interval.label;
    }
  }
  auto slicedRes = var.slice(desc1, desc2);
  ASSERT_TRUE(slicedRes.status().ok()) << slicedRes.status();
  auto sliced = slicedRes.value();

  auto slicedIntervalRes = sliced.get_intervals();
  ASSERT_TRUE(slicedIntervalRes.status().ok()) << slicedIntervalRes.status();
  auto slicedIntervals = slicedIntervalRes.value();
  ASSERT_EQ(slicedIntervals.size(), 2);
  EXPECT_EQ(slicedIntervals[0].inclusive_min, 100);
  EXPECT_EQ(slicedIntervals[0].exclusive_max, 400);
  EXPECT_EQ(slicedIntervals[1].inclusive_min, 200);
  EXPECT_EQ(slicedIntervals[1].exclusive_max, 300);
}

TEST(VariableData, outOfBoundsSlice) {
  auto varRes =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(varRes.ok());
  auto var = varRes.value();

  auto varDataRes = var.Read().result();
  ASSERT_TRUE(varDataRes.ok());

  auto varData = varDataRes.value();
  mdio::RangeDescriptor<mdio::Index> x_inbounds = {"x", 100, 250, 1};
  mdio::RangeDescriptor<mdio::Index> y_inbounds = {"y", 0, 500, 1};

  auto inbounds = varData.slice(x_inbounds, y_inbounds);
  EXPECT_TRUE(inbounds.status().ok()) << inbounds.status();

  mdio::RangeDescriptor<mdio::Index> x_outbounds = {"x", 250, 1000, 1};
  auto outbounds = varData.slice(x_outbounds, y_inbounds);
  EXPECT_FALSE(outbounds.status().ok())
      << "Slicing out of bounds should fail but did not";
}

TEST(VariableSpec, open) {
  // this is the whole thing
  auto creation =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(creation.ok());

  EXPECT_THAT(creation->dimensions().shape(), ::testing::ElementsAre(500, 500));

  EXPECT_THAT(creation->dimensions().labels(),
              ::testing::ElementsAre("x", "y"));

  EXPECT_EQ(creation->dtype(), mdio::constants::kInt16);

  EXPECT_TRUE(creation->spec().status().ok());

  std::stringstream ss;
  ss << creation.value();
  EXPECT_FALSE(ss.fail());

  EXPECT_EQ(creation->get_variable_name(), std::string("name"));

  EXPECT_EQ(creation->get_long_name(), std::string("foooooo ....."));

  // clang-format off
        ::nlohmann::json json_spec = {
            {"driver", "zarr"},
            {"kvstore",
                {
                    {"driver", "file"},
                    {"path", "name"}
                }
            }
        };
  // clang-format on

  auto construction =
      mdio::Variable<>::Open(json_spec, mdio::constants::kOpen).result();

  ASSERT_TRUE(construction.ok());

  EXPECT_EQ(construction->get_variable_name(), std::string("name"));

  EXPECT_EQ(construction->get_long_name(), std::string("foooooo ....."));

  // cleanup:
  std::filesystem::remove_all("name");
}

TEST(VariableSpec, createVariable) {
  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  auto json_schema = mdio::internal::ValidateAndProcessJson(json_good).value();
  auto [json_store, metadata] = json_schema;

  auto result =
      mdio::internal::CreateVariable(json_store, metadata, std::move(options))
          .result();

  ASSERT_TRUE(result.ok());

  // cleanup:
  std::filesystem::remove_all("name");
}

TEST(VariableSpec, openVariable) {
  auto json_schema = mdio::internal::ValidateAndProcessJson(json_good).value();
  auto [json_store, metadata] = json_schema;

  auto create_result = mdio::internal::CreateVariable(
                           json_store, metadata, mdio::constants::kCreateClean)
                           .result();

  ASSERT_TRUE(create_result.ok());

  ::nlohmann::json open_json = {
      {"driver", "zarr"}, {"kvstore", {{"driver", "file"}, {"path", "name"}}}};

  auto open_result =
      mdio::internal::OpenVariable(open_json, mdio::constants::kOpen).result();

  ASSERT_TRUE(open_result.ok());

  EXPECT_THAT(open_result->dimensions().labels(),
              ::testing::ElementsAre("x", "y"));

  EXPECT_EQ(open_result->get_variable_name(), "name");

  // cleanup:
  std::filesystem::remove_all("name");
}

TEST(VariableSpec, sliceVariable) {
  mdio::TransactionalOpenOptions options;
  auto opt = options.Set(std::move(mdio::constants::kCreateClean));

  auto json_schema = mdio::internal::ValidateAndProcessJson(json_good).value();
  auto [json_store, metadata] = json_schema;

  auto result =
      mdio::internal::CreateVariable(json_store, metadata, std::move(options))
          .result();

  ASSERT_TRUE(result.ok());

  //
  ::nlohmann::json open_json = {
      {"driver", "zarr"}, {"kvstore", {{"driver", "file"}, {"path", "name"}}}};

  mdio::TransactionalOpenOptions options2;
  opt = options2.Set(std::move(mdio::constants::kOpen));

  auto results2 =
      mdio::internal::OpenVariable(open_json, std::move(options2)).result();

  ASSERT_TRUE(result.ok());

  EXPECT_EQ(result->get_variable_name(), "name");

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 5, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 5, 10, 1};
  mdio::RangeDescriptor<mdio::Index> descInvalid = {"z", 0, 500, 1};

  auto result3 = result.value().slice(desc1, desc2);
  EXPECT_EQ(result3.status().ok(), true);

  // variable will ignore slice in dimension it doesn't have
  // auto result4 = result.value().slice(desc1, descInvalid);
  // EXPECT_EQ(result4.status().ok(), false);

  // cleanup:
  std::filesystem::remove_all("name");
}

TEST(VariableData, testConstruction) {
  auto variableFuture =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);

  auto variableObject = variableFuture.result();

  EXPECT_TRUE(variableObject.ok());

  auto variableDataFuture = variableObject->Read();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  EXPECT_THAT(variableDataObject.value().dimensions().shape(),
              ::testing::ElementsAre(500, 500));
  EXPECT_EQ(variableDataObject.value().dtype(), mdio::constants::kInt16);

  std::stringstream ss;
  ss << variableDataObject.value();
  EXPECT_FALSE(ss.fail());

  std::filesystem::remove_all("name");
}

TEST(VariableData, rank) {
  auto variableFuture =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
  ASSERT_TRUE(variableFuture.status().ok()) << variableFuture.status();
  auto variableObject = variableFuture.value();
  auto variableDataFuture = variableObject.Read();
  ASSERT_TRUE(variableDataFuture.status().ok()) << variableDataFuture.status();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_EQ(variableDataObject.value().rank(), 2);

  std::filesystem::remove_all("name");
}

TEST(VariableData, writeChunkedData) {
  auto result =
      mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean).result();

  ASSERT_TRUE(result.ok());

  auto variableDataFuture = result->Read();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  // Create a 500x500 array with the data to write
  auto data = tensorstore::AllocateArray<int16_t>({500, 500});
  for (int i = 0; i < 500; i++) {
    for (int j = 0; j < 500; j++) {
      data(i, j) = int16_t(j + (i * 500));
    }
  }

  // Write the data to the store
  TENSORSTORE_ASSERT_OK(tensorstore::Write(data, result.value().get_store())
                            .commit_future.result());

  // cleanup:
  std::filesystem::remove_all("name");
}

TEST(VariableData, flattenedOffset) {
  auto json = PopulateStore(json_good).value();
  auto variableObject = mdio::Variable<>::Open(json).result();
  EXPECT_TRUE(variableObject.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 50, 100, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 0, 100, 1};
  variableObject = variableObject.value().slice(desc1, desc2);

  auto variableDataFuture = variableObject->Read();

  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  auto varData = variableDataObject.value();

  EXPECT_EQ(varData.get_flattened_offset(), 5000);
}

TEST(VariableData, inMemoryEdits) {
  auto json = PopulateStore(json_good).value();
  auto variableObject = mdio::Variable<>::Open(json).result();
  EXPECT_TRUE(variableObject.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 100, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 0, 100, 1};
  variableObject = variableObject.value().slice(desc1, desc2);

  auto variableDataFuture = variableObject->Read();

  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  // We can read the data like this, but we can't make edits to it.
  // auto data = variableDataObject.value().get_data_accessor();
  // std::cout << "Value at 0: " << data[0][0] << std::endl;
  // std::cout << "Value at 1: " << data[0][1] << std::endl;
  // std::cout << "Value at 10: " << data[0][10] << std::endl;
  // std::cout << "Value at 99: " << data[0][99] << std::endl;

  // Running the reinterperted cast will flaten it but we can make edits.
  auto data = reinterpret_cast<int16_t*>(
      variableDataObject.value().get_data_accessor().data());
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 1);
  EXPECT_EQ(data[10], 10);
  EXPECT_EQ(data[99], 99);

  data[0] = 10000;
  data[1] = 10001;
  data[10] = 10010;
  data[99] = 10099;

  EXPECT_EQ(data[0], 10000);
  EXPECT_EQ(data[1], 10001);
  EXPECT_EQ(data[10], 10010);
  EXPECT_EQ(data[99], 10099);
  auto slicedVariableDataObject =
      variableDataObject.value().slice(desc1, desc2);
  EXPECT_TRUE(slicedVariableDataObject.status().ok());
}

TEST(VariableData, checkInMemoryEdits) {
  auto json = PopulateStore(json_good).value();
  auto variableObject = mdio::Variable<>::Open(json).result();
  EXPECT_TRUE(variableObject.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 100, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 0, 100, 1};
  variableObject = variableObject.value().slice(desc1, desc2);

  auto variableDataFuture = variableObject->Read();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  auto data = static_cast<int16_t*>(
      variableDataObject.value().get_data_accessor().data());
  auto uncastedData = variableDataObject.value().get_data_accessor();
  // These are not the same because one is a const short int and one is a const
  // tensorstore::Array EXPECT_EQ(data[0], uncastedData[0][0]);
  // EXPECT_EQ(data[1], uncastedData[0][1]);
  // EXPECT_EQ(data[10], uncastedData[0][10]);
  // EXPECT_EQ(data[99], uncastedData[0][99]);

  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 1);
  EXPECT_EQ(data[10], 10);
  EXPECT_EQ(data[99], 99);
  // The prints will appear like expected but the gtests will fail
  // std::cout << "Value at 0: " << data[0] << std::endl; // 0
  // std::cout << "Value at 1: " << data[1] << std::endl; // 1
  // std::cout << "Value at 10: " << data[10] << std::endl; // 10
  // std::cout << "Value at 99: " << data[99] << std::endl; // 99
  // EXPECT_EQ(uncastedData[0][0], 0);
  // EXPECT_EQ(uncastedData[0][1], 1);
  // EXPECT_EQ(uncastedData[0][10], 10);
  // EXPECT_EQ(uncastedData[0][99], 99);
}

TEST(VariableData, nonOriginSlice) {
  auto json = PopulateStore(json_good).value();
  auto variableObject = mdio::Variable<>::Open(json).result();
  EXPECT_TRUE(variableObject.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 100, 110, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 100, 110, 1};

  auto slicedVariableObject = variableObject.value().slice(desc1, desc2);
  auto variableDataFuture = slicedVariableObject->Read();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  auto data = static_cast<int16_t*>(
      variableDataObject.value().get_data_accessor().data());
  // Pointer black magic. There's an offset to access the first element but then
  // it's flattened
  EXPECT_EQ(data[1100], -15436);
  EXPECT_EQ(data[1101], -15435);
  EXPECT_EQ(data[1110], -14936);
  EXPECT_EQ(data[1111], -14935);
}

TEST(VariableData, writeTest) {
  auto json = PopulateStore(json_good).value();
  auto variableObject = mdio::Variable<>::Open(json).result();
  EXPECT_TRUE(variableObject.ok());

  mdio::RangeDescriptor<mdio::Index> desc1 = {"x", 0, 100, 1};
  mdio::RangeDescriptor<mdio::Index> desc2 = {"y", 0, 100, 1};
  variableObject = variableObject.value().slice(desc1, desc2);

  auto variableDataFuture = variableObject->Read();
  auto variableDataObject = variableDataFuture.result();
  EXPECT_TRUE(variableDataObject.ok());

  auto data = reinterpret_cast<int16_t*>(
      variableDataObject.value().get_data_accessor().data());
  data[0] = 10000;
  auto variableDataWriteFuture =
      variableObject->Write(variableDataObject.value());

  EXPECT_TRUE(variableDataWriteFuture.result().ok());

  variableObject = mdio::Variable<>::Open(json).result();

  EXPECT_TRUE(variableObject.ok());

  desc1 = {"x", 0, 100, 1};
  desc2 = {"y", 0, 100, 1};

  variableObject = variableObject.value().slice(desc1, desc2);

  auto _variableDataFuture = variableObject->Read();
  auto _variableDataObject = _variableDataFuture.result();
  EXPECT_TRUE(_variableDataObject.ok());

  auto data2 = reinterpret_cast<int16_t*>(
      _variableDataObject.value().get_data_accessor().data());
  EXPECT_EQ(data2[0], 10000);

  /// opening with dtype is safer because it will handle the errors.
  auto variableObject2 =
      mdio::Variable<mdio::dtypes::int16_t>::Open(json).result();
  EXPECT_TRUE(variableObject2.ok());
  auto _variableDataObject2 = variableObject2->Read().result();
  auto data22 = _variableDataObject2.value().get_data_accessor().data();
  EXPECT_EQ(data22[0], 10000);

  std::filesystem::remove_all("test.zarr");
  std::filesystem::remove_all("name");
}

TEST(VariableData, fromVariableFillTest) {
  auto goodJson = json_good;
  // Issue #144 reports double with fill value of NaN filling with 0's
  goodJson["metadata"]["dtype"] = "<f8";
  // Copy dataset factory fill value pattern for double.
  goodJson["metadata"]["fill_value"] = nlohmann::json::value_t::null;
  auto variable = mdio::Variable<mdio::dtypes::float64_t>::Open(
                      goodJson, mdio::constants::kCreateClean)
                      .value();
  mdio::Result<mdio::VariableData<mdio::dtypes::float64_t>> variableData =
      mdio::from_variable<mdio::dtypes::float64_t>(variable);

  // Use std::isnan to check for NaN
  EXPECT_TRUE(std::isnan(variableData.value().get_data_accessor().data()[0]))
      << "Expected NaN filled array but got "
      << variableData.value().get_data_accessor().data()[0];
}

}  // namespace
