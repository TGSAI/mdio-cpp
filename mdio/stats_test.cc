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

#include "mdio/stats.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

auto getCenterHist() {
  std::vector<float> binCenters = {1.0, 2.0, 3.0};
  std::vector<int32_t> counts = {1, 2, 3};
  return std::make_unique<mdio::internal::CenteredBinHistogram<float>>(
      binCenters, counts);
}

auto getEdgeHist() {
  std::vector<float> binEdges = {0.0, 1.0, 2.0, 3.0};
  std::vector<float> binWidths = {1.0, 1.0, 1.0};
  std::vector<int32_t> counts = {1, 2, 3};
  return std::make_unique<mdio::internal::EdgeDefinedHistogram<float>>(
      binEdges, binWidths, counts);
}

TEST(HistogramTest, constructCenterHist) {
  auto histogram = getCenterHist();
  nlohmann::json expected = {
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  EXPECT_EQ(histogram->getHistogram(), expected);
}

TEST(HistogramTest, constructEdgeHist) {
  auto histogram = getEdgeHist();
  nlohmann::json expected = {{"histogram",
                              {{"binEdges", {0.0, 1.0, 2.0, 3.0}},
                               {"binWidths", {1.0, 1.0, 1.0}},
                               {"counts", {1, 2, 3}}}}};
  EXPECT_EQ(histogram->getHistogram(), expected);
}

TEST(HistogramTest, centeredBinHistogramClone) {
  auto histogram = getCenterHist();
  auto clone = histogram->clone();
  EXPECT_EQ(clone->getHistogram(), histogram->getHistogram());
}

TEST(HistogramTest, edgeDefinedHistogramClone) {
  auto histogram = getEdgeHist();
  auto clone = histogram->clone();
  EXPECT_EQ(clone->getHistogram(), histogram->getHistogram());
}

TEST(HistogramTest, centeredFromJSON) {
  nlohmann::json expected = {
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  auto inertHist = mdio::internal::CenteredBinHistogram<float>({}, {});
  auto attrsRes = inertHist.FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto histogram = std::move(attrsRes.value());
  EXPECT_EQ(histogram->getHistogram(), expected);
}

TEST(HistogramTest, edgeFromJSON) {
  nlohmann::json expected = {{"histogram",
                              {{"binEdges", {0.0, 1.0, 2.0, 3.0}},
                               {"binWidths", {1.0, 1.0, 1.0}},
                               {"counts", {1, 2, 3}}}}};
  auto inertHist = mdio::internal::EdgeDefinedHistogram<float>({}, {}, {});
  auto attrsRes = inertHist.FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto histogram = std::move(attrsRes.value());
  EXPECT_EQ(histogram->getHistogram(), expected);
}

TEST(SummaryStatsTest, fromJson) {
  nlohmann::json expected = {
      {"count", 100},
      {"min", -1000.0},
      {"max", 1000.0},
      {"sum", 0.0},
      {"sumSquares", 0.0},
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  auto statsRes = mdio::internal::SummaryStats::FromJson(expected);
  ASSERT_TRUE(statsRes.status().ok()) << statsRes.status();
  auto stats = statsRes.value();
  EXPECT_EQ(stats.getBindable(), expected);
}

TEST(SummaryStatsTest, fromJsonInt) {
  nlohmann::json expected = {
      {"count", 100},
      {"min", -1000},
      {"max", 1000},
      {"sum", 0},
      {"sumSquares", 0},
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  auto statsRes = mdio::internal::SummaryStats::FromJson<int32_t>(expected);
  ASSERT_TRUE(statsRes.status().ok()) << statsRes.status();
  auto stats = statsRes.value();
  EXPECT_EQ(stats.getBindable(), expected);
}

TEST(SummaryStatsTest, fromJsonMissing) {
  nlohmann::json expected = {
      {"count", 100},
      {"min", -1000.0},
      // {"max", 1000.0},  // User forgot to add max field (required)
      {"sum", 0.0},
      {"sumSquares", 0.0},
      {"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}}};
  auto statsRes = mdio::internal::SummaryStats::FromJson(expected);
  std::cout << statsRes.status() << std::endl;
  ASSERT_FALSE(statsRes.status().ok()) << statsRes.status();
}

TEST(UserAttributesTest, fromJsonNoStats) {
  nlohmann::json expected = {{"attributes",
                              {{"foo", "bar"},
                               {"life", 42},
                               {"pi", 3.14159},
                               {"truth", true},
                               {"lies", false},
                               {"nothing", nullptr}}}};
  auto attrsRes = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto attrs = attrsRes.value();
  EXPECT_EQ(attrs.ToJson(), expected);
}

TEST(UserAttributesTest, fromJsonNoAttrs) {
  nlohmann::json expected = {
      {"statsV1",
       {{"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}},
        {"count", 100},
        {"min", -1000.0},
        {"max", 1000.0},
        {"sum", 0.0},
        {"sumSquares", 0.0}}}};
  auto attrsRes = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto attrs = attrsRes.value();
  EXPECT_EQ(attrs.ToJson(), expected);
}

TEST(UserAttributesTest, nothing) {
  nlohmann::json expected = nlohmann::json::object();
  auto attrs = mdio::UserAttributes::FromJson(expected);
  EXPECT_EQ(attrs.value().ToJson(), expected);
  auto none = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(none.status().ok()) << none.status();
  EXPECT_EQ(none.value().ToJson(), expected);
}

TEST(UserAttributesTest, fromJsonWithAttrs) {
  nlohmann::json expected = {
      {"statsV1",
       {{"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}},
        {"count", 100},
        {"min", -1000.0},
        {"max", 1000.0},
        {"sum", 0.0},
        {"sumSquares", 0.0}}},
      {"attributes",
       {{"foo", "bar"},
        {"life", 42},
        {"pi", 3.14159},
        {"truth", true},
        {"lies", false},
        {"nothing", nullptr}}}};
  auto attrsRes = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto attrs = attrsRes.value();
  EXPECT_EQ(attrs.ToJson(), expected);
}

TEST(UserAttributesTest, fromJsonStatsList) {
  nlohmann::json expected = {
      {"statsV1",
       {{{"histogram",
          {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}},
         {"count", 100},
         {"min", -1000.0},
         {"max", 1000.0},
         {"sum", 2000.0},
         {"sumSquares", 20000.0}},
        {{"histogram",
          {{"binEdges", {0.5, 1.5, 2.5, 3.5}},
           {"binWidths", {1.5, 3.0, 9.0}},
           {"counts", {3, 2, 1}}}},
         {"count", 789},
         {"min", -500.0},
         {"max", 500.0},
         {"sum", 1000.0},
         {"sumSquares", 15000.0}}}},
      {"attributes",
       {{"foo", "bar"},
        {"life", 42},
        {"pi", 3.14159},
        {"truth", true},
        {"lies", false},
        {"nothing", nullptr}}}};
  auto attrsRes = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();
  auto attrs = attrsRes.value();
  EXPECT_EQ(attrs.ToJson(), expected);
}

TEST(UserAttributesTest, fromDataset) {
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
  auto j = nlohmann::json::parse(schema);

  auto imgRes = mdio::UserAttributes::FromDatasetJson(j, "image");
  ASSERT_TRUE(imgRes.status().ok()) << imgRes.status();
  nlohmann::json expectedImage = nlohmann::json::object();
  expectedImage["statsV1"] = j["variables"][0]["metadata"]["statsV1"];
  expectedImage["attributes"] = j["variables"][0]["metadata"]["attributes"];

  auto boundUserAttrs = imgRes.value().ToJson();
  ASSERT_TRUE(boundUserAttrs.contains("statsV1"));
  ASSERT_TRUE(boundUserAttrs.contains("attributes"));

  // Floating point error is expected but gross.
  EXPECT_EQ(boundUserAttrs["attributes"], expectedImage["attributes"]);
  EXPECT_NEAR(boundUserAttrs["statsV1"]["count"],
              expectedImage["statsV1"]["count"], 1e-4);
  EXPECT_NEAR(boundUserAttrs["statsV1"]["min"], expectedImage["statsV1"]["min"],
              1e-4);
  EXPECT_NEAR(boundUserAttrs["statsV1"]["max"], expectedImage["statsV1"]["max"],
              1e-4);
  EXPECT_NEAR(boundUserAttrs["statsV1"]["sum"], expectedImage["statsV1"]["sum"],
              1e-4);
  EXPECT_NEAR(boundUserAttrs["statsV1"]["sumSquares"],
              expectedImage["statsV1"]["sumSquares"], 1e-4);
  // These should be ints
  EXPECT_EQ(boundUserAttrs["statsV1"]["histogram"]["binCenters"][0],
            expectedImage["statsV1"]["histogram"]["binCenters"][0]);
  EXPECT_EQ(boundUserAttrs["statsV1"]["histogram"]["binCenters"][1],
            expectedImage["statsV1"]["histogram"]["binCenters"][1]);
  EXPECT_EQ(boundUserAttrs["statsV1"]["histogram"]["counts"][0],
            expectedImage["statsV1"]["histogram"]["counts"][0]);
  EXPECT_EQ(boundUserAttrs["statsV1"]["histogram"]["counts"][1],
            expectedImage["statsV1"]["histogram"]["counts"][1]);

  auto missingVar = mdio::UserAttributes::FromDatasetJson(j, "xline");
  ASSERT_FALSE(missingVar.status().ok());
  EXPECT_EQ(missingVar.status().message(),
            "Variable xline not found in Dataset");
}

TEST(UserAttributes, locationAndReassignment) {
  nlohmann::json expected = {
      {"statsV1",
       {{"histogram", {{"binCenters", {1.0, 2.0, 3.0}}, {"counts", {1, 2, 3}}}},
        {"count", 100},
        {"min", -1000.0},
        {"max", 1000.0},
        {"sum", 0.0},
        {"sumSquares", 0.0}}},
      {"attributes",
       {{"foo", "bar"},
        {"life", 42},
        {"pi", 3.14159},
        {"truth", true},
        {"lies", false},
        {"nothing", nullptr}}}};

  auto attrsRes = mdio::UserAttributes::FromJson(expected);
  ASSERT_TRUE(attrsRes.status().ok()) << attrsRes.status();

  // This is the way I would like to do it. However, the copy constructor gets
  // deleted by the compiler and pivoting to a unique_ptr should be safer from
  // memory leaks and dangling pointers. auto attrs = attrsRes.value();
  // ASSERT_EQ(attrs.ToJson(), expected);
  // const void* attrsAddress = static_cast<const void*>(&attrs);
  // auto dittoAttrsRes = mdio::UserAttributes::FromJson(attrs.ToJson());
  // ASSERT_TRUE(dittoAttrsRes.status().ok()) << dittoAttrsRes.status();
  // auto dittoAttrs = dittoAttrsRes.value();
  // ASSERT_EQ(dittoAttrs.ToJson(), attrs.ToJson());
  // const void* dittoAttrsAddress = static_cast<const void*>(&dittoAttrs);
  // EXPECT_NE(attrsAddress, dittoAttrsAddress) << "Expected a different address
  // but got the same one!";

  // expected["attributes"]["foo"] = "baz";
  // dittoAttrs = mdio::UserAttributes::FromJson(expected).value();
  // EXPECT_EQ(dittoAttrs.ToJson(), expected);

  std::unique_ptr<mdio::UserAttributes> attrs =
      std::make_unique<mdio::UserAttributes>(attrsRes.value());
  const void* attrsAddress = static_cast<const void*>(attrs.get());
  auto dittoAttrsRes = mdio::UserAttributes::FromJson(attrs->ToJson());
  ASSERT_TRUE(dittoAttrsRes.status().ok()) << dittoAttrsRes.status();
  std::unique_ptr<mdio::UserAttributes> dittoAttrs =
      std::make_unique<mdio::UserAttributes>(dittoAttrsRes.value());
  const void* dittoAttrsAddress = static_cast<const void*>(dittoAttrs.get());
  EXPECT_NE(attrsAddress, dittoAttrsAddress)
      << "Expected a different address but got the same one!";

  expected["attributes"]["foo"] = "baz";
  dittoAttrs = std::make_unique<mdio::UserAttributes>(
      mdio::UserAttributes::FromJson(expected).value());
  EXPECT_EQ(dittoAttrs->ToJson(), expected);
  const void* newDittoAttrsAddress = static_cast<const void*>(dittoAttrs.get());
  EXPECT_NE(dittoAttrsAddress, newDittoAttrsAddress)
      << "Expected a different address but got the same one!";

  // auto newAttrs = std::move(attr)
}

}  // namespace
