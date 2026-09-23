// Copyright 2026 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdio/header_variable.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "mdio/dataset.h"

namespace {

TEST(HeaderVariableTest, FromSpecV3Scalar) {
  auto spec = nlohmann::json::parse(R"({
    "_mdio_header_only": true,
    "driver": "zarr3",
    "kvstore": {
      "driver": "file",
      "path": "/tmp/dataset/segy_file_header"
    },
    "_mdio_array_metadata": {
      "shape": [],
      "data_type": {
        "name": "fixed_length_utf32",
        "configuration": {"length_bytes": 4}
      },
      "attributes": {
        "textHeader": "C01 EXAMPLE",
        "binaryHeader": {"job_id": 1}
      }
    }
  })");

  auto header = mdio::HeaderVariable<>::FromSpec(spec);
  ASSERT_TRUE(header.ok()) << header.status();
  EXPECT_EQ(header->get_variable_name(), "segy_file_header");
  EXPECT_EQ(header->rank(), 0);
  EXPECT_EQ(header->get_dtype_name(), "fixed_length_utf32");
  EXPECT_TRUE(header->GetAttributes()["attributes"].contains("textHeader"));
}

TEST(HeaderVariableTest, FromSpecV2Scalar) {
  auto spec = nlohmann::json::parse(R"({
    "_mdio_header_only": true,
    "driver": "zarr",
    "kvstore": {
      "driver": "file",
      "path": "/tmp/dataset/segy_file_header"
    },
    "_mdio_zarray": {
      "shape": [],
      "dtype": "<U1"
    },
    "_mdio_zattrs": {
      "textHeader": "C01 EXAMPLE"
    }
  })");

  auto header = mdio::HeaderVariable<>::FromSpec(spec);
  ASSERT_TRUE(header.ok()) << header.status();
  EXPECT_EQ(header->get_variable_name(), "segy_file_header");
  EXPECT_EQ(header->get_dtype_name(), "<U1");
}

TEST(HeaderVariableTest, ArrayReadWriteUnsupported) {
  auto spec = nlohmann::json::parse(R"({
    "_mdio_header_only": true,
    "driver": "zarr3",
    "kvstore": {"driver": "file", "path": "/tmp/dataset/segy_file_header"},
    "_mdio_array_metadata": {
      "shape": [],
      "data_type": "string",
      "attributes": {"textHeader": "C01"}
    }
  })");

  auto header = mdio::HeaderVariable<>::FromSpec(spec);
  ASSERT_TRUE(header.ok()) << header.status();

  auto read_future = header->Read();
  EXPECT_FALSE(read_future.status().ok());
}

TEST(HeaderVariableTest, UpdateAttributes) {
  auto spec = nlohmann::json::parse(R"({
    "_mdio_header_only": true,
    "driver": "zarr3",
    "kvstore": {"driver": "file", "path": "/tmp/dataset/segy_file_header"},
    "_mdio_array_metadata": {
      "shape": [],
      "data_type": "string",
      "attributes": {"textHeader": "C01"}
    }
  })");

  auto header = mdio::HeaderVariable<>::FromSpec(spec);
  ASSERT_TRUE(header.ok()) << header.status();
  EXPECT_FALSE(header->was_updated());

  nlohmann::json updated = header->GetAttributes();
  updated["attributes"]["marker"] = "updated";
  ASSERT_TRUE(header->UpdateAttributes(updated).ok());
  EXPECT_TRUE(header->was_updated());
  EXPECT_EQ(header->GetAttributes()["attributes"]["marker"], "updated");
}

TEST(HeaderVariableTest, OperatorPrint) {
  auto spec = nlohmann::json::parse(R"({
    "_mdio_header_only": true,
    "driver": "zarr3",
    "kvstore": {"driver": "file", "path": "/tmp/dataset/segy_file_header"},
    "_mdio_array_metadata": {
      "shape": [],
      "data_type": "string",
      "attributes": {"textHeader": "C01"}
    }
  })");

  auto header = mdio::HeaderVariable<>::FromSpec(spec);
  ASSERT_TRUE(header.ok()) << header.status();

  std::stringstream ss;
  ss << header.value();
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("segy_file_header"));
}

class DatasetMetadataOnlyOpenTest
    : public ::testing::TestWithParam<mdio::zarr::ZarrVersion> {
 protected:
  static constexpr const char* kHeaderName = "segy_file_header";

  void SetUp() override {
    version_ = GetParam();
    base_path_ = version_ == mdio::zarr::ZarrVersion::kV3
                     ? "zarrs/header_open_v3"
                     : "zarrs/header_open_v2";
    std::filesystem::remove_all(base_path_);
  }

  void TearDown() override { std::filesystem::remove_all(base_path_); }

  static nlohmann::json HeaderAttrs() {
    return {{"textHeader", "C01 EXAMPLE"}, {"binaryHeader", {{"job_id", 1}}}};
  }

  static void WriteJsonFile(const std::string& path,
                            const nlohmann::json& json) {
    std::filesystem::create_directories(
        std::filesystem::path(path).parent_path());
    std::ofstream out(path);
    out << json.dump(4);
  }

  nlohmann::json SimpleSchema() const {
    return nlohmann::json::parse(R"({
      "metadata": {
        "name": "header_open_test",
        "apiVersion": "1.0.0",
        "createdOn": "2023-12-12T15:02:06.413469-06:00"
      },
      "variables": [
        {
          "name": "data",
          "dataType": "float32",
          "dimensions": [
            {"name": "x", "size": 8},
            {"name": "y", "size": 8}
          ],
          "metadata": {
            "chunkGrid": {
              "name": "regular",
              "configuration": { "chunkShape": [8, 8] }
            }
          }
        },
        {
          "name": "x",
          "dataType": "int32",
          "dimensions": [{"name": "x", "size": 8}]
        },
        {
          "name": "y",
          "dataType": "int32",
          "dimensions": [{"name": "y", "size": 8}]
        }
      ]
    })");
  }

  void InjectUnsupportedVariable() {
    if (version_ == mdio::zarr::ZarrVersion::kV3) {
      nlohmann::json zarr_json = {
          {"zarr_format", 3},
          {"node_type", "array"},
          {"shape", nlohmann::json::array()},
          {"data_type",
           {{"name", "fixed_length_utf32"},
            {"configuration", {{"length_bytes", 4}}}}},
          {"chunk_grid",
           {{"name", "regular"},
            {"configuration", {{"chunk_shape", nlohmann::json::array()}}}}},
          {"chunk_key_encoding",
           {{"name", "default"}, {"configuration", {{"separator", "/"}}}}},
          {"fill_value", nullptr},
          {"codecs", nlohmann::json::array({{{"name", "bytes"}}})},
          {"attributes", HeaderAttrs()}};
      WriteJsonFile(base_path_ + "/" + kHeaderName + "/zarr.json", zarr_json);
      return;
    }

    nlohmann::json zarray = {{"zarr_format", 2},
                             {"shape", nlohmann::json::array()},
                             {"chunks", nlohmann::json::array()},
                             {"dtype", "<U1"},
                             {"compressor", nullptr},
                             {"fill_value", ""},
                             {"order", "C"},
                             {"filters", nullptr},
                             {"dimension_separator", "/"}};

    const std::string zmetadata_path = base_path_ + "/.zmetadata";
    nlohmann::json zmetadata;
    {
      std::ifstream in(zmetadata_path);
      in >> zmetadata;
    }
    zmetadata.at("metadata")[std::string(kHeaderName) + "/.zarray"] = zarray;
    zmetadata.at("metadata")[std::string(kHeaderName) + "/.zattrs"] =
        HeaderAttrs();
    WriteJsonFile(zmetadata_path, zmetadata);
  }

  mdio::zarr::ZarrVersion version_;
  std::string base_path_;
};

TEST_P(DatasetMetadataOnlyOpenTest, OpenKeepsUnsupportedVariableAsMetadata) {
  auto schema = SimpleSchema();
  auto created = mdio::Dataset::from_json(schema, base_path_, version_,
                                          mdio::constants::kCreateClean);
  ASSERT_TRUE(created.status().ok()) << created.status();

  InjectUnsupportedVariable();

  auto opened = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(opened.status().ok()) << opened.status();
  auto dataset = opened.value();

  EXPECT_TRUE(dataset.variables.contains_key("data"));
  EXPECT_FALSE(dataset.variables.contains_key(kHeaderName));
  ASSERT_TRUE(dataset.header_variables.contains_key(kHeaderName));

  auto data = dataset.variables.get<mdio::dtypes::float32_t>("data");
  ASSERT_TRUE(data.status().ok()) << data.status();
  auto data_read = data.value().Read();
  ASSERT_TRUE(data_read.status().ok()) << data_read.status();

  auto header = dataset.get_header_variable(kHeaderName);
  ASSERT_TRUE(header.status().ok()) << header.status();
  EXPECT_EQ(header.value().get_variable_name(), kHeaderName);
  EXPECT_EQ(header.value().rank(), 0);
  EXPECT_FALSE(header.value().Read().status().ok());
  EXPECT_EQ(header.value().GetAttributes()["attributes"]["textHeader"],
            "C01 EXAMPLE");

  std::stringstream printed;
  printed << dataset;
  EXPECT_THAT(printed.str(),
              ::testing::HasSubstr("Header Variable: segy_file_header"));

  nlohmann::json updated = header.value().GetAttributes();
  updated["attributes"]["testMarker"] = "cpp-mdio";
  ASSERT_TRUE(header.value().UpdateAttributes(updated).ok());
  auto commit = dataset.CommitMetadata();
  ASSERT_TRUE(commit.status().ok()) << commit.status();

  auto reopened = mdio::Dataset::Open(base_path_, mdio::constants::kOpen);
  ASSERT_TRUE(reopened.status().ok()) << reopened.status();
  auto reopened_header = reopened.value().get_header_variable(kHeaderName);
  ASSERT_TRUE(reopened_header.status().ok()) << reopened_header.status();
  EXPECT_EQ(reopened_header.value().GetAttributes()["attributes"]["testMarker"],
            "cpp-mdio");
}

INSTANTIATE_TEST_SUITE_P(
    ZarrVersions, DatasetMetadataOnlyOpenTest,
    ::testing::Values(mdio::zarr::ZarrVersion::kV2,
                      mdio::zarr::ZarrVersion::kV3),
    [](const ::testing::TestParamInfo<mdio::zarr::ZarrVersion>& info) {
      return info.param == mdio::zarr::ZarrVersion::kV3 ? "V3" : "V2";
    });

}  // namespace
