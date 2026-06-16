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

#include <sstream>

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

}  // namespace
