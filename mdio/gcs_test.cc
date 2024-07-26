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

namespace {

// TODO: User should point to their own GCS bucket here.
// You may find the test dataset at: TODO: Upload the test dataset to a public object store
std::string const GCS_PATH = "gs://USER_BUCKET";

std::string const fullToyManifest = R"(
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

TEST(GCS, create) {
    if (GCS_PATH == "gs://USER_BUCKET") {
        GTEST_SKIP() << "Please set the GCS_PATH to your own bucket in the gcs_test.cc file.";
    }
    nlohmann::json j = nlohmann::json::parse(fullToyManifest);
    auto dataset =
        mdio::Dataset::from_json(j, GCS_PATH, mdio::constants::kCreateClean);
    EXPECT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST(GCS, open) {
    if (GCS_PATH == "gs://USER_BUCKET") {
        GTEST_SKIP() << "Please set the GCS_PATH to your own bucket in the gcs_test.cc file.";
    }
    nlohmann::json j = nlohmann::json::parse(fullToyManifest);
    auto dataset = mdio::Dataset::Open(GCS_PATH, mdio::constants::kOpen);
    EXPECT_TRUE(dataset.status().ok()) << dataset.status(); // TODO: How will timeouts work with this? Can we simulate
                                                            // it or make it excessively short to force one?
}

TEST(GCS, write) {
    if (GCS_PATH == "gs://USER_BUCKET") {
        GTEST_SKIP() << "Please set the GCS_PATH to your own bucket in the gcs_test.cc file.";
    }
    nlohmann::json j = nlohmann::json::parse(fullToyManifest);
    auto dataset = mdio::Dataset::Open(GCS_PATH, mdio::constants::kOpen);
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
            auto data = reinterpret_cast<mdio::dtypes::float32_t*>(variable.get_data_accessor().data());
            data[0] = 3.14f;
        } else if (dtype == mdio::constants::kFloat64 && name == "velocity") {
            auto data = reinterpret_cast<mdio::dtypes::float64_t*>(variable.get_data_accessor().data());
            data[0] = 2.71828;
        } else if (dtype == mdio::constants::kInt16 && name == "image_inline") {
            auto data = reinterpret_cast<mdio::dtypes::int16_t*>(variable.get_data_accessor().data());
            data[0] = 0xff;
        } else if (dtype == mdio::constants::kByte && name == "image_headers") {
            auto data = reinterpret_cast<mdio::dtypes::byte_t*>(variable.get_data_accessor().data());
            for (int i = 0; i < 12; i++) {
                data[i] = std::byte(0xff);
            }
        } else if (name == "inline") {
            auto data = reinterpret_cast<mdio::dtypes::uint32_t*>(variable.get_data_accessor().data());
            for (uint32_t i = 0; i < 256; ++i) {
                data[i] = i;
            }
        } else if (name == "crossline") {
            auto data = reinterpret_cast<mdio::dtypes::uint32_t*>(variable.get_data_accessor().data());
            for (uint32_t i = 0; i < 512; ++i) {
                data[i] = i;
            }
        } else if (name == "depth") {
            auto data = reinterpret_cast<mdio::dtypes::uint32_t*>(variable.get_data_accessor().data());
            for (uint32_t i = 0; i < 384; ++i) {
                data[i] = i;
            }
        }
    }

    // Pair the Variables to the VariableData objects via name matching so we can write them out correctly
    // This makes an assumption that the vectors are 1-1
    std::map<std::size_t, std::size_t> variableIdxPair;
    for (std::size_t i = 0; i < openVariables.size(); i++) {
        for (std::size_t j = 0; j < readVariables.size(); j++) {
            if (openVariables[i].get_variable_name() == readVariables[j].variableName) {
                variableIdxPair[i] = j;
                break;
            }
        }
    }

    // Now we can write the Variables back to the store
    std::vector<mdio::WriteFutures> writeFutures;
    for (auto& idxPair : variableIdxPair) {
        auto write = openVariables[idxPair.second].Write(readVariables[idxPair.first]);
        writeFutures.emplace_back(write);
    }

    // Now we make sure all the writes were successful
    for (auto& w : writeFutures) {
        ASSERT_TRUE(w.status().ok()) << w.status();
    }
}

TEST(GCS, read) {
    if (GCS_PATH == "gs://USER_BUCKET") {
        GTEST_SKIP() << "Please set the GCS_PATH to your own bucket in the gcs_test.cc file.";
    }
    nlohmann::json j = nlohmann::json::parse(fullToyManifest);
    auto dataset = mdio::Dataset::Open(GCS_PATH, mdio::constants::kOpen);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();

    auto ds = dataset.value();

    for (auto& kv : ds.coordinates) {
        std::string key = kv.first;
        auto var = ds.get_variable(key);
        ASSERT_TRUE(var.status().ok()) << var.status();
    }
    auto future = ds.SelectField("image_headers", "cdp-x");
    ASSERT_TRUE(future.status().ok()) << future.status();
}

} // namespace