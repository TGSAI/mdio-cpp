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

#include "mdio/dataset_factory.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "mdio/variable.h"

namespace {

TEST(Toy, create) {
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
      "compressor": {"name": "zfp", "mode": "fixed_accuracy", "tolerance": 0.05},
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
  nlohmann::json j = nlohmann::json::parse(schema);
  auto res = Construct(j, "zarrs/toy_dataset");
  ASSERT_FALSE(res.status().ok()) << res.status();

  j["variables"][2].erase("compressor");
  res = Construct(j, "zarrs/toy_dataset");

  ASSERT_TRUE(res.status().ok()) << res.status();
}

TEST(Teapot, create) {
  std::string teapotSchema = R"(
        {
  "metadata": {
    "name": "teapot_dome_3d",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00",
    "attributes": {
      "textHeader": [
        "C 1 CLIENT: ROCKY MOUNTAIN OILFIELD TESTING CENTER                              ",
        "C 2 PROJECT: NAVAL PETROLEUM RESERVE #3 (TEAPOT DOME); NATRONA COUNTY, WYOMING  ",
        "C 3 LINE: 3D                                                                    ",
        "C 4                                                                             ",
        "C 5 THIS IS THE FILTERED POST STACK MIGRATION                                   ",
        "C 6                                                                             ",
        "C 7 INLINE 1, XLINE 1:   X COORDINATE: 788937  Y COORDINATE: 938845             ",
        "C 8 INLINE 1, XLINE 188: X COORDINATE: 809501  Y COORDINATE: 939333             ",
        "C 9 INLINE 188, XLINE 1: X COORDINATE: 788039  Y COORDINATE: 976674             ",
        "C10 INLINE NUMBER:    MIN: 1  MAX: 345  TOTAL: 345                              ",
        "C11 CROSSLINE NUMBER: MIN: 1  MAX: 188  TOTAL: 188                              ",
        "C12 TOTAL NUMBER OF CDPS: 64860   BIN DIMENSION: 110' X 110'                    ",
        "C13                                                                             ",
        "C14                                                                             ",
        "C15                                                                             ",
        "C16                                                                             ",
        "C17                                                                             ",
        "C18                                                                             ",
        "C19 GENERAL SEGY INFORMATION                                                    ",
        "C20 RECORD LENGHT (MS): 3000                                                    ",
        "C21 SAMPLE RATE (MS): 2.0                                                       ",
        "C22 DATA FORMAT: 4 BYTE IBM FLOATING POINT                                      ",
        "C23 BYTES  13- 16: CROSSLINE NUMBER (TRACE)                                     ",
        "C24 BYTES  17- 20: INLINE NUMBER (LINE)                                         ",
        "C25 BYTES  81- 84: CDP_X COORD                                                  ",
        "C26 BYTES  85- 88: CDP_Y COORD                                                  ",
        "C27 BYTES 181-184: INLINE NUMBER (LINE)                                         ",
        "C28 BYTES 185-188: CROSSLINE NUMBER (TRACE)                                     ",
        "C29 BYTES 189-192: CDP_X COORD                                                  ",
        "C30 BYTES 193-196: CDP_Y COORD                                                  ",
        "C31                                                                             ",
        "C32                                                                             ",
        "C33                                                                             ",
        "C34                                                                             ",
        "C35                                                                             ",
        "C36 Processed by: Excel Geophysical Services, Inc.                              ",
        "C37               8301 East Prentice Ave. Ste. 402                              ",
        "C38               Englewood, Colorado 80111                                     ",
        "C39               (voice) 303.694.9629 (fax) 303.771.1646                       ",
        "C40 END EBCDIC                                                                  "
      ]
    }
  },
  "variables": [
    {
      "name": "pstm_stack",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187},
        {"name": "time", "size": 1501}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128, 128] }
        },
        "statsV1": {
          "count": 50402079,
          "sum": -8594.54,
          "sumSquares": 40571268.0,
          "min": -8.375323,
          "max": 7.7237024,
          "histogram": {
            "binCenters": [
              -8.375323,
              -6.9117756,
              -5.448228,
              -3.98468,
              -2.5211322,
              -1.0575843,
              0.40596345,
              1.8695112,
              3.333059,
              4.796607,
              6.2601547,
              7.7237024
            ],
            "counts": [
              275,
              2317,
              28419,
              425196,
              4706898,
              77260778,
              13268643,
              1081341,
              59339,
              3612,
              197
            ]
          }
        }
    },
      "coordinates": ["inline", "crossline", "time", "cdp-x", "cdp-y", "trace_mask"],
      "compressor": {"name": "blosc", "algorithm": "zstd"}
    },
    {
      "name": "packed_headers",
      "dataType": {
        "fields": [
          {"name": "cdp-x", "format": "float32"},
          {"name": "cdp-y", "format": "float32"},
          {"name": "inline", "format": "uint16"},
          {"name": "crossline", "format": "uint16"}
        ]
      },
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128] }
        }
      },
      "coordinates": ["inline", "crossline", "cdp-x", "cdp-y", "trace_mask"],
      "compressor": {"name": "blosc", "algorithm": "lz4"}
    },
    {
      "name": "trace_mask",
      "dataType": "bool",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "coordinates": ["inline", "crossline", "cdp-x", "cdp-y"],
      "compressor": {"name": "blosc", "algorithm": "lz4"}
    },
    {
      "name": "inline",
      "dataType": "uint16",
      "dimensions": [{"name": "inline", "size": 345}]
    },
    {
      "name": "crossline",
      "dataType": "uint16",
      "dimensions": [{"name": "crossline", "size": 187}]
    },
    {
      "name": "time",
      "dataType": "uint16",
      "dimensions": [{"name": "time", "size": 1501}],
      "metadata": {
        "unitsV1": {"time":  "ms"}
      }
    },
    {
      "name": "cdp-x",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "unitsV1": {"length": "m"}
      }
    },
    {
      "name": "cdp-y",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "unitsV1": {"length": "m"}
      }
    }
  ]
}
)";

  nlohmann::json j = nlohmann::json::parse(teapotSchema);
  auto res = Construct(j, "zarrs/teapot_dome_3d");
  ASSERT_TRUE(res.status().ok()) << res.status();
}

TEST(Variable, createTeapot) {
  std::string teapotSchema = R"(
        {
  "metadata": {
    "name": "teapot_dome_3d",
    "apiVersion": "1.0.0",
    "createdOn": "2023-12-12T15:02:06.413469-06:00",
    "attributes": {    
      "textHeader": [
        "C 1 CLIENT: ROCKY MOUNTAIN OILFIELD TESTING CENTER                              ",
        "C 2 PROJECT: NAVAL PETROLEUM RESERVE #3 (TEAPOT DOME); NATRONA COUNTY, WYOMING  ",
        "C 3 LINE: 3D                                                                    ",
        "C 4                                                                             ",
        "C 5 THIS IS THE FILTERED POST STACK MIGRATION                                   ",
        "C 6                                                                             ",
        "C 7 INLINE 1, XLINE 1:   X COORDINATE: 788937  Y COORDINATE: 938845             ",
        "C 8 INLINE 1, XLINE 188: X COORDINATE: 809501  Y COORDINATE: 939333             ",
        "C 9 INLINE 188, XLINE 1: X COORDINATE: 788039  Y COORDINATE: 976674             ",
        "C10 INLINE NUMBER:    MIN: 1  MAX: 345  TOTAL: 345                              ",
        "C11 CROSSLINE NUMBER: MIN: 1  MAX: 188  TOTAL: 188                              ",
        "C12 TOTAL NUMBER OF CDPS: 64860   BIN DIMENSION: 110' X 110'                    ",
        "C13                                                                             ",
        "C14                                                                             ",
        "C15                                                                             ",
        "C16                                                                             ",
        "C17                                                                             ",
        "C18                                                                             ",
        "C19 GENERAL SEGY INFORMATION                                                    ",
        "C20 RECORD LENGHT (MS): 3000                                                    ",
        "C21 SAMPLE RATE (MS): 2.0                                                       ",
        "C22 DATA FORMAT: 4 BYTE IBM FLOATING POINT                                      ",
        "C23 BYTES  13- 16: CROSSLINE NUMBER (TRACE)                                     ",
        "C24 BYTES  17- 20: INLINE NUMBER (LINE)                                         ",
        "C25 BYTES  81- 84: CDP_X COORD                                                  ",
        "C26 BYTES  85- 88: CDP_Y COORD                                                  ",
        "C27 BYTES 181-184: INLINE NUMBER (LINE)                                         ",
        "C28 BYTES 185-188: CROSSLINE NUMBER (TRACE)                                     ",
        "C29 BYTES 189-192: CDP_X COORD                                                  ",
        "C30 BYTES 193-196: CDP_Y COORD                                                  ",
        "C31                                                                             ",
        "C32                                                                             ",
        "C33                                                                             ",
        "C34                                                                             ",
        "C35                                                                             ",
        "C36 Processed by: Excel Geophysical Services, Inc.                              ",
        "C37               8301 East Prentice Ave. Ste. 402                              ",
        "C38               Englewood, Colorado 80111                                     ",
        "C39               (voice) 303.694.9629 (fax) 303.771.1646                       ",
        "C40 END EBCDIC                                                                  "
      ]
    }
  },
  "variables": [
    {
      "name": "pstm_stack",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187},
        {"name": "time", "size": 1501}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128, 128] }
        },
        "statsV1": {
          "count": 50402079,
          "sum": -8594.54,
          "sumSquares": 40571268.0,
          "min": -8.375323,
          "max": 7.7237024,
          "histogram": {
            "binCenters": [
              -8.375323,
              -6.9117756,
              -5.448228,
              -3.98468,
              -2.5211322,
              -1.0575843,
              0.40596345,
              1.8695112,
              3.333059,
              4.796607,
              6.2601547,
              7.7237024
            ],
            "counts": [
              275,
              2317,
              28419,
              425196,
              4706898,
              77260778,
              13268643,
              1081341,
              59339,
              3612,
              197
            ]
          }
        }
    },
      "coordinates": ["inline", "crossline", "time", "cdp-x", "cdp-y", "trace_mask"],
      "compressor": {"name": "blosc", "algorithm": "zstd"}
    },
    {
      "name": "packed_headers",
      "dataType": {
        "fields": [
          {"name": "cdp-x", "format": "float32"},
          {"name": "cdp-y", "format": "float32"},
          {"name": "inline", "format": "uint16"},
          {"name": "crossline", "format": "uint16"}
        ]
      },
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": { "chunkShape": [128, 128] }
        }
      },
      "coordinates": ["inline", "crossline", "cdp-x", "cdp-y", "trace_mask"],
      "compressor": {"name": "blosc", "algorithm": "lz4"}
    },
    {
      "name": "trace_mask",
      "dataType": "bool",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "coordinates": ["inline", "crossline", "cdp-x", "cdp-y"],
      "compressor": {"name": "blosc", "algorithm": "lz4"}
    },
    {
      "name": "inline",
      "dataType": "uint16",
      "dimensions": [{"name": "inline", "size": 345}]
    },
    {
      "name": "crossline",
      "dataType": "uint16",
      "dimensions": [{"name": "crossline", "size": 187}]
    },
    {
      "name": "time",
      "dataType": "uint16",
      "dimensions": [{"name": "time", "size": 1501}],
      "metadata": {
        "unitsV1": {"time":  "ms"}
      }
    },
    {
      "name": "cdp-x",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "unitsV1": {"length": "m"}
      }
    },
    {
      "name": "cdp-y",
      "dataType": "float32",
      "dimensions": [
        {"name": "inline", "size": 345},
        {"name": "crossline", "size": 187}
      ],
      "metadata": {
        "unitsV1": {"length": "m"}
      }
    }
  ]
}
)";

  nlohmann::json j = nlohmann::json::parse(teapotSchema);
  auto res = Construct(j, "zarrs/teapot_dome_3d");
  ASSERT_TRUE(res.status().ok()) << res.status();

  nlohmann::json metadata = std::get<0>(res.value());  // NOLINT UNUSED
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    auto varStatus =
        mdio::Variable<>::Open(variable, mdio::constants::kCreateClean);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

/*NOLINT*/ std::string manifest = R"(
{
    "metadata": {
        "name": "simple",
        "apiVersion": "1.0.0",
        "createdOn": "2023-12-12T15:02:06.413469-06:00",
        "attributes": {    
        }
    },
    "variables": [
        {
            "name": "twoD",
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
            },
            "coordinates": ["x", "y"],
            "compressor": {"name": "blosc", "algorithm": "zstd"}
        },
        {
            "name": "x",
            "dataType": "uint64",
            "dimensions": [{"name": "x", "size": 10}]
        },
        {
            "name": "y",
            "dataType": "uint64",
            "dimensions": [{"name": "y", "size": 10}]
        }
    ]
}
    )";

TEST(Variable, simple) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto res = Construct(j, "zarrs/simple_dataset");
  ASSERT_TRUE(res.status().ok()) << res.status();

  nlohmann::json metadata = std::get<0>(res.value());  // NOLINT UNUSED
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    auto varStatus =
        mdio::Variable<>::Open(variable, mdio::constants::kCreateClean);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

TEST(Variable, maxSizeExceeded) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  // Set all Variables to exceed the maximum size
  j["variables"][0]["dimensions"][0]["size"] = 0x7fffffffffffffff;
  j["variables"][0]["dimensions"][1]["size"] = 0x7fffffffffffffff;
  j["variables"][1]["dimensions"][0]["size"] = 0x7fffffffffffffff;
  j["variables"][2]["dimensions"][0]["size"] = 0x7fffffffffffffff;

  auto res = Construct(j, "zarrs/simple_dataset");
  ASSERT_FALSE(res.status().ok())
      << "Construction succeeded despite exceeding maximum size";
}

TEST(Variable, maxSizeReached) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  // Set all Variables to reach the maximum size
  j["variables"][0]["dimensions"][0]["size"] = mdio::constants::kMaxSize;
  j["variables"][0]["dimensions"][1]["size"] = mdio::constants::kMaxSize;
  j["variables"][1]["dimensions"][0]["size"] = mdio::constants::kMaxSize;
  j["variables"][2]["dimensions"][0]["size"] = mdio::constants::kMaxSize;

  auto res = Construct(j, "zarrs/simple_dataset");
  ASSERT_TRUE(res.status().ok()) << res.status();
}

TEST(Xarray, open) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto res = Construct(j, "zarrs/simple_dataset");
  ASSERT_TRUE(res.status().ok()) << res.status();

  nlohmann::json metadata = std::get<0>(res.value());  // NOLINT UNUSED
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    variable.erase("attributes");
    auto varStatus = mdio::Variable<>::Open(variable, mdio::constants::kOpen);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

TEST(EncodeBase64, basicEncoding) {
  std::string input = "Hello, World!";
  std::string encoded = encode_base64(input);
  EXPECT_EQ(encoded, "SGVsbG8sIFdvcmxkIQ==");
}

TEST(EncodeBase64, emptyString) {
  std::string input = "";
  std::string encoded = encode_base64(input);
  EXPECT_EQ(encoded, "");
}

TEST(EncodeBase64, binaryData) {
  // Test with null bytes (as used for fill_value encoding)
  std::string input(8, '\0');
  std::string encoded = encode_base64(input);
  EXPECT_EQ(encoded, "AAAAAAAAAAA=");
}

TEST(TransformCompressor, nonBloscCompressorV2) {
  nlohmann::json input = {{"compressor", {{"name", "gzip"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Only blosc compressor is supported"));
}

TEST(TransformCompressor, nonBloscCompressorV3) {
  nlohmann::json input = {{"compressor", {{"name", "gzip"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV3);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Only blosc compressor is supported"));
}

TEST(TransformCompressor, missingCompressorName) {
  nlohmann::json input = {{"compressor", {{"algorithm", "zstd"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Compressor name must be specified"));
}

TEST(TransformCompressor, noCompressorV2) {
  nlohmann::json input = nlohmann::json::object();
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(variable["metadata"]["compressor"].is_null());
}

TEST(TransformCompressor, noCompressorV3) {
  // V3 with no compressor leaves the stub's default codecs untouched.
  nlohmann::json input = nlohmann::json::object();
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV3);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_FALSE(variable["metadata"].contains("codecs"));
}

TEST(TransformCompressor, bloscDefaultsV2) {
  nlohmann::json input = {{"compressor", {{"name", "blosc"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_TRUE(status.ok()) << status;
  const auto& comp = variable["metadata"]["compressor"];
  EXPECT_EQ(comp["id"], "blosc");
  EXPECT_EQ(comp["cname"], "lz4");
  EXPECT_EQ(comp["clevel"], 5);
  EXPECT_EQ(comp["shuffle"], 1);
  EXPECT_EQ(comp["blocksize"], 0);
}

TEST(TransformCompressor, bloscLegacyKeysV2) {
  // Legacy MDIO-cpp keys: "algorithm"/"level" plus integer shuffle.
  nlohmann::json input = {{"compressor",
                           {{"name", "blosc"},
                            {"algorithm", "zstd"},
                            {"level", 7},
                            {"shuffle", 2},
                            {"blocksize", 1024}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_TRUE(status.ok()) << status;
  const auto& comp = variable["metadata"]["compressor"];
  EXPECT_EQ(comp["cname"], "zstd");
  EXPECT_EQ(comp["clevel"], 7);
  EXPECT_EQ(comp["shuffle"], 2);
  EXPECT_EQ(comp["blocksize"], 1024);
}

TEST(TransformCompressor, bloscSchemaKeysV2) {
  // Schema keys: "cname"/"clevel" plus string shuffle enum.
  nlohmann::json input = {{"compressor",
                           {{"name", "blosc"},
                            {"cname", "zstd"},
                            {"clevel", 3},
                            {"shuffle", "bitshuffle"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_TRUE(status.ok()) << status;
  const auto& comp = variable["metadata"]["compressor"];
  EXPECT_EQ(comp["cname"], "zstd");
  EXPECT_EQ(comp["clevel"], 3);
  EXPECT_EQ(comp["shuffle"], 2);
}

TEST(TransformCompressor, clevelOutOfRangeV2) {
  nlohmann::json input = {{"compressor", {{"name", "blosc"}, {"clevel", 10}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV2);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Compressor level must be between 0 and 9"));
}

TEST(TransformCompressor, bloscDefaultsV3) {
  nlohmann::json input = {{"compressor", {{"name", "blosc"}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV3);
  ASSERT_TRUE(status.ok()) << status;
  const auto& codecs = variable["metadata"]["codecs"];
  ASSERT_EQ(codecs.size(), 2u);
  EXPECT_EQ(codecs[0]["name"], "bytes");
  EXPECT_EQ(codecs[1]["name"], "blosc");
  const auto& cfg = codecs[1]["configuration"];
  EXPECT_EQ(cfg["cname"], "lz4");
  EXPECT_EQ(cfg["clevel"], 5);
  EXPECT_EQ(cfg["shuffle"], "shuffle");
  EXPECT_EQ(cfg["blocksize"], 0);
}

TEST(TransformCompressor, bloscLegacyKeysV3) {
  // Legacy keys with integer shuffle should map to the V3 string enum.
  nlohmann::json input = {{"compressor",
                           {{"name", "blosc"},
                            {"algorithm", "zstd"},
                            {"level", 7},
                            {"shuffle", 0}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV3);
  ASSERT_TRUE(status.ok()) << status;
  const auto& cfg = variable["metadata"]["codecs"][1]["configuration"];
  EXPECT_EQ(cfg["cname"], "zstd");
  EXPECT_EQ(cfg["clevel"], 7);
  EXPECT_EQ(cfg["shuffle"], "noshuffle");
}

TEST(TransformCompressor, clevelOutOfRangeV3) {
  nlohmann::json input = {{"compressor", {{"name", "blosc"}, {"clevel", -1}}}};
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  auto status =
      transform_compressor(input, variable, mdio::zarr::ZarrVersion::kV3);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Compressor level must be between 0 and 9"));
}

TEST(ResolveBloscCname, prefersCnameOverAlgorithm) {
  nlohmann::json compressor = {{"cname", "zstd"}, {"algorithm", "lz4"}};
  EXPECT_EQ(resolve_blosc_cname(compressor), "zstd");
}

TEST(ResolveBloscCname, fallsBackToAlgorithm) {
  nlohmann::json compressor = {{"algorithm", "lz4"}};
  EXPECT_EQ(resolve_blosc_cname(compressor), "lz4");
}

TEST(ResolveBloscCname, defaultsToLz4) {
  nlohmann::json compressor = nlohmann::json::object();
  EXPECT_EQ(resolve_blosc_cname(compressor), "lz4");
}

TEST(ResolveBloscClevel, prefersClevelOverLevel) {
  nlohmann::json compressor = {{"clevel", 3}, {"level", 7}};
  auto result = resolve_blosc_clevel(compressor);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), 3);
}

TEST(ResolveBloscClevel, fallsBackToLevel) {
  nlohmann::json compressor = {{"level", 7}};
  auto result = resolve_blosc_clevel(compressor);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), 7);
}

TEST(ResolveBloscClevel, defaultsToFive) {
  nlohmann::json compressor = nlohmann::json::object();
  auto result = resolve_blosc_clevel(compressor);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), 5);
}

TEST(ResolveBloscClevel, rejectsOutOfRange) {
  for (int bad : {-1, 10}) {
    nlohmann::json compressor = {{"clevel", bad}};
    auto result = resolve_blosc_clevel(compressor);
    EXPECT_FALSE(result.ok()) << "clevel " << bad << " should be rejected";
  }
}

TEST(ResolveBloscClevel, acceptsBoundaries) {
  for (int ok : {0, 9}) {
    nlohmann::json compressor = {{"clevel", ok}};
    auto result = resolve_blosc_clevel(compressor);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(result.value(), ok);
  }
}

TEST(ResolveBloscBlocksize, returnsProvidedValue) {
  nlohmann::json compressor = {{"blocksize", 1024}};
  EXPECT_EQ(resolve_blosc_blocksize(compressor), 1024);
}

TEST(ResolveBloscBlocksize, defaultsToZero) {
  nlohmann::json compressor = nlohmann::json::object();
  EXPECT_EQ(resolve_blosc_blocksize(compressor), 0);
}

TEST(BloscShuffle, toIntFromStringEnum) {
  EXPECT_EQ(blosc_shuffle_to_int("noshuffle"), 0);
  EXPECT_EQ(blosc_shuffle_to_int("shuffle"), 1);
  EXPECT_EQ(blosc_shuffle_to_int("bitshuffle"), 2);
}

TEST(BloscShuffle, toIntPassesThroughInteger) {
  EXPECT_EQ(blosc_shuffle_to_int(0), 0);
  EXPECT_EQ(blosc_shuffle_to_int(1), 1);
  EXPECT_EQ(blosc_shuffle_to_int(2), 2);
}

TEST(BloscShuffle, toStringFromInteger) {
  EXPECT_EQ(blosc_shuffle_to_string(0), "noshuffle");
  EXPECT_EQ(blosc_shuffle_to_string(1), "shuffle");
  EXPECT_EQ(blosc_shuffle_to_string(2), "bitshuffle");
}

TEST(BloscShuffle, toStringPassesThroughString) {
  EXPECT_EQ(blosc_shuffle_to_string("bitshuffle"), "bitshuffle");
}

TEST(TransformMetadata, gcsPath) {
  nlohmann::json variable = {
      {"kvstore", {{"driver", "file"}, {"path", "myvar"}}}};
  auto status = transform_metadata("gs://my-bucket/path/to/dataset", variable);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(variable["kvstore"]["driver"], "gcs");
  EXPECT_EQ(variable["kvstore"]["bucket"], "my-bucket");
  EXPECT_THAT(variable["kvstore"]["path"].get<std::string>(),
              testing::HasSubstr("path/to/dataset"));
}

TEST(TransformMetadata, s3Path) {
  nlohmann::json variable = {
      {"kvstore", {{"driver", "file"}, {"path", "myvar"}}}};
  auto status = transform_metadata("s3://my-bucket/path/to/dataset", variable);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(variable["kvstore"]["driver"], "s3");
  EXPECT_EQ(variable["kvstore"]["bucket"], "my-bucket");
  EXPECT_THAT(variable["kvstore"]["path"].get<std::string>(),
              testing::HasSubstr("path/to/dataset"));
}

TEST(TransformMetadata, localPath) {
  nlohmann::json variable = {
      {"kvstore", {{"driver", "file"}, {"path", "myvar"}}}};
  auto status = transform_metadata("/local/path/to/dataset", variable);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(variable["kvstore"]["driver"], "file");
  EXPECT_FALSE(variable["kvstore"].contains("bucket"));
  EXPECT_THAT(variable["kvstore"]["path"].get<std::string>(),
              testing::HasSubstr("/local/path/to/dataset"));
}

TEST(GetDimensions, conflictingSizes) {
  nlohmann::json spec = R"({
    "variables": [
      {
        "name": "var1",
        "dimensions": [
          {"name": "x", "size": 100}
        ]
      },
      {
        "name": "var2",
        "dimensions": [
          {"name": "x", "size": 200}
        ]
      }
    ]
  })"_json;
  auto result = get_dimensions(spec);
  ASSERT_FALSE(result.status().ok());
  EXPECT_THAT(result.status().message(),
              testing::HasSubstr("conflicting sizes"));
}

TEST(GetDimensions, consistentSizes) {
  nlohmann::json spec = R"({
    "variables": [
      {
        "name": "var1",
        "dimensions": [
          {"name": "x", "size": 100},
          {"name": "y", "size": 50}
        ]
      },
      {
        "name": "var2",
        "dimensions": [
          {"name": "x", "size": 100}
        ]
      }
    ]
  })"_json;
  auto result = get_dimensions(spec);
  ASSERT_TRUE(result.status().ok()) << result.status();
  auto dims = result.value();
  EXPECT_EQ(dims["x"], 100);
  EXPECT_EQ(dims["y"], 50);
}

TEST(Construct, explicitV3Version) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto res = Construct(j, "zarrs/v3_dataset", mdio::zarr::ZarrVersion::kV3);
  ASSERT_TRUE(res.status().ok()) << res.status();

  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  // All variables should use zarr3 driver
  for (const auto& variable : variables) {
    EXPECT_EQ(variable["driver"], "zarr3");
  }
}

TEST(Construct, explicitV2Version) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto res = Construct(j, "zarrs/v2_dataset", mdio::zarr::ZarrVersion::kV2);
  ASSERT_TRUE(res.status().ok()) << res.status();

  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  // All variables should use zarr driver (V2)
  for (const auto& variable : variables) {
    EXPECT_EQ(variable["driver"], "zarr");
  }
}

TEST(ZarrLayout, v2Facts) {
  const ZarrLayout layout = LayoutFor(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(layout.version, mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(layout.dtype_key, "dtype");
  EXPECT_FALSE(layout.uses_codec_pipeline);
}

TEST(ZarrLayout, v3Facts) {
  const ZarrLayout layout = LayoutFor(mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(layout.version, mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(layout.dtype_key, "data_type");
  EXPECT_TRUE(layout.uses_codec_pipeline);
}

TEST(ZarrLayout, makeStubV2) {
  auto stub = LayoutFor(mdio::zarr::ZarrVersion::kV2).MakeStub();
  EXPECT_EQ(stub["driver"], "zarr");
  EXPECT_TRUE(stub["metadata"].contains("dtype"));
  EXPECT_TRUE(stub["metadata"].contains("chunks"));
  EXPECT_FALSE(stub["metadata"].contains("chunk_grid"));
}

TEST(ZarrLayout, makeStubV3) {
  auto stub = LayoutFor(mdio::zarr::ZarrVersion::kV3).MakeStub();
  EXPECT_EQ(stub["driver"], "zarr3");
  EXPECT_TRUE(stub["metadata"].contains("data_type"));
  EXPECT_TRUE(stub["metadata"].contains("chunk_grid"));
  EXPECT_EQ(stub["metadata"]["codecs"][0]["name"], "bytes");
}

TEST(ZarrLayout, setChunkShapeV2) {
  nlohmann::json variable = {{"metadata", nlohmann::json::object()}};
  LayoutFor(mdio::zarr::ZarrVersion::kV2)
      .SetChunkShape(variable, nlohmann::json::array({10, 20}));
  EXPECT_EQ(variable["metadata"]["chunks"], (nlohmann::json::array({10, 20})));
}

TEST(ZarrLayout, setChunkShapeV3) {
  nlohmann::json variable = {
      {"metadata",
       {{"chunk_grid", {{"configuration", nlohmann::json::object()}}}}}};
  LayoutFor(mdio::zarr::ZarrVersion::kV3)
      .SetChunkShape(variable, nlohmann::json::array({10, 20}));
  EXPECT_EQ(variable["metadata"]["chunk_grid"]["configuration"]["chunk_shape"],
            (nlohmann::json::array({10, 20})));
}

TEST(TransformChunks, usesExplicitChunkGrid) {
  nlohmann::json json = {
      {"metadata",
       {{"chunkGrid", {{"configuration", {{"chunkShape", {4, 8}}}}}}}}};
  nlohmann::json variable = {{"metadata", {{"shape", {16, 32}}}}};
  transform_chunks(json, variable, LayoutFor(mdio::zarr::ZarrVersion::kV2));
  EXPECT_EQ(variable["metadata"]["chunks"], (nlohmann::json::array({4, 8})));
}

TEST(TransformChunks, fallsBackToShapeWhenNoMetadata) {
  nlohmann::json json = nlohmann::json::object();
  nlohmann::json variable = {{"metadata", {{"shape", {16, 32}}}}};
  transform_chunks(json, variable, LayoutFor(mdio::zarr::ZarrVersion::kV2));
  EXPECT_EQ(variable["metadata"]["chunks"], (nlohmann::json::array({16, 32})));
}

TEST(TransformChunks, fallsBackToShapeWhenMetadataLacksChunkGrid) {
  nlohmann::json json = {{"metadata", {{"statsV1", nlohmann::json::object()}}}};
  nlohmann::json variable = {{"metadata", {{"shape", {16, 32}}}}};
  transform_chunks(json, variable, LayoutFor(mdio::zarr::ZarrVersion::kV3));
  EXPECT_EQ(variable["metadata"]["chunk_grid"]["configuration"]["chunk_shape"],
            (nlohmann::json::array({16, 32})));
}

TEST(TransformAttributes, longNameDefaultsToEmpty) {
  nlohmann::json json = {{"name", "x"}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["long_name"], "");
}

TEST(TransformAttributes, longNamePassesThrough) {
  nlohmann::json json = {{"name", "x"}, {"longName", "Inline"}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["long_name"], "Inline");
}

TEST(TransformAttributes, dimensionNamesFromObjectForm) {
  nlohmann::json json = {
      {"name", "image"},
      {"dimensions", {{{"name", "inline"}}, {{"name", "crossline"}}}}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["dimension_names"],
            (nlohmann::json::array({"inline", "crossline"})));
}

TEST(TransformAttributes, dimensionNamesFromStringForm) {
  nlohmann::json json = {{"name", "image"},
                         {"dimensions", {"inline", "depth"}}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["dimension_names"],
            (nlohmann::json::array({"inline", "depth"})));
}

TEST(TransformAttributes, dimensionNamesDefaultToVariableName) {
  // A dimension coordinate omits "dimensions"; it names itself.
  nlohmann::json json = {{"name", "inline"}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["dimension_names"],
            (nlohmann::json::array({"inline"})));
}

TEST(TransformAttributes, coordinatesExcludeDimensions) {
  // "inline" is also a dimension, so it must be dropped from coordinates.
  nlohmann::json json = {{"name", "image"},
                         {"dimensions", {"inline", "crossline"}},
                         {"coordinates", {"inline", "cdp_x", "cdp_y"}}};
  nlohmann::json variable = {{"attributes", nlohmann::json::object()}};
  transform_attributes(json, variable);
  EXPECT_EQ(variable["attributes"]["coordinates"], "cdp_x cdp_y");
}

}  // namespace
