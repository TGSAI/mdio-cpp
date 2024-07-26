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

#include "dataset_factory.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "variable.h"

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

  nlohmann::json metadata = std::get<0>(res.value());
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    std::cout << variable.dump() << "\n\n";
  }
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

  nlohmann::json metadata = std::get<0>(res.value());
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    std::cout << variable.dump() << "\n\n";
  }
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

  nlohmann::json metadata = std::get<0>(res.value());
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    auto varStatus =
        mdio::Variable<>::Open(variable, mdio::constants::kCreateClean);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

std::string manifest = R"(
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

  nlohmann::json metadata = std::get<0>(res.value());
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    auto varStatus =
        mdio::Variable<>::Open(variable, mdio::constants::kCreateClean);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

TEST(xarray, open) {
  nlohmann::json j = nlohmann::json::parse(manifest);
  auto res = Construct(j, "zarrs/simple_dataset");
  ASSERT_TRUE(res.status().ok()) << res.status();

  nlohmann::json metadata = std::get<0>(res.value());
  std::vector<nlohmann::json> variables = std::get<1>(res.value());
  for (auto& variable : variables) {
    variable.erase("attributes");
    auto varStatus = mdio::Variable<>::Open(variable, mdio::constants::kOpen);
    EXPECT_TRUE(varStatus.status().ok()) << varStatus.status();
  }
}

}  // namespace