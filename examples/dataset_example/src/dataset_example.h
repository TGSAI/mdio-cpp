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

#include <fstream>
#include <nlohmann/json-schema.hpp>
#include <nlohmann/json.hpp>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/internal/path_util.h"
#include "absl/flags/parse.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "absl/types/span.h"

/**
 * @brief In MDIO collections of seismic data and other data is schematized.
 * A "Dataset" represents a collection of related data. This data has the
 * basic characteristics of "variable", dimension coordinate (inline/crossline
 * etc) and coordinates. A variable might be a stack or a horizon. A coordinate
 * is a variable that is associated with another variable within the same
 * domain. Here we provide a schema for a toy dataset.
 */
::nlohmann::json GetToyExample() {
  std::string schema = R"({
      "metadata": {
        "name": "seismic_3d",
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
    })";
  return ::nlohmann::json::parse(schema);
};
