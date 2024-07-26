#include <string>

/*! \mainpage The MDIO C++ Library
 # MDIO v1.1

Welcome to the MDIO - a descriptive format for energy data that is intended to
reduce storage costs,  improve the efficiency of I/O and make energy data and
workflows understandable and reproducible.


# Getting Started

This project uses CMake for the build and requires CMake 3.24 or better to
build. The project build is configured to use the fetch and install it 3rd party
dependencies.

To build the mdio, clone the repos and create a build directory:
```
mkdir build
cd build
cmake ..
```

Each mdio target has the preffix "mdio" in its name, to build the tests run
the following commands from the build directory:
```
make -j32 mdio_acceptance_test
```

The acceptance test will validate that the MDIO/C++ data can be read by Python's
Xarray. To ensure that the test passes, make sure your Python environment has
Xarray install, and run the acceptance test:
```
cd build/mdio/
./mdio_acceptance_test
```

The dataset and variables have their own test suite too:
```
make -j32 mdio_variable_test
make -j32 mdio_dataset_test
```

Each mdio library will provide an associated CMake alias, e.g. mdio::mdio
which can be use to link against mdio in your project.

## Key Features

- **Standardized Schema Compliance**: MDIO enforces a strict adherence to a
standardized schema for all data inputs, ensuring consistency, reliability, and
ease of data interoperability.

- **Cloud and On-Premise Storage**: MDIO is intended to efficiently support
energy datasets for local filesystems and HPC, and cloud object stores.
Currenlty MDIO supports gcs and s3.

- **Xarray and Python mdio Compatibility**: We prioritize compatibility with
popular data analysis tools like Xarray and Python mdio, allowing for
straightforward integration with your existing workflows.

- **High Scalability and Performance**: Scalable asynchronous and concurrent I/O
and tensor operations to handle complex and large energy datasets with ease,
ensuring that your data processing remains fast and efficient, even as your data
grows.

## Project Vision

Our vision is to provide a tool that not only simplifies the management of
energy data but also enhances the quality and depth of energy analysis. By
keeping units, dimensions, and other critical metadata with the data, MDIO
ensures that every dataset is not just a collection of numbers but a rich,
self-explaining narrative of energy insights.

## Target Audience

MDIO is built for a wide range of users, including:

- **New Energy Solution**: WRF wind data models and associated 2-d attributes.

### Project Roadmap

#### Phase 1: Bug Fixes and Stability
- **Goal**: Improve the reliability and stability of the tool.
- **Milestones**:
  - <span style="color:green">✔</span> Resolve high-importance defects (e.g.,
"Fix nghttp2 dependency").
  - <span style="color:green">✔</span> Complete critical updates to test suites.
  - <span style="color:green">✔</span> Address issues causing undefined behavior
or potential crashes (e.g., "Variable schematization").

#### Phase 2: I/O Performance Optimization
- **Goal**: Enhance the efficiency and performance of the tool.
- **Milestones**:
  - <span style="color:green">✔</span> Implement benchmarking to identify
performance bottlenecks.
  - Optimize high-impact areas identified through benchmarking.

#### Phase 3: Cost Reduction and Efficiency
- **Goal**: Reduce operational costs and improve efficiency.
- **Milestones**:
  - Build dataset factory methods for common use cases.
  - Streamline data management processes to reduce storage or compute costs
(e.g. "SEGY", "SEP" I/O).

#### Phase 4: Feature Completeness and Compliance
- **Goal**: Ensure feature completeness and compliance with standards.
- **Milestones**:
  - Complete implementation of deferred features (e.g., "numpy slice
semantics").

#### Phase 5: Process Optimization
- **Goal**: Optimize the MDIO library for seismic processing use cases.
- **Milestones**:
  - Analyze how clients are using MDIO and articulate bottlenecks and pain
points.
  - Improve documentation and examples to reduce the cognitive load of adopting
MDIO.
  - Resolve performance critical issues (runtime or storage costs).

## (dependency) Google's Tensorstore library

We use the [tensorstore](https://google.github.io/tensorstore/) library to
provide native a C/C++ interface to ZArr. If you're familiar with the Python
DASK library, tensorstore has very similar semantics when it comes to
manipulating data and creating asynchronous execution.

tensorstore is used under an Apache 2.0 license.

Relevant features of the tensorstore library are:

1. Read/write ZArr data in memory, from disk, with GCFS buckets (Google file
system).
2. Encode/decode data with some basic data compression BLOCS, zlib, lz4, zstd
and jpeg.
3. Concurrency; concurrent (multi-threaded) reads/writes.
4. Objects designed with async futures/promises architecture.
4. Logical array slicing operations.
5. Basic iterators.
6. Chunk aligned iterators.
7. Informative error messages and exception handling.

Nice to have features of tensorstore:

1. A **companion** Python library.
2. Transactions, used to stage groups of modifications.
3. Caching.
4. Progress monitoring.
5. Abstraction over the tensorstore "driver", read generic array data from
buckets.
 *
*/

static const std::string kDatasetSchema = R"(
{
   "title": "Dataset",
   "description": "Represents an MDIO v1 dataset.\n\nA dataset consists of variables and metadata.",
   "type": "object",
   "properties": {
      "variables": {
         "description": "Variables in MDIO dataset",
         "items": {
            "$ref": "#/$defs/Variable"
         },
         "title": "Variables",
         "type": "array"
      },
      "metadata": {
         "allOf": [
            {
               "$ref": "#/$defs/DatasetMetadata"
            }
         ],
         "description": "Dataset metadata."
      }
   },
   "$defs": {
      "AllUnits": {
         "additionalProperties": false,
         "description": "All Units.",
         "properties": {
            "unitsV1": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/LengthUnitModel"
                  },
                  {
                     "$ref": "#/$defs/TimeUnitModel"
                  },
                  {
                     "$ref": "#/$defs/AngleUnitModel"
                  },
                  {
                     "$ref": "#/$defs/DensityUnitModel"
                  },
                  {
                     "$ref": "#/$defs/SpeedUnitModel"
                  },
                  {
                     "$ref": "#/$defs/FrequencyUnitModel"
                  },
                  {
                     "$ref": "#/$defs/VoltageUnitModel"
                  },
                  {
                     "items": {
                        "anyOf": [
                           {
                              "$ref": "#/$defs/LengthUnitModel"
                           },
                           {
                              "$ref": "#/$defs/TimeUnitModel"
                           },
                           {
                              "$ref": "#/$defs/AngleUnitModel"
                           },
                           {
                              "$ref": "#/$defs/DensityUnitModel"
                           },
                           {
                              "$ref": "#/$defs/SpeedUnitModel"
                           },
                           {
                              "$ref": "#/$defs/FrequencyUnitModel"
                           },
                           {
                              "$ref": "#/$defs/VoltageUnitModel"
                           }
                        ]
                     },
                     "type": "array"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "title": "Unitsv1"
            }
         },
         "title": "AllUnits",
         "type": "object"
      },
      "AngleUnitEnum": {
         "description": "Enum class representing units of angle.",
         "enum": [
            "deg",
            "rad"
         ],
         "title": "AngleUnitEnum",
         "type": "string"
      },
      "AngleUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of angle.",
         "properties": {
            "angle": {
               "allOf": [
                  {
                     "$ref": "#/$defs/AngleUnitEnum"
                  }
               ],
               "description": "Unit of angle."
            }
         },
         "required": [
            "angle"
         ],
         "title": "AngleUnitModel",
         "type": "object"
      },
      "Blosc": {
         "additionalProperties": false,
         "description": "Data Model for Blosc options.",
         "properties": {
            "name": {
               "default": "blosc",
               "description": "Name of the compressor.",
               "title": "Name",
               "type": "string"
            },
            "algorithm": {
               "allOf": [
                  {
                     "$ref": "#/$defs/BloscAlgorithm"
                  }
               ],
               "default": "lz4",
               "description": "The Blosc compression algorithm to be used."
            },
            "level": {
               "default": 5,
               "description": "The compression level.",
               "maximum": 9,
               "minimum": 0,
               "title": "Level",
               "type": "integer"
            },
            "shuffle": {
               "allOf": [
                  {
                     "$ref": "#/$defs/BloscShuffle"
                  }
               ],
               "default": 1,
               "description": "The shuffle strategy to be applied before compression."
            },
            "blocksize": {
               "default": 0,
               "description": "The size of the block to be used for compression.",
               "title": "Blocksize",
               "type": "integer"
            }
         },
         "title": "Blosc",
         "type": "object"
      },
      "BloscAlgorithm": {
         "description": "Enum for Blosc algorithm options.",
         "enum": [
            "blosclz",
            "lz4",
            "lz4hc",
            "zlib",
            "zstd"
         ],
         "title": "BloscAlgorithm",
         "type": "string"
      },
      "BloscShuffle": {
         "description": "Enum for Blosc shuffle options.",
         "enum": [
            0,
            1,
            2,
            -1
         ],
         "title": "BloscShuffle",
         "type": "integer"
      },
      "CenteredBinHistogram": {
         "additionalProperties": false,
         "description": "Class representing a center bin histogram.",
         "properties": {
            "counts": {
               "description": "Count of each each bin.",
               "items": {
                  "type": "integer"
               },
               "title": "Counts",
               "type": "array"
            },
            "binCenters": {
               "description": "List of bin centers.",
               "items": {
                  "anyOf": [
                     {
                        "type": "number"
                     },
                     {
                        "type": "integer"
                     }
                  ]
               },
               "title": "Bincenters",
               "type": "array"
            }
         },
         "required": [
            "counts",
            "binCenters"
         ],
         "title": "CenteredBinHistogram",
         "type": "object"
      },
      "Coordinate": {
         "additionalProperties": false,
         "description": "An MDIO coordinate array with metadata.",
         "properties": {
            "dataType": {
               "allOf": [
                  {
                     "$ref": "#/$defs/ScalarType"
                  }
               ],
               "description": "Data type of coordinate."
            },
            "dimensions": {
               "anyOf": [
                  {
                     "items": {
                        "$ref": "#/$defs/NamedDimension"
                     },
                     "type": "array"
                  },
                  {
                     "items": {
                        "type": "string"
                     },
                     "type": "array"
                  }
               ],
               "description": "List of Dimension collection or reference to dimension names.",
               "title": "Dimensions"
            },
            "compressor": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/Blosc"
                  },
                  {
                     "$ref": "#/$defs/ZFP"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Compression settings.",
               "title": "Compressor"
            },
            "name": {
               "description": "Name of the array.",
               "title": "Name",
               "type": "string"
            },
            "longName": {
               "anyOf": [
                  {
                     "type": "string"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Fully descriptive name.",
               "title": "Longname"
            },
            "metadata": {
               "anyOf": [
                  {
                     "items": {
                        "anyOf": [
                           {
                              "$ref": "#/$defs/AllUnits"
                           },
                           {
                              "$ref": "#/$defs/UserAttributes"
                           }
                        ]
                     },
                     "type": "array"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Coordinate metadata.",
               "title": "Metadata"
            }
         },
         "required": [
            "dataType",
            "dimensions",
            "name"
         ],
         "title": "Coordinate",
         "type": "object"
      },
      "DatasetMetadata": {
         "additionalProperties": false,
         "description": "The metadata about the dataset.",
         "properties": {
            "name": {
               "description": "Name or identifier for the dataset.",
               "title": "Name",
               "type": "string"
            },
            "apiVersion": {
               "description": "The version of the MDIO API that the dataset complies with.",
               "title": "Apiversion",
               "type": "string"
            },
            "attributes": {
               "anyOf": [
                  {
                     "type": "object"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "User defined attributes as key/value pairs.",
               "title": "Attributes"
            }
         },
         "required": [
            "name",
            "apiVersion"
         ],
         "title": "DatasetMetadata",
         "type": "object"
      },
      "DensityUnitEnum": {
         "description": "Enum class representing units of density.",
         "enum": [
            "g/cm**3",
            "kg/m**3",
            "lb/gal"
         ],
         "title": "DensityUnitEnum",
         "type": "string"
      },
      "DensityUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of density.",
         "properties": {
            "density": {
               "allOf": [
                  {
                     "$ref": "#/$defs/DensityUnitEnum"
                  }
               ],
               "description": "Unit of density."
            }
         },
         "required": [
            "density"
         ],
         "title": "DensityUnitModel",
         "type": "object"
      },
      "EdgeDefinedHistogram": {
         "additionalProperties": false,
         "description": "A class representing an edge-defined histogram.",
         "properties": {
            "counts": {
               "description": "Count of each each bin.",
               "items": {
                  "type": "integer"
               },
               "title": "Counts",
               "type": "array"
            },
            "binEdges": {
               "description": "The left edges of the histogram bins.",
               "items": {
                  "anyOf": [
                     {
                        "type": "number"
                     },
                     {
                        "type": "integer"
                     }
                  ]
               },
               "title": "Binedges",
               "type": "array"
            },
            "binWidths": {
               "description": "The widths of the histogram bins.",
               "items": {
                  "anyOf": [
                     {
                        "type": "number"
                     },
                     {
                        "type": "integer"
                     }
                  ]
               },
               "title": "Binwidths",
               "type": "array"
            }
         },
         "required": [
            "counts",
            "binEdges",
            "binWidths"
         ],
         "title": "EdgeDefinedHistogram",
         "type": "object"
      },
      "FrequencyUnitEnum": {
         "const": "Hz",
         "description": "Enum class representing units of frequency.",
         "enum": [
            "Hz"
         ],
         "title": "FrequencyUnitEnum",
         "type": "string"
      },
      "FrequencyUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of frequency.",
         "properties": {
            "frequency": {
               "allOf": [
                  {
                     "$ref": "#/$defs/FrequencyUnitEnum"
                  }
               ],
               "description": "Unit of frequency."
            }
         },
         "required": [
            "frequency"
         ],
         "title": "FrequencyUnitModel",
         "type": "object"
      },
      "LengthUnitEnum": {
         "description": "Enum class representing metric units of length.",
         "enum": [
            "mm",
            "cm",
            "m",
            "km",
            "in",
            "ft",
            "yd",
            "mi"
         ],
         "title": "LengthUnitEnum",
         "type": "string"
      },
      "LengthUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of length.",
         "properties": {
            "length": {
               "allOf": [
                  {
                     "$ref": "#/$defs/LengthUnitEnum"
                  }
               ],
               "description": "Unit of length."
            }
         },
         "required": [
            "length"
         ],
         "title": "LengthUnitModel",
         "type": "object"
      },
      "NamedDimension": {
         "additionalProperties": false,
         "description": "Represents a single dimension with a name and size.",
         "properties": {
            "name": {
               "description": "Unique identifier for the dimension.",
               "title": "Name",
               "type": "string"
            },
            "size": {
               "description": "Total size of the dimension.",
               "exclusiveMinimum": 0,
               "title": "Size",
               "type": "integer"
            }
         },
         "required": [
            "name",
            "size"
         ],
         "title": "NamedDimension",
         "type": "object"
      },
      "RectilinearChunkGrid": {
         "additionalProperties": false,
         "description": "Represents a rectangular and irregularly spaced chunk grid.",
         "properties": {
            "name": {
               "default": "rectilinear",
               "description": "The name of the chunk grid.",
               "title": "Name",
               "type": "string"
            },
            "configuration": {
               "allOf": [
                  {
                     "$ref": "#/$defs/RectilinearChunkShape"
                  }
               ],
               "description": "Configuration of the irregular chunk grid."
            }
         },
         "required": [
            "configuration"
         ],
         "title": "RectilinearChunkGrid",
         "type": "object"
      },
      "RectilinearChunkShape": {
         "additionalProperties": false,
         "description": "Represents irregular chunk sizes along each dimension.",
         "properties": {
            "chunkShape": {
               "description": "Lengths of the chunk along each dimension of the array.",
               "items": {
                  "items": {
                     "type": "integer"
                  },
                  "type": "array"
               },
               "title": "Chunkshape",
               "type": "array"
            }
         },
         "required": [
            "chunkShape"
         ],
         "title": "RectilinearChunkShape",
         "type": "object"
      },
      "RegularChunkGrid": {
         "additionalProperties": false,
         "description": "Represents a rectangular and regularly spaced chunk grid.",
         "properties": {
            "name": {
               "default": "regular",
               "description": "The name of the chunk grid.",
               "title": "Name",
               "type": "string"
            },
            "configuration": {
               "allOf": [
                  {
                     "$ref": "#/$defs/RegularChunkShape"
                  }
               ],
               "description": "Configuration of the regular chunk grid."
            }
         },
         "required": [
            "configuration"
         ],
         "title": "RegularChunkGrid",
         "type": "object"
      },
      "RegularChunkShape": {
         "additionalProperties": false,
         "description": "Represents regular chunk sizes along each dimension.",
         "properties": {
            "chunkShape": {
               "description": "Lengths of the chunk along each dimension of the array.",
               "items": {
                  "type": "integer"
               },
               "title": "Chunkshape",
               "type": "array"
            }
         },
         "required": [
            "chunkShape"
         ],
         "title": "RegularChunkShape",
         "type": "object"
      },
      "ScalarType": {
         "description": "Scalar array data type.",
         "enum": [
            "bool",
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "float16",
            "float32",
            "float64",
            "longdouble",
            "complex64",
            "complex128",
            "clongdouble"
         ],
         "title": "ScalarType",
         "type": "string"
      },
      "SpeedUnitEnum": {
         "description": "Enum class representing units of speed.",
         "enum": [
            "m/s",
            "ft/s"
         ],
         "title": "SpeedUnitEnum",
         "type": "string"
      },
      "SpeedUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of speed.",
         "properties": {
            "speed": {
               "allOf": [
                  {
                     "$ref": "#/$defs/SpeedUnitEnum"
                  }
               ],
               "description": "Unit of speed."
            }
         },
         "required": [
            "speed"
         ],
         "title": "SpeedUnitModel",
         "type": "object"
      },
      "StructuredField": {
         "additionalProperties": false,
         "description": "Structured array field with name, format.",
         "properties": {
            "format": {
               "$ref": "#/$defs/ScalarType"
            },
            "name": {
               "title": "Name",
               "type": "string"
            }
         },
         "required": [
            "format",
            "name"
         ],
         "title": "StructuredField",
         "type": "object"
      },
      "StructuredType": {
         "additionalProperties": false,
         "description": "Structured array type with packed fields.",
         "properties": {
            "fields": {
               "items": {
                  "$ref": "#/$defs/StructuredField"
               },
               "title": "Fields",
               "type": "array"
            }
         },
         "required": [
            "fields"
         ],
         "title": "StructuredType",
         "type": "object"
      },
      "SummaryStatistics": {
         "additionalProperties": false,
         "description": "Data model for some statistics in MDIO v1 arrays.",
         "properties": {
            "count": {
               "description": "The number of data points.",
               "title": "Count",
               "type": "integer"
            },
            "sum": {
               "description": "The total of all data values.",
               "title": "Sum",
               "type": "number"
            },
            "sumSquares": {
               "description": "The total of all data values squared.",
               "title": "Sumsquares",
               "type": "number"
            },
            "min": {
               "description": "The smallest value in the variable.",
               "title": "Min",
               "type": "number"
            },
            "max": {
               "description": "The largest value in the variable.",
               "title": "Max",
               "type": "number"
            },
            "histogram": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/CenteredBinHistogram"
                  },
                  {
                     "$ref": "#/$defs/EdgeDefinedHistogram"
                  }
               ],
               "description": "Binned frequency distribution.",
               "title": "Histogram"
            }
         },
         "required": [
            "count",
            "sum",
            "sumSquares",
            "min",
            "max",
            "histogram"
         ],
         "title": "SummaryStatistics",
         "type": "object"
      },
      "TimeUnitEnum": {
         "description": "Enum class representing units of time.",
         "enum": [
            "ns",
            "\u00b5s",
            "ms",
            "s",
            "min",
            "h",
            "d"
         ],
         "title": "TimeUnitEnum",
         "type": "string"
      },
      "TimeUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of time.",
         "properties": {
            "time": {
               "allOf": [
                  {
                     "$ref": "#/$defs/TimeUnitEnum"
                  }
               ],
               "description": "Unit of time."
            }
         },
         "required": [
            "time"
         ],
         "title": "TimeUnitModel",
         "type": "object"
      },
      "UserAttributes": {
         "additionalProperties": false,
         "description": "User defined attributes as key/value pairs.",
         "properties": {
            "attributes": {
               "anyOf": [
                  {
                     "type": "object"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "User defined attributes as key/value pairs.",
               "title": "Attributes"
            }
         },
         "title": "UserAttributes",
         "type": "object"
      },
      "Variable": {
         "additionalProperties": false,
         "description": "An MDIO variable that has coordinates and metadata.",
         "properties": {
            "dataType": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/ScalarType"
                  },
                  {
                     "$ref": "#/$defs/StructuredType"
                  }
               ],
               "description": "Type of the array.",
               "title": "Datatype"
            },
            "dimensions": {
               "anyOf": [
                  {
                     "items": {
                        "$ref": "#/$defs/NamedDimension"
                     },
                     "type": "array"
                  },
                  {
                     "items": {
                        "type": "string"
                     },
                     "type": "array"
                  }
               ],
               "description": "List of Dimension collection or reference to dimension names.",
               "title": "Dimensions"
            },
            "compressor": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/Blosc"
                  },
                  {
                     "$ref": "#/$defs/ZFP"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Compression settings.",
               "title": "Compressor"
            },
            "name": {
               "description": "Name of the array.",
               "title": "Name",
               "type": "string"
            },
            "longName": {
               "anyOf": [
                  {
                     "type": "string"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Fully descriptive name.",
               "title": "Longname"
            },
            "coordinates": {
               "anyOf": [
                  {
                     "items": {
                        "$ref": "#/$defs/Coordinate"
                     },
                     "type": "array"
                  },
                  {
                     "items": {
                        "type": "string"
                     },
                     "type": "array"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Coordinates of the MDIO variable dimensions.",
               "title": "Coordinates"
            },
            "metadata": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/VariableMetadata"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Variable metadata."
            }
         },
         "required": [
            "dataType",
            "dimensions",
            "name"
         ],
         "title": "Variable",
         "type": "object"
      },
      "VariableMetadata": {
         "additionalProperties": false,
         "properties": {
            "chunkGrid": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/RegularChunkGrid"
                  },
                  {
                     "$ref": "#/$defs/RectilinearChunkGrid"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Chunk grid specification for the array.",
               "title": "Chunkgrid"
            },
            "unitsV1": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/LengthUnitModel"
                  },
                  {
                     "$ref": "#/$defs/TimeUnitModel"
                  },
                  {
                     "$ref": "#/$defs/AngleUnitModel"
                  },
                  {
                     "$ref": "#/$defs/DensityUnitModel"
                  },
                  {
                     "$ref": "#/$defs/SpeedUnitModel"
                  },
                  {
                     "$ref": "#/$defs/FrequencyUnitModel"
                  },
                  {
                     "$ref": "#/$defs/VoltageUnitModel"
                  },
                  {
                     "items": {
                        "anyOf": [
                           {
                              "$ref": "#/$defs/LengthUnitModel"
                           },
                           {
                              "$ref": "#/$defs/TimeUnitModel"
                           },
                           {
                              "$ref": "#/$defs/AngleUnitModel"
                           },
                           {
                              "$ref": "#/$defs/DensityUnitModel"
                           },
                           {
                              "$ref": "#/$defs/SpeedUnitModel"
                           },
                           {
                              "$ref": "#/$defs/FrequencyUnitModel"
                           },
                           {
                              "$ref": "#/$defs/VoltageUnitModel"
                           }
                        ]
                     },
                     "type": "array"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "title": "Unitsv1"
            },
            "statsV1": {
               "anyOf": [
                  {
                     "$ref": "#/$defs/SummaryStatistics"
                  },
                  {
                     "items": {
                        "$ref": "#/$defs/SummaryStatistics"
                     },
                     "type": "array"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Minimal summary statistics.",
               "title": "Statsv1"
            },
            "attributes": {
               "anyOf": [
                  {
                     "type": "object"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "User defined attributes as key/value pairs.",
               "title": "Attributes"
            }
         },
         "title": "VariableMetadata",
         "type": "object"
      },
      "VoltageUnitEnum": {
         "description": "Enum class representing units of voltage.",
         "enum": [
            "\u00b5V",
            "mV",
            "V"
         ],
         "title": "VoltageUnitEnum",
         "type": "string"
      },
      "VoltageUnitModel": {
         "additionalProperties": false,
         "description": "Model representing units of voltage.",
         "properties": {
            "voltage": {
               "allOf": [
                  {
                     "$ref": "#/$defs/VoltageUnitEnum"
                  }
               ],
               "description": "Unit of voltage."
            }
         },
         "required": [
            "voltage"
         ],
         "title": "VoltageUnitModel",
         "type": "object"
      },
      "ZFP": {
         "additionalProperties": false,
         "description": "Data Model for ZFP options.",
         "properties": {
            "name": {
               "default": "zfp",
               "description": "Name of the compressor.",
               "title": "Name",
               "type": "string"
            },
            "mode": {
               "$ref": "#/$defs/ZFPMode"
            },
            "tolerance": {
               "anyOf": [
                  {
                     "type": "number"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Fixed accuracy in terms of absolute error tolerance.",
               "title": "Tolerance"
            },
            "rate": {
               "anyOf": [
                  {
                     "type": "number"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Fixed rate in terms of number of compressed bits per value.",
               "title": "Rate"
            },
            "precision": {
               "anyOf": [
                  {
                     "type": "integer"
                  },
                  {
                     "type": "null"
                  }
               ],
               "default": null,
               "description": "Fixed precision in terms of number of uncompressed bits per value.",
               "title": "Precision"
            },
            "writeHeader": {
               "default": true,
               "description": "Encode array shape, scalar type, and compression parameters.",
               "title": "Writeheader",
               "type": "boolean"
            }
         },
         "required": [
            "mode"
         ],
         "title": "ZFP",
         "type": "object"
      },
      "ZFPMode": {
         "description": "Enum for ZFP algorithm modes.",
         "enum": [
            "fixed_accuracy",
            "fixed_precision",
            "fixed_rate",
            "reversible"
         ],
         "title": "ZFPMode",
         "type": "string"
      }
   },
   "$schema": "https://json-schema.org/draft/2020-12/schema",
   "additionalProperties": false,
   "required": [
      "variables",
      "metadata"
   ]
}
)";
