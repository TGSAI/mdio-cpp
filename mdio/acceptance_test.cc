#include <filesystem>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "dataset_factory.h"
#include "mdio/dataset.h"

#include <cstdlib>

namespace {

namespace VariableTesting {

using float16_t = mdio::dtypes::float_16_t;

// TODO: Extend test coverage to include uint
nlohmann::json i2Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/i2"
        }
    }
)"_json;

nlohmann::json i4Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/i4"
        }
    }
)"_json;

nlohmann::json i8Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/i8"
        }
    }
)"_json;

nlohmann::json f2Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/f2"
        }
    }
)"_json;

nlohmann::json f4Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/f4"
        }
    }
)"_json;

nlohmann::json f8Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/f8"
        }
    }
)"_json;

nlohmann::json voidedBase = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/voided"
        }
    }
)"_json;

nlohmann::json u2Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/u2"
        }
    }
)"_json;

nlohmann::json u4Base = R"(
    {
        "driver": "zarr",
        "kvstore": {
            "driver": "file",
            "path": "zarrs/acceptance/u4"
        }
    }
)"_json;

// Test to set up some pre-existing elements for testing
TEST(Variable, SETUP) {
    mdio::TransactionalOpenOptions options;
    auto opt = options.Set(std::move(mdio::constants::kCreateClean));
    nlohmann::json i2Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/i2"
            },
            "metadata": {
                "dtype": "<i2",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "2-byte integer test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto i2Schema = mdio::internal::ValidateAndProcessJson(i2Spec).value();
    auto [i2Store, i2Metadata] = i2Schema;
    auto i2 = mdio::internal::CreateVariable(i2Store, i2Metadata, std::move(options));
    ASSERT_TRUE(i2.status().ok()) << i2.status();

    nlohmann::json i4Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/i4"
            },
            "metadata": {
                "dtype": "<i4",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "4-byte integer test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto i4Schema = mdio::internal::ValidateAndProcessJson(i4Spec).value();
    auto [i4Store, i4Metadata] = i4Schema;
    auto i4 = mdio::internal::CreateVariable(i4Store, i4Metadata, std::move(options));
    ASSERT_TRUE(i4.status().ok()) << i4.status();

    nlohmann::json i8Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/i8"
            },
            "metadata": {
                "dtype": "<i8",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "8-byte integer test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto i8Schema = mdio::internal::ValidateAndProcessJson(i8Spec).value();
    auto [i8Store, i8Metadata] = i8Schema;
    auto i8 = mdio::internal::CreateVariable(i8Store, i8Metadata, std::move(options));
    ASSERT_TRUE(i8.status().ok()) << i8.status();

    nlohmann::json f2Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/f2"
            },
            "metadata": {
                "dtype": "<f2",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "2-byte float test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto f2Schema = mdio::internal::ValidateAndProcessJson(f2Spec).value();
    auto [f2Store, f2Metadata] = f2Schema;
    auto f2 = mdio::internal::CreateVariable(f2Store, f2Metadata, std::move(options));
    ASSERT_TRUE(f2.status().ok()) << f2.status();

    nlohmann::json f4Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/f4"
            },
            "metadata": {
                "dtype": "<f4",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "4-byte float test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto f4Schema = mdio::internal::ValidateAndProcessJson(f4Spec).value();
    auto [f4Store, f4Metadata] = f4Schema;
    auto f4 = mdio::internal::CreateVariable(f4Store, f4Metadata, std::move(options));
    ASSERT_TRUE(f4.status().ok()) << f4.status();

    nlohmann::json f8Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/f8"
            },
            "metadata": {
                "dtype": "<f8",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "long_name": "8-byte float test",
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto f8Schema = mdio::internal::ValidateAndProcessJson(f8Spec).value();
    auto [f8Store, f8Metadata] = f8Schema;
    auto f8 = mdio::internal::CreateVariable(f8Store, f8Metadata, std::move(options));
    ASSERT_TRUE(f8.status().ok()) << f8.status();

    nlohmann::json voidedSpec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/voided"
            },
            "field": "integer16",
            "metadata": {
                "dtype": [["integer16", "<i2"], ["float32", "<f4"], ["double", "<f8"]],
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "struct array test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto voidedSchema = mdio::internal::ValidateAndProcessJson(voidedSpec).value();
    auto [voidedStore, voidedMetadata] = voidedSchema;
    auto voided = mdio::internal::CreateVariable(voidedStore, voidedMetadata, std::move(options));
    ASSERT_TRUE(voided.status().ok()) << voided.status();
}

TEST(Variable, open) {
    EXPECT_TRUE(mdio::Variable<>::Open(i2Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(i4Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(i8Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(f2Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(f4Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(f8Base, mdio::constants::kOpen).status().ok());
    EXPECT_TRUE(mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen).status().ok());
}

TEST(Variable, name) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_EQ(i2.value().get_variable_name(), "i2") << i2.value().get_variable_name();
    EXPECT_EQ(i4.value().get_variable_name(), "i4") << i4.value().get_variable_name();
    EXPECT_EQ(i8.value().get_variable_name(), "i8") << i8.value().get_variable_name();
    EXPECT_EQ(f2.value().get_variable_name(), "f2") << f2.value().get_variable_name();
    EXPECT_EQ(f4.value().get_variable_name(), "f4") << f4.value().get_variable_name();
    EXPECT_EQ(f8.value().get_variable_name(), "f8") << f8.value().get_variable_name();
    EXPECT_EQ(voided.value().get_variable_name(), "voided") << voided.value().get_variable_name();
}

TEST(Variable, longName) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_EQ(i2.value().get_long_name(), "2-byte integer test") << i2.value().get_long_name();
    EXPECT_EQ(i4.value().get_long_name(), "4-byte integer test") << i4.value().get_long_name();
    EXPECT_EQ(i8.value().get_long_name(), "8-byte integer test") << i8.value().get_long_name();
    EXPECT_EQ(f2.value().get_long_name(), "2-byte float test") << f2.value().get_long_name();
    EXPECT_EQ(f4.value().get_long_name(), "4-byte float test") << f4.value().get_long_name();
    EXPECT_EQ(f8.value().get_long_name(), "8-byte float test") << f8.value().get_long_name();
    EXPECT_EQ(voided.value().get_long_name(), "struct array test") << voided.value().get_long_name();
}

TEST(Variable, optionalAttrs) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_EQ(i2.value().getMetadata()["attributes"]["foo"], "bar") << i2.value().getMetadata();
    EXPECT_EQ(i4.value().getMetadata()["attributes"]["foo"], "bar") << i4.value().getMetadata();
    EXPECT_EQ(i8.value().getMetadata()["attributes"]["foo"], "bar") << i8.value().getMetadata();
    EXPECT_EQ(f2.value().getMetadata()["attributes"]["foo"], "bar") << f2.value().getMetadata();
    EXPECT_EQ(f4.value().getMetadata()["attributes"]["foo"], "bar") << f4.value().getMetadata();
    EXPECT_EQ(f8.value().getMetadata()["attributes"]["foo"], "bar") << f8.value().getMetadata();
    EXPECT_EQ(voided.value().getMetadata()["attributes"]["foo"], "bar") << voided.value().getMetadata();
}

TEST(Variable, namedDimensions) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_EQ(i2.value().getMetadata()["dimension_names"].size(), 2) << i2.value().getMetadata();
    EXPECT_EQ(i4.value().getMetadata()["dimension_names"].size(), 2) << i4.value().getMetadata();
    EXPECT_EQ(i8.value().getMetadata()["dimension_names"].size(), 2) << i8.value().getMetadata();
    EXPECT_EQ(f2.value().getMetadata()["dimension_names"].size(), 2) << f2.value().getMetadata();
    EXPECT_EQ(f4.value().getMetadata()["dimension_names"].size(), 2) << f4.value().getMetadata();
    EXPECT_EQ(f8.value().getMetadata()["dimension_names"].size(), 2) << f8.value().getMetadata();
    EXPECT_EQ(voided.value().getMetadata()["dimension_names"].size(), 2) << voided.value().getMetadata();
}

TEST(Variable, sliceByDimIdx) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    mdio::SliceDescriptor zeroIdxSlice = {0, 0, 5, 1};
    mdio::SliceDescriptor oneIdxSlice = {1, 0, 5, 1};

    auto i2Slice = i2.value().slice(zeroIdxSlice, oneIdxSlice);
    auto i4Slice = i4.value().slice(zeroIdxSlice, oneIdxSlice);
    auto i8Slice = i8.value().slice(zeroIdxSlice, oneIdxSlice);
    auto f2Slice = f2.value().slice(zeroIdxSlice, oneIdxSlice);
    auto f4Slice = f4.value().slice(zeroIdxSlice, oneIdxSlice);
    auto f8Slice = f8.value().slice(zeroIdxSlice, oneIdxSlice);
    auto voidedSlice = voided.value().slice(zeroIdxSlice, oneIdxSlice);

    EXPECT_TRUE(i2Slice.status().ok()) << i2Slice.status();
    EXPECT_TRUE(i4Slice.status().ok()) << i4Slice.status();
    EXPECT_TRUE(i8Slice.status().ok()) << i8Slice.status();
    EXPECT_TRUE(f2Slice.status().ok()) << f2Slice.status();
    EXPECT_TRUE(f4Slice.status().ok()) << f4Slice.status();
    EXPECT_TRUE(f8Slice.status().ok()) << f8Slice.status();
    EXPECT_TRUE(voidedSlice.status().ok()) << voidedSlice.status();
    EXPECT_THAT(i2Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i2Slice.value().dimensions();
    EXPECT_THAT(i4Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i4Slice.value().dimensions();
    EXPECT_THAT(i8Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i8Slice.value().dimensions();
    EXPECT_THAT(f2Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f2Slice.value().dimensions();
    EXPECT_THAT(f4Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f4Slice.value().dimensions();
    EXPECT_THAT(f8Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f8Slice.value().dimensions();
    EXPECT_THAT(voidedSlice.value().dimensions().shape(), ::testing::ElementsAre(5, 5, 14))
        << voidedSlice.value().dimensions();
}

TEST(Variable, xarrayCompatibility) {
    const char* basePath = std::getenv("PROJECT_BASE_PATH");
    if (!basePath) {
        std::cout << "PROJECT_BASE_PATH environment variable not set. Expecting to be in the 'build/mdio' directory." << std::endl;
        basePath = "../..";
    }

    std::string srcPath = std::string(basePath) + "/mdio/zarr_compatibility.py";
    std::string filePathBase = "./zarrs/acceptance/";
    std::string command = "python3 " + srcPath + " " + filePathBase;

    std::string i2 = command + "i2";
    std::string i4 = command + "i4";
    std::string i8 = command + "i8";
    std::string f2 = command + "f2";
    std::string f4 = command + "f4";
    std::string f8 = command + "f8";
    std::string voided = command + "voided";

    EXPECT_TRUE(std::system(i2.c_str()) == 0) << "Failed to read i2\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(i4.c_str()) == 0) << "Failed to read i4\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(i8.c_str()) == 0) << "Failed to read i8\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(f2.c_str()) == 0) << "Failed to read f2\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(f4.c_str()) == 0) << "Failed to read f4\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(f8.c_str()) == 0) << "Failed to read f8\n\tMore detailed error expected above...";
    EXPECT_TRUE(std::system(voided.c_str()) == 0) << "Failed to read voided\n\tMore detailed error expected above...";
}

TEST(Variable, dimensionUnits) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_TRUE(i2.value().getMetadata().contains("dimension_units")) << i2.value().getMetadata();
    EXPECT_TRUE(i4.value().getMetadata().contains("dimension_units")) << i4.value().getMetadata();
    EXPECT_TRUE(i8.value().getMetadata().contains("dimension_units")) << i8.value().getMetadata();
    EXPECT_TRUE(f2.value().getMetadata().contains("dimension_units")) << f2.value().getMetadata();
    EXPECT_TRUE(f4.value().getMetadata().contains("dimension_units")) << f4.value().getMetadata();
    EXPECT_TRUE(f8.value().getMetadata().contains("dimension_units")) << f8.value().getMetadata();
    EXPECT_TRUE(voided.value().getMetadata().contains("dimension_units")) << voided.value().getMetadata();
}

TEST(Variable, chunkSize) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_TRUE(i2.value().get_chunk_shape().status().ok()) << i2.value().get_chunk_shape().status();
    EXPECT_TRUE(i4.value().get_chunk_shape().status().ok()) << i4.value().get_chunk_shape().status();
    EXPECT_TRUE(i8.value().get_chunk_shape().status().ok()) << i8.value().get_chunk_shape().status();
    EXPECT_TRUE(f2.value().get_chunk_shape().status().ok()) << f2.value().get_chunk_shape().status();
    EXPECT_TRUE(f4.value().get_chunk_shape().status().ok()) << f4.value().get_chunk_shape().status();
    EXPECT_TRUE(f8.value().get_chunk_shape().status().ok()) << f8.value().get_chunk_shape().status();
    EXPECT_TRUE(voided.value().get_chunk_shape().status().ok()) << voided.value().get_chunk_shape().status();
}

TEST(Variable, getCompressor) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    auto i2Json = i2.value().get_spec();
    auto i4Json = i4.value().get_spec();
    auto i8Json = i8.value().get_spec();
    auto f2Json = f2.value().get_spec();
    auto f4Json = f4.value().get_spec();
    auto f8Json = f8.value().get_spec();
    auto voidedJson = voided.value().get_spec();

    ASSERT_TRUE(i2Json.status().ok()) << i2Json.status();
    ASSERT_TRUE(i4Json.status().ok()) << i4Json.status();
    ASSERT_TRUE(i8Json.status().ok()) << i8Json.status();
    ASSERT_TRUE(f2Json.status().ok()) << f2Json.status();
    ASSERT_TRUE(f4Json.status().ok()) << f4Json.status();
    ASSERT_TRUE(f8Json.status().ok()) << f8Json.status();
    ASSERT_TRUE(voidedJson.status().ok()) << voidedJson.status();

    EXPECT_TRUE(i2Json.value()["metadata"].contains("compressor")) << i2Json.value();
    EXPECT_TRUE(i4Json.value()["metadata"].contains("compressor")) << i4Json.value();
    EXPECT_TRUE(i8Json.value()["metadata"].contains("compressor")) << i8Json.value();
    EXPECT_TRUE(f2Json.value()["metadata"].contains("compressor")) << f2Json.value();
    EXPECT_TRUE(f4Json.value()["metadata"].contains("compressor")) << f4Json.value();
    EXPECT_TRUE(f8Json.value()["metadata"].contains("compressor")) << f8Json.value();
    EXPECT_TRUE(voidedJson.value()["metadata"].contains("compressor")) << voidedJson.value();
}

TEST(Variable, shape) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_THAT(i2.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << i2.value().dimensions();
    EXPECT_THAT(i4.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << i4.value().dimensions();
    EXPECT_THAT(i8.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << i8.value().dimensions();
    EXPECT_THAT(f2.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << f2.value().dimensions();
    EXPECT_THAT(f4.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << f4.value().dimensions();
    EXPECT_THAT(f8.value().dimensions().shape(), ::testing::ElementsAre(10, 10)) << f8.value().dimensions();
    EXPECT_THAT(voided.value().dimensions().shape(), ::testing::ElementsAre(10, 10, 14)) << voided.value().dimensions();
}

TEST(Variable, dtype) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    EXPECT_EQ(i2.value().dtype(), mdio::constants::kInt16) << i2.value().dtype();
    EXPECT_EQ(i4.value().dtype(), mdio::constants::kInt32) << i4.value().dtype();
    EXPECT_EQ(i8.value().dtype(), mdio::constants::kInt64) << i8.value().dtype();
    EXPECT_EQ(f2.value().dtype(), mdio::constants::kFloat16) << f2.value().dtype();
    EXPECT_EQ(f4.value().dtype(), mdio::constants::kFloat32) << f4.value().dtype();
    EXPECT_EQ(f8.value().dtype(), mdio::constants::kFloat64) << f8.value().dtype();
    EXPECT_EQ(voided.value().dtype(), mdio::constants::kByte) << voided.value().dtype();
}

TEST(Variable, domain) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    const mdio::Index EXPECTED_SHAPE = 10;
    EXPECT_EQ(i2.value().dimensions().shape()[0], EXPECTED_SHAPE) << i2.value().dimensions();
    EXPECT_EQ(i4.value().dimensions().shape()[0], EXPECTED_SHAPE) << i4.value().dimensions();
    EXPECT_EQ(i8.value().dimensions().shape()[0], EXPECTED_SHAPE) << i8.value().dimensions();
    EXPECT_EQ(f2.value().dimensions().shape()[0], EXPECTED_SHAPE) << f2.value().dimensions();
    EXPECT_EQ(f4.value().dimensions().shape()[0], EXPECTED_SHAPE) << f4.value().dimensions();
    EXPECT_EQ(f8.value().dimensions().shape()[0], EXPECTED_SHAPE) << f8.value().dimensions();
    EXPECT_EQ(voided.value().dimensions().shape()[0], EXPECTED_SHAPE) << voided.value().dimensions();

    EXPECT_EQ(i2.value().dimensions().rank(), 2) << i2.value().dimensions();
    EXPECT_EQ(i4.value().dimensions().rank(), 2) << i4.value().dimensions();
    EXPECT_EQ(i8.value().dimensions().rank(), 2) << i8.value().dimensions();
    EXPECT_EQ(f2.value().dimensions().rank(), 2) << f2.value().dimensions();
    EXPECT_EQ(f4.value().dimensions().rank(), 2) << f4.value().dimensions();
    EXPECT_EQ(f8.value().dimensions().rank(), 2) << f8.value().dimensions();
    EXPECT_EQ(voided.value().dimensions().rank(), 3) << voided.value().dimensions();
}

TEST(Variable, sliceByDimName) {
    auto i2 = mdio::Variable<>::Open(i2Base, mdio::constants::kOpen);
    auto i4 = mdio::Variable<>::Open(i4Base, mdio::constants::kOpen);
    auto i8 = mdio::Variable<>::Open(i8Base, mdio::constants::kOpen);
    auto f2 = mdio::Variable<>::Open(f2Base, mdio::constants::kOpen);
    auto f4 = mdio::Variable<>::Open(f4Base, mdio::constants::kOpen);
    auto f8 = mdio::Variable<>::Open(f8Base, mdio::constants::kOpen);
    auto voided = mdio::Variable<>::Open(voidedBase, mdio::constants::kOpen);
    ASSERT_TRUE(i2.status().ok()) << i2.status();
    ASSERT_TRUE(i4.status().ok()) << i4.status();
    ASSERT_TRUE(i8.status().ok()) << i8.status();
    ASSERT_TRUE(f2.status().ok()) << f2.status();
    ASSERT_TRUE(f4.status().ok()) << f4.status();
    ASSERT_TRUE(f8.status().ok()) << f8.status();
    ASSERT_TRUE(voided.status().ok()) << voided.status();

    mdio::SliceDescriptor inlineSlice = {"inline", 0, 5, 1};
    mdio::SliceDescriptor crosslineSpec = {"crossline", 0, 5, 1};
    auto i2Slice = i2.value().slice(inlineSlice, crosslineSpec);
    auto i4Slice = i4.value().slice(inlineSlice, crosslineSpec);
    auto i8Slice = i8.value().slice(inlineSlice, crosslineSpec);
    auto f2Slice = f2.value().slice(inlineSlice, crosslineSpec);
    auto f4Slice = f4.value().slice(inlineSlice, crosslineSpec);
    auto f8Slice = f8.value().slice(inlineSlice, crosslineSpec);
    auto voidedSlice = voided.value().slice(inlineSlice, crosslineSpec);

    EXPECT_TRUE(i2Slice.status().ok()) << i2.status();
    EXPECT_TRUE(i4Slice.status().ok()) << i4Slice.status();
    EXPECT_TRUE(i8Slice.status().ok()) << i8Slice.status();
    EXPECT_TRUE(f2Slice.status().ok()) << f2Slice.status();
    EXPECT_TRUE(f4Slice.status().ok()) << f4Slice.status();
    EXPECT_TRUE(f8Slice.status().ok()) << f8Slice.status();
    EXPECT_TRUE(voidedSlice.status().ok()) << voidedSlice.status();
    EXPECT_THAT(i2Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i2Slice.value().dimensions();
    EXPECT_THAT(i4Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i4Slice.value().dimensions();
    EXPECT_THAT(i8Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << i8Slice.value().dimensions();
    EXPECT_THAT(f2Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f2Slice.value().dimensions();
    EXPECT_THAT(f4Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f4Slice.value().dimensions();
    EXPECT_THAT(f8Slice.value().dimensions().shape(), ::testing::ElementsAre(5, 5)) << f8Slice.value().dimensions();
    EXPECT_THAT(voidedSlice.value().dimensions().shape(), ::testing::ElementsAre(5, 5, 14))
        << voidedSlice.value().dimensions();
}

// A test to clean up after the test suite
TEST(Variable, TEARDOWN) {
    std::filesystem::remove_all("zarrs/acceptance/i2");
    std::filesystem::remove_all("zarrs/acceptance/i4");
    std::filesystem::remove_all("zarrs/acceptance/i8");
    std::filesystem::remove_all("zarrs/acceptance/f2");
    std::filesystem::remove_all("zarrs/acceptance/f4");
    std::filesystem::remove_all("zarrs/acceptance/f8");
    std::filesystem::remove_all("zarrs/acceptance/voided");
    std::filesystem::remove_all("zarrs/acceptance");
    ASSERT_TRUE(true);
}

} // namespace VariableTesting

namespace VariableDataTest {

template <typename T = void,
          mdio::DimensionIndex R = mdio::dynamic_rank,
          mdio::ReadWriteMode M = mdio::ReadWriteMode::dynamic>
mdio::Variable<T, R, M> getVariable() {
    nlohmann::json i2Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/i2"
            }
        }
    )"_json;
    auto i2 = mdio::Variable<>::Open(i2Spec, (mdio::constants::kOpen));
    if (!i2.status().ok()) {
        std::cout << "Error opening i2: " << i2.status() << std::endl;
        return mdio::Variable<T, R, M>();
    }
    return i2.value();
}

TEST(VariableData, SETUP) {
    mdio::TransactionalOpenOptions options;
    auto opt = options.Set(std::move(mdio::constants::kCreateClean));
    nlohmann::json i2Spec = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/i2"
            },
            "metadata": {
                "dtype": "<i2",
                "shape": [10, 10],
                "chunks": [5, 5],
                "dimension_separator": "/",
                "compressor": {
                    "id": "blosc"
                }
            },
            "attributes": {
                "metadata": {
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "long_name": "2-byte integer test",
                "dimension_names": ["inline", "crossline"],
                "dimension_units": ["m", "m"]
            }
        }
    )"_json;
    auto i2Schema = mdio::internal::ValidateAndProcessJson(i2Spec).value();
    auto [i2Store, i2Metadata] = i2Schema;
    auto i2 = mdio::internal::CreateVariable(i2Store, i2Metadata, std::move(options));
    ASSERT_TRUE(i2.status().ok()) << i2.status();
}

TEST(VariableData, name) {
    auto variableData = getVariable().Read().value();
    EXPECT_EQ(variableData.variableName, "i2") << variableData.variableName;
}

TEST(VariableData, longName) {
    auto variableData = getVariable().Read().value();
    EXPECT_EQ(variableData.longName, "2-byte integer test") << variableData.longName;
}

TEST(VariableData, optionalAttrs) {
    auto variableData = getVariable().Read().value();
    EXPECT_EQ(variableData.metadata["attributes"]["foo"], "bar") << variableData.metadata.dump(4);
}

TEST(VariableData, namedDimensions) {
    auto variableData = getVariable().Read().value();
    EXPECT_EQ(variableData.metadata["dimension_names"].size(), 2) << variableData.metadata;
}

TEST(VariableData, sliceByDimIdx) {
    auto variableData = getVariable().Read().value();
    mdio::SliceDescriptor zeroIdxSlice = {0, 0, 5, 1};
    mdio::SliceDescriptor oneIdxSlice = {1, 0, 5, 1};
    auto slicedVariableData = variableData.slice(zeroIdxSlice, oneIdxSlice);
    ASSERT_TRUE(slicedVariableData.status().ok()) << slicedVariableData.status();
    EXPECT_THAT(slicedVariableData.value().domain().shape(), ::testing::ElementsAre(5, 5))
        << slicedVariableData.value().domain();
}

TEST(VariableData, sliceByDimName) {
    auto variableData = getVariable().Read().value();
    mdio::SliceDescriptor inlineSlice = {"inline", 0, 5, 1};
    mdio::SliceDescriptor crosslineSpec = {"crossline", 0, 5, 1};
    auto slicedVariableData = variableData.slice(inlineSlice, crosslineSpec);
    ASSERT_TRUE(slicedVariableData.status().ok()) << slicedVariableData.status();
    EXPECT_THAT(slicedVariableData.value().domain().shape(), ::testing::ElementsAre(5, 5))
        << slicedVariableData.value().domain();
}

TEST(VariableData, writeToStore) {
    auto variable = getVariable();
    auto variableData = variable.Read().value();
    auto data = reinterpret_cast<int16_t*>(variableData.get_data_accessor().data());
    data[0] = 0xff;
    auto writeFuture = variable.Write(variableData);
    writeFuture.result();
    EXPECT_TRUE(writeFuture.status().ok()) << writeFuture.status();

    auto variableCheck = getVariable().Read().value();
    auto dataCheck = reinterpret_cast<int16_t*>(variableCheck.get_data_accessor().data());
    EXPECT_EQ(dataCheck[0], 0xff) << dataCheck[0];
}

TEST(VariableData, dimensionUnits) {
    auto variableData = getVariable().Read().value();
    EXPECT_EQ(variableData.metadata["dimension_units"].size(), 2) << variableData.metadata;
}

TEST(VariableData, TEARDOWN) {
    std::filesystem::remove_all("zarrs/acceptance/i2");
    ASSERT_TRUE(true);
}

} // namespace VariableDataTest

namespace DatasetTest {

// clang-format off
std::string datasetManifest = R"(
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
    }
  ]
}
    )";
// clang-format on

TEST(DatasetSpec, valid) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto res = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(res.status().ok()) << res.status();
    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = res.value();
    std::vector<nlohmann::json> variables = std::get<1>(parsed);
    EXPECT_EQ(variables.size(), 9) << variables.size();
}

TEST(DatasetSpec, invalid) {
    // manifest["variables"][0] missing "name" field
    // Compressor for image_inline is also invalid
    std::string manifest = R"(
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

    nlohmann::json j = nlohmann::json::parse(manifest);
    auto res = Construct(j, "zarrs/acceptance");
    ASSERT_FALSE(res.status().ok()) << res.status();
}

TEST(Dataset, fillValue) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto ds = mdio::Dataset::from_json(
        j, "zarrs/acceptance", mdio::constants::kCreateClean);
    ASSERT_TRUE(ds.status().ok()) << ds.status();

    std::string key = "image_headers";
    auto var = ds.value().get_variable(key);
    ASSERT_TRUE(var.status().ok()) << var.status();
    auto vdf = var.value().Read();
    ASSERT_TRUE(vdf.status().ok()) << vdf.status();
    auto vd = vdf.value();

    auto data = reinterpret_cast<mdio::dtypes::byte_t*>(vd.get_data_accessor().data());
    std::byte zero = std::byte(0);
    for (int i = 0; i < 1000000; i++) {
        ASSERT_EQ(data[i], zero) << "Expected 0 at byte " << i << " but got " << static_cast<int>(data[i]);
    }

    // This still doesn't work. We end up with the same empty fill values {, , , , , , , , , , , }
    // auto dataRes = tensorstore::StaticDataTypeCast<mdio::dtypes::byte_t>(vd.get_data_accessor());
    // ASSERT_TRUE(dataRes.status().ok()) << dataRes.status();
    // auto d = dataRes.value();
    // std::cout << d[0][0] << std::endl;
}

TEST(Dataset, open) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(construct.status().ok()) << construct.status();

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(
        metadata, variables, mdio::constants::kCreateClean);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
}

TEST(Dataset, condensed) {
    std::string path = "zarrs/acceptance/";
    auto ds = mdio::Dataset::Open(path, mdio::constants::kOpen);
    ASSERT_TRUE(ds.status().ok()) << ds.status();

    std::string missingSlashPath = "zarrs/acceptance";
    auto ds2 = mdio::Dataset::Open(missingSlashPath, mdio::constants::kOpen);
    ASSERT_TRUE(ds2.status().ok()) << ds2.status();
}

TEST(Dataset, read) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(construct.status().ok()) << construct.status();

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
    auto ds = dataset.value();

    for (auto& kv : ds.coordinates) {
        std::string key = kv.first;
        auto var = ds.get_variable(key);
        ASSERT_TRUE(var.status().ok()) << var.status();
    }
}

TEST(Dataset, write) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
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
            auto data = reinterpret_cast<float*>(variable.get_data_accessor().data());
            data[0] = 3.14f;
        } else if (dtype == mdio::constants::kFloat64 && name == "velocity") {
            auto data = reinterpret_cast<double*>(variable.get_data_accessor().data());
            data[0] = 2.71828;
        } else if (dtype == mdio::constants::kInt16 && name == "image_inline") {
            auto data = reinterpret_cast<int16_t*>(variable.get_data_accessor().data());
            data[0] = 0xff;
        } else if (dtype == mdio::constants::kByte && name == "image_headers") {
            auto data = reinterpret_cast<mdio::dtypes::byte_t*>(variable.get_data_accessor().data());
            for (int i = 0; i < 12; i++) {
                data[i] = std::byte(0xff);
            }
        } else if (name == "inline") {
            auto data = reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
            for (uint32_t i = 0; i < 256; ++i) {
                data[i] = i;
            }
        } else if (name == "crossline") {
            auto data = reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
            for (uint32_t i = 0; i < 512; ++i) {
                data[i] = i;
            }
        } else if (name == "depth") {
            auto data = reinterpret_cast<uint32_t*>(variable.get_data_accessor().data());
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

    std::string fielded = "image_headers";
    ASSERT_TRUE(ds.SelectField(fielded, "cdp-x").status().ok());
    auto wf = ds.get_variable(fielded).value().Write(readVariables[4]);
    ASSERT_FALSE(wf.status().ok()) << wf.status();

    nlohmann::json imageJson = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/image"
            }
        }
    )"_json;

    nlohmann::json velocityJson = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/velocity"
            }
        }
    )"_json;

    nlohmann::json imageInlineJson = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/image_inline"
            }
        }
    )"_json;

    nlohmann::json imageHeadersJson = R"(
        {
            "driver": "zarr",
            "kvstore": {
                "driver": "file",
                "path": "zarrs/acceptance/image_headers"
            }
        }
    )"_json;

    auto image = mdio::Variable<>::Open(imageJson, mdio::constants::kOpen);
    auto velocity = mdio::Variable<>::Open(velocityJson, mdio::constants::kOpen);
    auto imageInline = mdio::Variable<>::Open(imageInlineJson, mdio::constants::kOpen);
    auto imageHeaders = mdio::Variable<>::Open(imageHeadersJson, mdio::constants::kOpen);

    ASSERT_TRUE(image.status().ok()) << image.status();
    ASSERT_TRUE(velocity.status().ok()) << velocity.status();
    ASSERT_TRUE(imageInline.status().ok()) << imageInline.status();
    ASSERT_TRUE(imageHeaders.status().ok()) << imageHeaders.status();

    auto imageData = image.result()->Read();
    auto velocityData = velocity.result()->Read();
    auto imageInlineData = imageInline.result()->Read();
    auto imageHeadersData = imageHeaders.result()->Read();

    ASSERT_TRUE(imageData.status().ok()) << imageData.status();
    ASSERT_TRUE(velocityData.status().ok()) << velocityData.status();
    ASSERT_TRUE(imageInlineData.status().ok()) << imageInlineData.status();
    ASSERT_TRUE(imageHeadersData.status().ok()) << imageHeadersData.status();

    auto castedImage = reinterpret_cast<float*>(imageData.value().get_data_accessor().data());
    auto castedVelociy = reinterpret_cast<double*>(velocityData.value().get_data_accessor().data());
    auto castedImageInline = reinterpret_cast<int16_t*>(imageInlineData.value().get_data_accessor().data());
    auto castedImageHeaders =
        reinterpret_cast<mdio::dtypes::byte_t*>(imageHeadersData.value().get_data_accessor().data());

    EXPECT_EQ(castedImage[0], 3.14f) << castedImage[0];
    EXPECT_EQ(castedVelociy[0], 2.71828) << castedVelociy[0];
    EXPECT_EQ(castedImageInline[0], 0xff) << castedImageInline[0];
    // EXPECT_EQ(castedImageHeaders[0], std::byte(0xffffffffffff)) << "Struct array was not correct value";
}

TEST(Dataset, name) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(construct.status().ok()) << construct.status();

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();

    auto ds = dataset.value();

    EXPECT_EQ(ds.getMetadata()["name"], "campos_3d") << ds.getMetadata();
}

TEST(Dataset, optionalAttrs) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(construct.status().ok()) << construct.status();

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();

    auto ds = dataset.value();

    EXPECT_TRUE(ds.getMetadata().contains("name")) << ds.getMetadata();
}

TEST(Dataset, isel) {
    std::string path = "zarrs/acceptance";
    auto dataset = mdio::Dataset::Open(path, mdio::constants::kOpen);

    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
    auto ds = dataset.value();

    mdio::SliceDescriptor desc1 = {"inline", 0, 5, 1};
    auto slice = ds.isel(desc1);
    ASSERT_TRUE(slice.status().ok());

    auto domain = slice->domain;
    ASSERT_EQ(domain.rank(), 3) << "This should have a rank of 3...";

    // Check depth range
    auto depthRange = domain[1];
    EXPECT_EQ(depthRange.interval().inclusive_min(), 0) << "Depth range should start at 0";
    EXPECT_EQ(depthRange.interval().exclusive_max(), 384) << "Depth range should end at 384";

    // Check crossline range
    auto crosslineRange = domain[0];
    EXPECT_EQ(crosslineRange.interval().inclusive_min(), 0) << "Crossline range should start at 0";
    EXPECT_EQ(crosslineRange.interval().exclusive_max(), 512) << "Crossline range should end at 512";

    // Check inline range
    auto inlineRange = domain[2];
    EXPECT_EQ(inlineRange.interval().inclusive_min(), 0) << "Inline range should start at 0";
    EXPECT_EQ(inlineRange.interval().exclusive_max(), 5) << "Inline range should end at 5";
}

TEST(Dataset, xarrayCompatible) {
    const char* basePath = std::getenv("PROJECT_BASE_PATH");
    if (!basePath) {
        std::cout << "PROJECT_BASE_PATH environment variable not set. Expecting to be in the 'build/mdio' directory." << std::endl;
        basePath = "../..";
    }

    std::string srcPath = std::string(basePath) + "/mdio/xarray_compatibility_test.py";
    std::string datasetPath = "./zarrs/acceptance";
    std::string command = "python3 " + srcPath + " " + datasetPath + " False";
    int status = system(command.c_str());
    ASSERT_TRUE(status == 0)
        << "xarray compatibility test failed without consolidated metadata\n\tThere was some expected output above...";

    command = "python3 " + srcPath + " " + datasetPath + " True";
    status = system(command.c_str());
    ASSERT_TRUE(status == 0)
        << "xarray compatibility test failed with consolidated metadata\n\tThere was some expected output above...";
}

TEST(Dataset, listVars) {
    nlohmann::json j = nlohmann::json::parse(datasetManifest);
    auto construct = Construct(j, "zarrs/acceptance");
    ASSERT_TRUE(construct.status().ok()) << construct.status();

    std::tuple<nlohmann::json, std::vector<nlohmann::json>> parsed = construct.value();
    nlohmann::json metadata = std::get<0>(parsed);
    std::vector<nlohmann::json> variables = std::get<1>(parsed);

    auto dataset = mdio::Dataset::Open(metadata, variables, mdio::constants::kOpen);
    ASSERT_TRUE(dataset.status().ok()) << dataset.status();

    std::vector<std::string> varList = dataset.value().variables.get_keys();
    for (auto& name : varList) {
        std::cout << "Variable name: " << name << "\n";
    }
}

TEST(Dataset, SelectField) {
    std::string path = "zarrs/acceptance";
    auto dataset = mdio::Dataset::Open(path, mdio::constants::kOpen);
    std::string name = "image_headers";

    ASSERT_TRUE(dataset.status().ok()) << dataset.status();
    auto ds = dataset.value();
    std::cout << ds.get_variable(name).value() << std::endl;
    EXPECT_TRUE(ds.SelectField("image_headers", "cdp-x").status().ok()) << "Failed to pull cdp-x from image_headers";
    std::cout << ds.get_variable(name).value() << std::endl;
    EXPECT_TRUE(ds.SelectField("image_headers", "elevation").status().ok())
        << "Failed to pull elevation from image_headers";
    std::cout << ds.get_variable(name).value() << std::endl;
    EXPECT_TRUE(ds.SelectField("image_headers", "").status().ok()) << "Failed to set to byte array";
    std::cout << ds.get_variable(name).value() << std::endl;
    EXPECT_FALSE(ds.SelectField("image", "NotAField").status().ok()) << "Somehow pulled NotAField from image";
    EXPECT_FALSE(ds.SelectField("NotAVariable", "NotAField").status().ok()) << "Somehow pulled NotAField from "
                                                                               "NotAVariable";
    EXPECT_FALSE(ds.SelectField("image_headers", "NotAField").status().ok()) << "Somehow pulled NotAField from "
                                                                                "image_headers";
}

TEST(Dataset, TEARDOWN) {
    std::filesystem::remove_all("zarrs/acceptance");
    ASSERT_TRUE(true);
}

} // namespace DatasetTest

} // namespace