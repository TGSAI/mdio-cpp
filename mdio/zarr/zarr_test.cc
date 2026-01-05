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

#include "mdio/zarr/zarr.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace {

// =============================================================================
// ZarrDriver Tests
// =============================================================================

namespace ZarrDriverTests {

TEST(ZarrDriver, GetDefaultVersion) {
  auto version = mdio::zarr::GetDefaultVersion();
  EXPECT_EQ(version, mdio::zarr::ZarrVersion::kV2);
}

TEST(ZarrDriver, GetDriverName_V2) {
  auto name = mdio::zarr::GetDriverName(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(name, "zarr");
}

TEST(ZarrDriver, GetDriverName_V3) {
  auto name = mdio::zarr::GetDriverName(mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(name, "zarr3");
}

TEST(ZarrDriver, ParseVersion_Integer_V2) {
  nlohmann::json version_spec = 2;
  auto result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV2);
}

TEST(ZarrDriver, ParseVersion_Integer_V3) {
  nlohmann::json version_spec = 3;
  auto result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV3);
}

TEST(ZarrDriver, ParseVersion_String_V2) {
  nlohmann::json version_spec = "2";
  auto result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV2);

  version_spec = "v2";
  result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV2);
}

TEST(ZarrDriver, ParseVersion_String_V3) {
  nlohmann::json version_spec = "3";
  auto result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV3);

  version_spec = "v3";
  result = mdio::zarr::ParseVersion(version_spec);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), mdio::zarr::ZarrVersion::kV3);
}

TEST(ZarrDriver, ParseVersion_Invalid) {
  nlohmann::json version_spec = 4;
  auto result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());

  version_spec = "v4";
  result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());
}

TEST(ZarrDriver, ParseVersion_InvalidType_Array) {
  nlohmann::json version_spec = nlohmann::json::array({2, 3});
  auto result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              testing::HasSubstr("Expected integer or string"));
}

TEST(ZarrDriver, ParseVersion_InvalidType_Object) {
  nlohmann::json version_spec = {{"version", 2}};
  auto result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              testing::HasSubstr("Expected integer or string"));
}

TEST(ZarrDriver, ParseVersion_InvalidType_Null) {
  nlohmann::json version_spec = nullptr;
  auto result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              testing::HasSubstr("Expected integer or string"));
}

TEST(ZarrDriver, ParseVersion_InvalidType_Boolean) {
  nlohmann::json version_spec = true;
  auto result = mdio::zarr::ParseVersion(version_spec);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              testing::HasSubstr("Expected integer or string"));
}

TEST(ZarrDriver, ZarrConfig_DefaultValues) {
  mdio::zarr::ZarrConfig config;

  EXPECT_EQ(config.version, mdio::zarr::ZarrVersion::kV2);
  EXPECT_TRUE(config.use_consolidated_metadata);
  EXPECT_EQ(config.dimension_separator, "/");
}

TEST(ZarrDriver, ZarrConfig_CustomValues) {
  mdio::zarr::ZarrConfig config;
  config.version = mdio::zarr::ZarrVersion::kV3;
  config.use_consolidated_metadata = false;
  config.dimension_separator = ".";

  EXPECT_EQ(config.version, mdio::zarr::ZarrVersion::kV3);
  EXPECT_FALSE(config.use_consolidated_metadata);
  EXPECT_EQ(config.dimension_separator, ".");
}

TEST(ZarrDriver, SupportsConsolidatedMetadata_V2) {
  EXPECT_TRUE(
      mdio::zarr::SupportsConsolidatedMetadata(mdio::zarr::ZarrVersion::kV2));
}

TEST(ZarrDriver, SupportsConsolidatedMetadata_V3) {
  // V3 does NOT support consolidated metadata
  EXPECT_FALSE(
      mdio::zarr::SupportsConsolidatedMetadata(mdio::zarr::ZarrVersion::kV3));
}

TEST(ZarrDriver, GetGroupMetadataFileName_V2) {
  auto filename =
      mdio::zarr::GetGroupMetadataFileName(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(filename, ".zgroup");
}

TEST(ZarrDriver, GetGroupMetadataFileName_V3) {
  auto filename =
      mdio::zarr::GetGroupMetadataFileName(mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(filename, "zarr.json");
}

TEST(ZarrDriver, GetArrayMetadataFileName_V2) {
  auto filename = mdio::zarr::GetArrayMetadataFileName(
      mdio::zarr::ZarrVersion::kV2, "myarray");
  EXPECT_EQ(filename, "myarray/.zarray");
}

TEST(ZarrDriver, GetArrayMetadataFileName_V3) {
  auto filename = mdio::zarr::GetArrayMetadataFileName(
      mdio::zarr::ZarrVersion::kV3, "myarray");
  EXPECT_EQ(filename, "myarray/zarr.json");
}

TEST(ZarrDriver, GetAttributesFileName_V2_Root) {
  auto filename =
      mdio::zarr::GetAttributesFileName(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(filename, ".zattrs");
}

TEST(ZarrDriver, GetAttributesFileName_V2_Array) {
  auto filename =
      mdio::zarr::GetAttributesFileName(mdio::zarr::ZarrVersion::kV2, "arr");
  EXPECT_EQ(filename, "arr/.zattrs");
}

TEST(ZarrDriver, GetAttributesFileName_V3_Root) {
  auto filename =
      mdio::zarr::GetAttributesFileName(mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(filename, "zarr.json");
}

TEST(ZarrDriver, GetAttributesFileName_V3_Array) {
  auto filename =
      mdio::zarr::GetAttributesFileName(mdio::zarr::ZarrVersion::kV3, "arr");
  EXPECT_EQ(filename, "arr/zarr.json");
}

TEST(ZarrDriver, GetConsolidatedMetadataFileName) {
  auto filename = mdio::zarr::GetConsolidatedMetadataFileName();
  EXPECT_EQ(filename, ".zmetadata");
}

// =============================================================================
// Path and Driver Utility Tests
// =============================================================================

TEST(ZarrDriver, InferDriverFromPath_LocalFile) {
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("/path/to/data"), "file");
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("relative/path"), "file");
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("./data"), "file");
}

TEST(ZarrDriver, InferDriverFromPath_GCS) {
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("gs://bucket/path"), "gcs");
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("gs://my-bucket/nested/path"),
            "gcs");
}

TEST(ZarrDriver, InferDriverFromPath_S3) {
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("s3://bucket/path"), "s3");
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("s3://my-bucket/nested/path"),
            "s3");
}

TEST(ZarrDriver, InferDriverFromPath_ShortPaths) {
  // Paths shorter than 5 chars should default to file
  EXPECT_EQ(mdio::zarr::InferDriverFromPath("abc"), "file");
  EXPECT_EQ(mdio::zarr::InferDriverFromPath(""), "file");
}

TEST(ZarrDriver, NormalizePath_RemovesTrailingSlash) {
  EXPECT_EQ(mdio::zarr::NormalizePath("/path/to/data/"), "/path/to/data");
  EXPECT_EQ(mdio::zarr::NormalizePath("/path/to/data///"), "/path/to/data");
}

TEST(ZarrDriver, NormalizePath_NoTrailingSlash) {
  EXPECT_EQ(mdio::zarr::NormalizePath("/path/to/data"), "/path/to/data");
}

TEST(ZarrDriver, NormalizePath_EmptyPath) {
  EXPECT_EQ(mdio::zarr::NormalizePath(""), "");
  EXPECT_EQ(mdio::zarr::NormalizePath("/"), "");
}

TEST(ZarrDriver, NormalizePathWithSlash_AddsTrailingSlash) {
  EXPECT_EQ(mdio::zarr::NormalizePathWithSlash("/path/to/data"),
            "/path/to/data/");
}

TEST(ZarrDriver, NormalizePathWithSlash_AlreadyHasSlash) {
  EXPECT_EQ(mdio::zarr::NormalizePathWithSlash("/path/to/data/"),
            "/path/to/data/");
}

TEST(ZarrDriver, NormalizePathWithSlash_EmptyPath) {
  EXPECT_EQ(mdio::zarr::NormalizePathWithSlash(""), "");
}

TEST(ZarrDriver, ExtractCloudPath_GCS) {
  auto [bucket, path] = mdio::zarr::ExtractCloudPath("gs://my-bucket/path/to/data");
  EXPECT_EQ(bucket, "my-bucket");
  EXPECT_EQ(path, "path/to/data");
}

TEST(ZarrDriver, ExtractCloudPath_S3) {
  auto [bucket, path] = mdio::zarr::ExtractCloudPath("s3://my-bucket/nested/path");
  EXPECT_EQ(bucket, "my-bucket");
  EXPECT_EQ(path, "nested/path");
}

TEST(ZarrDriver, ExtractCloudPath_BucketOnly) {
  auto [bucket, path] = mdio::zarr::ExtractCloudPath("gs://my-bucket");
  EXPECT_EQ(bucket, "my-bucket");
  EXPECT_EQ(path, "");
}

TEST(ZarrDriver, ExtractCloudPath_ShortUrl) {
  auto [bucket, path] = mdio::zarr::ExtractCloudPath("gs:/");
  EXPECT_EQ(bucket, "");
  EXPECT_EQ(path, "");
}

// =============================================================================
// JSON Utility Tests
// =============================================================================

TEST(ZarrDriver, GetJsonString_ExistingKey) {
  nlohmann::json json = {{"name", "test"}, {"version", "1.0"}};
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "name"), "test");
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "version"), "1.0");
}

TEST(ZarrDriver, GetJsonString_MissingKey) {
  nlohmann::json json = {{"name", "test"}};
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "missing"), "");
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "missing", "default"), "default");
}

TEST(ZarrDriver, GetJsonString_NonStringValue) {
  nlohmann::json json = {{"number", 42}, {"array", {1, 2, 3}}};
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "number"), "");
  EXPECT_EQ(mdio::zarr::GetJsonString(json, "array", "fallback"), "fallback");
}

TEST(ZarrDriver, GetJsonObject_ExistingKey) {
  nlohmann::json json = {{"metadata", {{"key", "value"}}}};
  auto obj = mdio::zarr::GetJsonObject(json, "metadata");
  EXPECT_TRUE(obj.is_object());
  EXPECT_EQ(obj["key"], "value");
}

TEST(ZarrDriver, GetJsonObject_MissingKey) {
  nlohmann::json json = {{"name", "test"}};
  auto obj = mdio::zarr::GetJsonObject(json, "missing");
  EXPECT_TRUE(obj.is_object());
  EXPECT_TRUE(obj.empty());
}

TEST(ZarrDriver, GetJsonObject_NonObjectValue) {
  nlohmann::json json = {{"name", "test"}, {"array", {1, 2, 3}}};
  auto obj = mdio::zarr::GetJsonObject(json, "name");
  EXPECT_TRUE(obj.is_object());
  EXPECT_TRUE(obj.empty());
}

TEST(ZarrDriver, ExtractVariableName_WithSlash) {
  EXPECT_EQ(mdio::zarr::ExtractVariableName("myvar/.zarray"), "myvar");
  EXPECT_EQ(mdio::zarr::ExtractVariableName("data/zarr.json"), "data");
  EXPECT_EQ(mdio::zarr::ExtractVariableName("nested/path/file"), "nested");
}

TEST(ZarrDriver, ExtractVariableName_NoSlash) {
  EXPECT_EQ(mdio::zarr::ExtractVariableName("myvar"), "myvar");
  EXPECT_EQ(mdio::zarr::ExtractVariableName(".zarray"), ".zarray");
}

TEST(ZarrDriver, ParseJsonFromReadResult_Valid) {
  // Create a mock read result with valid JSON
  tensorstore::kvstore::ReadResult result;
  result.state = tensorstore::kvstore::ReadResult::kValue;
  result.value = absl::Cord(R"({"key": "value", "number": 42})");

  auto parsed = mdio::zarr::ParseJsonFromReadResult(result);
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(parsed.value()["key"], "value");
  EXPECT_EQ(parsed.value()["number"], 42);
}

TEST(ZarrDriver, ParseJsonFromReadResult_InvalidJson) {
  tensorstore::kvstore::ReadResult result;
  result.state = tensorstore::kvstore::ReadResult::kValue;
  result.value = absl::Cord("not valid json {{{");

  auto parsed = mdio::zarr::ParseJsonFromReadResult(result);
  EXPECT_FALSE(parsed.ok());
  EXPECT_THAT(parsed.status().message(),
              testing::HasSubstr("JSON parse error"));
}

TEST(ZarrDriver, ParseJsonFromReadResult_NoValue) {
  tensorstore::kvstore::ReadResult result;
  result.state = tensorstore::kvstore::ReadResult::kMissing;
  // value is not set

  auto parsed = mdio::zarr::ParseJsonFromReadResult(result);
  EXPECT_FALSE(parsed.ok());
  EXPECT_THAT(parsed.status().message(),
              testing::HasSubstr("no value"));
}

}  // namespace ZarrDriverTests

// =============================================================================
// Zarr V2 Tests
// =============================================================================

namespace ZarrV2Tests {

TEST(ZarrV2, DriverName) { EXPECT_EQ(mdio::zarr::v2::kDriverName, "zarr"); }

TEST(ZarrV2, ZarrFormat) { EXPECT_EQ(mdio::zarr::v2::kZarrFormat, 2); }

TEST(ZarrV2, ConsolidatedFormat) {
  EXPECT_EQ(mdio::zarr::v2::kConsolidatedFormat, 1);
}

TEST(ZarrV2, ToZarrDtype_int8) {
  auto result = mdio::zarr::v2::ToZarrDtype("int8");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<i1");
}

TEST(ZarrV2, ToZarrDtype_int16) {
  auto result = mdio::zarr::v2::ToZarrDtype("int16");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<i2");
}

TEST(ZarrV2, ToZarrDtype_int32) {
  auto result = mdio::zarr::v2::ToZarrDtype("int32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<i4");
}

TEST(ZarrV2, ToZarrDtype_int64) {
  auto result = mdio::zarr::v2::ToZarrDtype("int64");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<i8");
}

TEST(ZarrV2, ToZarrDtype_uint8) {
  auto result = mdio::zarr::v2::ToZarrDtype("uint8");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<u1");
}

TEST(ZarrV2, ToZarrDtype_uint16) {
  auto result = mdio::zarr::v2::ToZarrDtype("uint16");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<u2");
}

TEST(ZarrV2, ToZarrDtype_uint32) {
  auto result = mdio::zarr::v2::ToZarrDtype("uint32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<u4");
}

TEST(ZarrV2, ToZarrDtype_uint64) {
  auto result = mdio::zarr::v2::ToZarrDtype("uint64");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<u8");
}

TEST(ZarrV2, ToZarrDtype_float16) {
  auto result = mdio::zarr::v2::ToZarrDtype("float16");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<f2");
}

TEST(ZarrV2, ToZarrDtype_float32) {
  auto result = mdio::zarr::v2::ToZarrDtype("float32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<f4");
}

TEST(ZarrV2, ToZarrDtype_float64) {
  auto result = mdio::zarr::v2::ToZarrDtype("float64");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<f8");
}

TEST(ZarrV2, ToZarrDtype_bool) {
  auto result = mdio::zarr::v2::ToZarrDtype("bool");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "|b1");
}

TEST(ZarrV2, ToZarrDtype_complex64) {
  auto result = mdio::zarr::v2::ToZarrDtype("complex64");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<c8");
}

TEST(ZarrV2, ToZarrDtype_complex128) {
  auto result = mdio::zarr::v2::ToZarrDtype("complex128");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<c16");
}

TEST(ZarrV2, ToZarrDtype_invalid) {
  auto result = mdio::zarr::v2::ToZarrDtype("unknown_type");
  EXPECT_FALSE(result.ok());
}

TEST(ZarrV2, CreateZgroup) {
  auto zgroup = mdio::zarr::v2::CreateZgroup();
  EXPECT_TRUE(zgroup.contains("zarr_format"));
  EXPECT_EQ(zgroup["zarr_format"], 2);
}

TEST(ZarrV2, CreateVariableSpec) {
  auto spec = mdio::zarr::v2::CreateVariableSpec("test_var");
  EXPECT_TRUE(spec.contains("driver"));
  EXPECT_EQ(spec["driver"], "zarr");
  EXPECT_TRUE(spec.contains("kvstore"));
  EXPECT_TRUE(spec.contains("metadata"));
  EXPECT_TRUE(spec.contains("attributes"));
}

TEST(ZarrV2, PrepareVariableAttributes) {
  nlohmann::json input = {
      {"attributes",
       {{"dimension_names", {"x", "y"}},
        {"variable_name", "test"},
        {"long_name", "Test Variable"},
        {"metadata", {{"chunkGrid", "should_be_removed"}, {"key", "value"}}}}}};

  auto attrs = mdio::zarr::v2::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_FALSE(attrs.contains("dimension_names"));
  EXPECT_FALSE(attrs.contains("variable_name"));
  EXPECT_EQ(attrs["long_name"], "Test Variable");
  EXPECT_TRUE(attrs.contains("key"));
  EXPECT_EQ(attrs["key"], "value");
  EXPECT_FALSE(attrs.contains("metadata"));
  EXPECT_FALSE(attrs.contains("chunkGrid"));
}

TEST(ZarrV2, GetZarray_WithDefaults) {
  // Test GetZarray with minimal metadata - should apply defaults
  nlohmann::json input = {
      {"metadata", {{"shape", {100, 200}}, {"dtype", "<f4"}}}};
  auto result = mdio::zarr::v2::GetZarray(input);
  ASSERT_TRUE(result.ok()) << result.status();

  auto zarray = result.value();
  EXPECT_EQ(zarray["order"], "C");
  EXPECT_TRUE(zarray["filters"].is_null());
  EXPECT_TRUE(zarray["fill_value"].is_null());
  EXPECT_EQ(zarray["zarr_format"], 2);
  EXPECT_EQ(zarray["dimension_separator"], "/");
  // chunks should default to shape when not provided
  EXPECT_EQ(zarray["chunks"], zarray["shape"]);
}

TEST(ZarrV2, GetZarray_WithAllFields) {
  nlohmann::json input = {{"metadata",
                           {{"shape", {100, 200}},
                            {"dtype", "<f4"},
                            {"chunks", {50, 50}},
                            {"order", "F"},
                            {"fill_value", 0.0},
                            {"zarr_format", 2},
                            {"dimension_separator", "."},
                            {"compressor", {{"id", "blosc"}}}}}};
  auto result = mdio::zarr::v2::GetZarray(input);
  ASSERT_TRUE(result.ok()) << result.status();

  auto zarray = result.value();
  EXPECT_EQ(zarray["order"], "F");
  EXPECT_EQ(zarray["dimension_separator"], ".");
  EXPECT_EQ(zarray["chunks"][0], 50);
  EXPECT_EQ(zarray["chunks"][1], 50);
}

TEST(ZarrV2, GetZarray_NoMetadataKey) {
  // Test GetZarray without metadata key - should create empty metadata
  nlohmann::json input = nlohmann::json::object();
  // Note: This will fail because shape and dtype are required
  // but it tests the metadata initialization path
  auto result = mdio::zarr::v2::GetZarray(input);
  // Should fail because shape/dtype are not provided
  EXPECT_FALSE(result.ok());
}

TEST(ZarrV2, PrepareVariableAttributes_EmptyCoordinatesString) {
  // Test with empty string coordinates - should be removed
  nlohmann::json input = {{"attributes",
                           {{"dimension_names", {"x", "y"}},
                            {"coordinates", ""},
                            {"long_name", "Test Variable"}}}};

  auto attrs = mdio::zarr::v2::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_FALSE(attrs.contains("coordinates"));
  EXPECT_TRUE(attrs.contains("long_name"));
}

TEST(ZarrV2, PrepareVariableAttributes_EmptyCoordinatesArray) {
  // Test with empty array coordinates - should be removed
  nlohmann::json input = {{"attributes",
                           {{"dimension_names", {"x", "y"}},
                            {"coordinates", nlohmann::json::array()},
                            {"long_name", "Test Variable"}}}};

  auto attrs = mdio::zarr::v2::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_FALSE(attrs.contains("coordinates"));
}

TEST(ZarrV2, PrepareVariableAttributes_ValidCoordinates) {
  nlohmann::json input = {{"attributes",
                           {{"dimension_names", {"x", "y"}},
                            {"coordinates", "cdp-x cdp-y"},
                            {"long_name", "Test Variable"}}}};

  auto attrs = mdio::zarr::v2::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("coordinates"));
  EXPECT_EQ(attrs["coordinates"], "cdp-x cdp-y");
}

TEST(ZarrV2, PrepareVariableAttributes_EmptyLongName) {
  // Test with empty long_name - should be removed
  nlohmann::json input = {
      {"attributes", {{"dimension_names", {"x", "y"}}, {"long_name", ""}}}};

  auto attrs = mdio::zarr::v2::PrepareVariableAttributes(input);

  EXPECT_FALSE(attrs.contains("long_name"));
}

}  // namespace ZarrV2Tests

// =============================================================================
// Zarr V3 Tests
// =============================================================================

namespace ZarrV3Tests {

TEST(ZarrV3, DriverName) { EXPECT_EQ(mdio::zarr::v3::kDriverName, "zarr3"); }

TEST(ZarrV3, ZarrFormat) { EXPECT_EQ(mdio::zarr::v3::kZarrFormat, 3); }

TEST(ZarrV3, NodeTypes) {
  EXPECT_EQ(mdio::zarr::v3::kGroupNodeType, "group");
  EXPECT_EQ(mdio::zarr::v3::kArrayNodeType, "array");
}

TEST(ZarrV3, ToZarrDtype_int8) {
  auto result = mdio::zarr::v3::ToZarrDtype("int8");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "int8");
}

TEST(ZarrV3, ToZarrDtype_float32) {
  auto result = mdio::zarr::v3::ToZarrDtype("float32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "float32");
}

TEST(ZarrV3, ToZarrDtype_float64) {
  auto result = mdio::zarr::v3::ToZarrDtype("float64");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "float64");
}

TEST(ZarrV3, ToZarrDtype_complex128) {
  auto result = mdio::zarr::v3::ToZarrDtype("complex128");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "complex128");
}

TEST(ZarrV3, ToZarrDtype_invalid) {
  auto result = mdio::zarr::v3::ToZarrDtype("unknown_type");
  EXPECT_FALSE(result.ok());
}

TEST(ZarrV3, CreateGroupMetadata_Empty) {
  auto metadata = mdio::zarr::v3::CreateGroupMetadata();
  EXPECT_EQ(metadata["zarr_format"], 3);
  EXPECT_EQ(metadata["node_type"], "group");
  EXPECT_FALSE(metadata.contains("attributes"));
}

TEST(ZarrV3, CreateGroupMetadata_WithAttributes) {
  nlohmann::json attrs = {{"name", "test"}, {"version", "1.0"}};
  auto metadata = mdio::zarr::v3::CreateGroupMetadata(attrs);
  EXPECT_EQ(metadata["zarr_format"], 3);
  EXPECT_EQ(metadata["node_type"], "group");
  EXPECT_TRUE(metadata.contains("attributes"));
  EXPECT_EQ(metadata["attributes"]["name"], "test");
}

TEST(ZarrV3, CreateArrayMetadata) {
  std::vector<int64_t> shape = {100, 200};
  std::vector<int64_t> chunks = {50, 50};
  auto metadata = mdio::zarr::v3::CreateArrayMetadata(shape, chunks, "float32");

  EXPECT_EQ(metadata["zarr_format"], 3);
  EXPECT_EQ(metadata["node_type"], "array");
  EXPECT_EQ(metadata["shape"], shape);
  EXPECT_TRUE(metadata.contains("chunk_grid"));
  EXPECT_EQ(metadata["chunk_grid"]["name"], "regular");
  EXPECT_EQ(metadata["chunk_grid"]["configuration"]["chunk_shape"], chunks);
  EXPECT_EQ(metadata["data_type"], "float32");
  EXPECT_TRUE(metadata.contains("codecs"));
}

TEST(ZarrV3, CreateVariableSpec) {
  auto spec = mdio::zarr::v3::CreateVariableSpec("test_var");
  EXPECT_TRUE(spec.contains("driver"));
  EXPECT_EQ(spec["driver"], "zarr3");
  EXPECT_TRUE(spec.contains("kvstore"));
  EXPECT_TRUE(spec.contains("metadata"));
  EXPECT_TRUE(spec.contains("attributes"));
}

TEST(ZarrV3, PrepareVariableAttributes) {
  nlohmann::json input = {
      {"attributes",
       {{"dimension_names", {"x", "y", "z"}},
        {"variable_name", "test"},
        {"long_name", ""},  // Empty should be removed
        {"metadata",
         {{"chunkGrid", "should_be_removed"}, {"custom", "value"}}}}}};

  auto attrs = mdio::zarr::v3::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_FALSE(attrs.contains("dimension_names"));
  EXPECT_FALSE(attrs.contains("variable_name"));
  EXPECT_FALSE(attrs.contains("long_name"));  // Empty removed
  EXPECT_TRUE(attrs.contains("custom"));
  EXPECT_EQ(attrs["custom"], "value");
}

TEST(ZarrV3, ConvertToMdioMetadata) {
  nlohmann::json zarr_json = {
      {"zarr_format", 3},
      {"node_type", "array"},
      {"attributes", {{"_ARRAY_DIMENSIONS", {"x", "y"}}, {"key", "value"}}}};

  auto metadata = mdio::zarr::v3::ConvertToMdioMetadata(zarr_json);

  EXPECT_TRUE(metadata.contains("dimension_names"));
  EXPECT_FALSE(metadata.contains("_ARRAY_DIMENSIONS"));
  EXPECT_TRUE(metadata.contains("key"));
}

TEST(ZarrV3, PrepareVariableAttributes_EmptyCoordinates) {
  // Test empty string coordinates - should be removed (covers line 204)
  nlohmann::json input = {{"attributes",
                           {{"dimension_names", {"x", "y"}},
                            {"coordinates", ""},
                            {"long_name", "Test Variable"}}}};

  auto attrs = mdio::zarr::v3::PrepareVariableAttributes(input);

  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_FALSE(attrs.contains("coordinates"));
  EXPECT_TRUE(attrs.contains("long_name"));

  // Also test empty array coordinates
  nlohmann::json input2 = {{"attributes",
                            {{"dimension_names", {"x", "y"}},
                             {"coordinates", nlohmann::json::array()}}}};

  auto attrs2 = mdio::zarr::v3::PrepareVariableAttributes(input2);
  EXPECT_FALSE(attrs2.contains("coordinates"));
}

TEST(ZarrV3, ReadVariableAttributes) {
  // Create a temporary directory with a zarr.json file
  auto tmpDir = std::filesystem::temp_directory_path() /
                ("zarr_v3_read_attrs_" + std::to_string(std::rand()));
  std::filesystem::create_directories(tmpDir);

  // Write a test zarr.json file
  nlohmann::json test_zarr_json = {
      {"zarr_format", 3},
      {"node_type", "array"},
      {"attributes", {{"_ARRAY_DIMENSIONS", {"x", "y"}}, {"units", "meters"}}}};

  std::ofstream out(tmpDir / "zarr.json");
  out << test_zarr_json.dump(4);
  out.close();

  // Open KvStore - file driver needs trailing slash for directories
  nlohmann::json kvstore_spec = {{"driver", "file"},
                                 {"path", tmpDir.string() + "/"}};

  auto kvs_result = tensorstore::kvstore::Open(kvstore_spec).result();
  ASSERT_TRUE(kvs_result.ok()) << kvs_result.status();

  auto attrs_future =
      mdio::zarr::v3::ReadVariableAttributes(kvs_result.value());
  auto attrs_result = attrs_future.result();
  ASSERT_TRUE(attrs_result.ok()) << attrs_result.status();

  auto attrs = attrs_result.value();
  EXPECT_TRUE(attrs.contains("_ARRAY_DIMENSIONS"));
  EXPECT_EQ(attrs["units"], "meters");

  // Cleanup
  std::filesystem::remove_all(tmpDir);
}

TEST(ZarrV3, ReadVariableAttributes_NoAttributesInFile) {
  auto tmpDir = std::filesystem::temp_directory_path() /
                ("zarr_v3_no_attrs_" + std::to_string(std::rand()));
  std::filesystem::create_directories(tmpDir);

  // Write zarr.json without attributes field
  nlohmann::json test_zarr_json = {{"zarr_format", 3}, {"node_type", "array"}};

  std::ofstream out(tmpDir / "zarr.json");
  out << test_zarr_json.dump(4);
  out.close();

  // File driver needs trailing slash for directories
  nlohmann::json kvstore_spec = {{"driver", "file"},
                                 {"path", tmpDir.string() + "/"}};
  auto kvs_result = tensorstore::kvstore::Open(kvstore_spec).result();
  ASSERT_TRUE(kvs_result.ok()) << kvs_result.status();

  auto attrs_future =
      mdio::zarr::v3::ReadVariableAttributes(kvs_result.value());
  auto attrs_result = attrs_future.result();
  ASSERT_TRUE(attrs_result.ok()) << attrs_result.status();

  // Should return empty object when no attributes field
  EXPECT_TRUE(attrs_result.value().is_object());
  EXPECT_TRUE(attrs_result.value().empty());

  std::filesystem::remove_all(tmpDir);
}

// =============================================================================
// V3 Utility Function Tests
// =============================================================================

TEST(ZarrV3, IsArrayMetadata_True) {
  nlohmann::json array_json = {{"node_type", "array"}, {"zarr_format", 3}};
  EXPECT_TRUE(mdio::zarr::v3::IsArrayMetadata(array_json));
}

TEST(ZarrV3, IsArrayMetadata_False_Group) {
  nlohmann::json group_json = {{"node_type", "group"}, {"zarr_format", 3}};
  EXPECT_FALSE(mdio::zarr::v3::IsArrayMetadata(group_json));
}

TEST(ZarrV3, IsArrayMetadata_False_NoNodeType) {
  nlohmann::json json = {{"zarr_format", 3}};
  EXPECT_FALSE(mdio::zarr::v3::IsArrayMetadata(json));
}

TEST(ZarrV3, ExtractChildArrayCandidates_ValidEntries) {
  std::vector<tensorstore::kvstore::ListEntry> entries;
  entries.push_back({"var1/zarr.json", {}});
  entries.push_back({"var2/zarr.json", {}});
  entries.push_back({"var3/data/chunk", {}});  // Not a zarr.json
  entries.push_back({"zarr.json", {}});        // Root, no slash

  auto candidates = mdio::zarr::v3::ExtractChildArrayCandidates(entries);

  EXPECT_EQ(candidates.size(), 2);
  EXPECT_THAT(candidates, testing::Contains("var1"));
  EXPECT_THAT(candidates, testing::Contains("var2"));
  EXPECT_THAT(candidates, testing::Not(testing::Contains("var3")));
}

TEST(ZarrV3, ExtractChildArrayCandidates_NoDuplicates) {
  std::vector<tensorstore::kvstore::ListEntry> entries;
  entries.push_back({"var1/zarr.json", {}});
  entries.push_back({"var1/zarr.json", {}});  // Duplicate
  entries.push_back({"var1/data", {}});

  auto candidates = mdio::zarr::v3::ExtractChildArrayCandidates(entries);

  EXPECT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0], "var1");
}

TEST(ZarrV3, ExtractChildArrayCandidates_Empty) {
  std::vector<tensorstore::kvstore::ListEntry> entries;

  auto candidates = mdio::zarr::v3::ExtractChildArrayCandidates(entries);

  EXPECT_TRUE(candidates.empty());
}

TEST(ZarrV3, BuildVariableSpec_LocalFile) {
  auto spec = mdio::zarr::v3::BuildVariableSpec("file", "/path/to/dataset",
                                                "myvar");

  EXPECT_EQ(spec["driver"], "zarr3");
  EXPECT_EQ(spec["kvstore"]["driver"], "file");
  EXPECT_EQ(spec["kvstore"]["path"], "/path/to/dataset/myvar");
}

TEST(ZarrV3, BuildVariableSpec_GCS) {
  auto spec = mdio::zarr::v3::BuildVariableSpec("gcs", "bucket/path", "myvar");

  EXPECT_EQ(spec["driver"], "zarr3");
  EXPECT_EQ(spec["kvstore"]["driver"], "gcs");
  EXPECT_EQ(spec["kvstore"]["path"], "bucket/path/myvar");
}

}  // namespace ZarrV3Tests

// =============================================================================
// Unified Zarr Interface Tests
// =============================================================================

namespace ZarrUnifiedTests {

TEST(ZarrUnified, GetDriverNameForVersion_V2) {
  auto name = mdio::zarr::GetDriverNameForVersion(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(name, "zarr");
}

TEST(ZarrUnified, GetDriverNameForVersion_V3) {
  auto name = mdio::zarr::GetDriverNameForVersion(mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(name, "zarr3");
}

TEST(ZarrUnified, ConvertDtype_V2) {
  auto result =
      mdio::zarr::ConvertDtype(mdio::zarr::ZarrVersion::kV2, "float32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "<f4");
}

TEST(ZarrUnified, ConvertDtype_V3) {
  auto result =
      mdio::zarr::ConvertDtype(mdio::zarr::ZarrVersion::kV3, "float32");
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result.value(), "float32");
}

TEST(ZarrUnified, CreateVariableSpec_V2) {
  auto spec =
      mdio::zarr::CreateVariableSpec(mdio::zarr::ZarrVersion::kV2, "var");
  EXPECT_EQ(spec["driver"], "zarr");
}

TEST(ZarrUnified, CreateVariableSpec_V3) {
  auto spec =
      mdio::zarr::CreateVariableSpec(mdio::zarr::ZarrVersion::kV3, "var");
  EXPECT_EQ(spec["driver"], "zarr3");
}

TEST(ZarrUnified, IsV3Spec_True) {
  nlohmann::json spec = {{"driver", "zarr3"}};
  EXPECT_TRUE(mdio::zarr::IsV3Spec(spec));
}

TEST(ZarrUnified, IsV3Spec_False) {
  nlohmann::json spec = {{"driver", "zarr"}};
  EXPECT_FALSE(mdio::zarr::IsV3Spec(spec));
}

TEST(ZarrUnified, IsV3Spec_NoDriver) {
  nlohmann::json spec = {{"path", "test"}};
  EXPECT_FALSE(mdio::zarr::IsV3Spec(spec));
}

TEST(ZarrUnified, GetVersionFromSpec_V2) {
  nlohmann::json spec = {{"driver", "zarr"}};
  EXPECT_EQ(mdio::zarr::GetVersionFromSpec(spec), mdio::zarr::ZarrVersion::kV2);
}

TEST(ZarrUnified, GetVersionFromSpec_V3) {
  nlohmann::json spec = {{"driver", "zarr3"}};
  EXPECT_EQ(mdio::zarr::GetVersionFromSpec(spec), mdio::zarr::ZarrVersion::kV3);
}

TEST(ZarrUnified, UpdateSpecVersion_ToV3) {
  nlohmann::json spec = {{"driver", "zarr"}, {"path", "test"}};
  auto updated =
      mdio::zarr::UpdateSpecVersion(spec, mdio::zarr::ZarrVersion::kV3);
  EXPECT_EQ(updated["driver"], "zarr3");
  EXPECT_EQ(updated["path"], "test");  // Other fields preserved
}

TEST(ZarrUnified, UpdateSpecVersion_ToV2) {
  nlohmann::json spec = {{"driver", "zarr3"}, {"path", "test"}};
  auto updated =
      mdio::zarr::UpdateSpecVersion(spec, mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(updated["driver"], "zarr");
  EXPECT_EQ(updated["path"], "test");
}

}  // namespace ZarrUnifiedTests

}  // namespace
