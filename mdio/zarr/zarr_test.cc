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

#include "mdio/zarr/zarr.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>

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

}  // namespace ZarrDriverTests

// =============================================================================
// Zarr V2 Tests
// =============================================================================

namespace ZarrV2Tests {

TEST(ZarrV2, DriverName) {
  EXPECT_EQ(mdio::zarr::v2::kDriverName, "zarr");
}

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

}  // namespace ZarrV2Tests

// =============================================================================
// Zarr V3 Tests
// =============================================================================

namespace ZarrV3Tests {

TEST(ZarrV3, DriverName) {
  EXPECT_EQ(mdio::zarr::v3::kDriverName, "zarr3");
}

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
  auto metadata =
      mdio::zarr::v3::CreateArrayMetadata(shape, chunks, "float32");

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

}  // namespace ZarrV3Tests

// =============================================================================
// Unified Zarr Interface Tests
// =============================================================================

namespace ZarrUnifiedTests {

TEST(ZarrUnified, GetDriverNameForVersion_V2) {
  auto name =
      mdio::zarr::GetDriverNameForVersion(mdio::zarr::ZarrVersion::kV2);
  EXPECT_EQ(name, "zarr");
}

TEST(ZarrUnified, GetDriverNameForVersion_V3) {
  auto name =
      mdio::zarr::GetDriverNameForVersion(mdio::zarr::ZarrVersion::kV3);
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
