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

#ifndef MDIO_HEADER_VARIABLE_H_
#define MDIO_HEADER_VARIABLE_H_

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "mdio/impl.h"
#include "mdio/stats.h"
#include "mdio/variable.h"
#include "mdio/zarr/zarr.h"
#include "tensorstore/index_space/index_domain_builder.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/util/future.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace mdio {

namespace internal {

inline bool IsHeaderOnlySpec(const ::nlohmann::json& spec) {
  return spec.contains("_mdio_header_only") &&
         spec["_mdio_header_only"].get<bool>();
}

inline std::string VariableNameFromKvstorePath(const std::string& path) {
  std::vector<std::string> path_components = absl::StrSplit(path, '/');
  return path_components.back();
}

inline Result<tensorstore::IndexDomain<>> BuildDomainFromShapeAndLabels(
    const std::vector<Index>& shape,
    const std::vector<std::string>& labels) {
  tensorstore::IndexDomainBuilder<> builder(shape.size());
  if (!shape.empty()) {
    builder.shape(shape);
  }
  if (!labels.empty()) {
    if (labels.size() != shape.size()) {
      return absl::InvalidArgumentError(
          "dimension label count does not match shape rank");
    }
    builder.labels(labels);
  }
  return builder.Finalize();
}

inline std::vector<std::string> ExtractDimensionLabels(
    const ::nlohmann::json& array_metadata,
    const ::nlohmann::json& attrs) {
  if (array_metadata.contains("dimension_names") &&
      array_metadata["dimension_names"].is_array()) {
    std::vector<std::string> labels;
    for (const auto& dim : array_metadata["dimension_names"]) {
      labels.push_back(dim.get<std::string>());
    }
    return labels;
  }
  if (attrs.contains("_ARRAY_DIMENSIONS") &&
      attrs["_ARRAY_DIMENSIONS"].is_array()) {
    std::vector<std::string> labels;
    for (const auto& dim : attrs["_ARRAY_DIMENSIONS"]) {
      labels.push_back(dim.get<std::string>());
    }
    return labels;
  }
  if (attrs.contains("dimension_names") &&
      attrs["dimension_names"].is_array()) {
    std::vector<std::string> labels;
    for (const auto& dim : attrs["dimension_names"]) {
      labels.push_back(dim.get<std::string>());
    }
    return labels;
  }
  return {};
}

}  // namespace internal

/**
 * @brief A metadata-only variable whose content lives in Zarr attributes.
 *
 * Header variables (e.g. the SEG-Y file header) use string/bytes/datetime Zarr
 * dtypes that TensorStore cannot open as arrays. They are surfaced in a Dataset
 * for inspection and attribute updates, but array read/write is not supported.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic>
class HeaderVariable : public VariableBase {
 public:
  HeaderVariable() = default;

  friend std::ostream& operator<<(std::ostream& os, const HeaderVariable& obj) {
    os << obj.get_variable_name() << "\t" << obj.dimensions() << "\n";
    os << obj.dtypeName_ << "\t" << obj.rank();
    return os;
  }

  /**
   * @brief Constructs a HeaderVariable from a discovery spec emitted by the Zarr
   * layer.
   */
  static Result<HeaderVariable> FromSpec(const ::nlohmann::json& spec) {
    if (!internal::IsHeaderOnlySpec(spec)) {
      return absl::InvalidArgumentError(
          "Spec is not marked as a metadata-only header variable.");
    }
    if (!spec.contains("kvstore") || !spec["kvstore"].contains("path")) {
      return absl::InvalidArgumentError(
          "Header variable spec is missing kvstore path.");
    }

    const std::string variable_name =
        internal::VariableNameFromKvstorePath(
            spec["kvstore"]["path"].get<std::string>());
    const zarr::ZarrVersion zarr_version = zarr::GetVersionFromSpec(spec);

    ::nlohmann::json attrs = ::nlohmann::json::object();
    ::nlohmann::json array_metadata = ::nlohmann::json::object();
    std::string dtype_name;

    if (zarr_version == zarr::ZarrVersion::kV3) {
      if (!spec.contains("_mdio_array_metadata")) {
        return absl::InvalidArgumentError(
            "V3 header variable spec is missing _mdio_array_metadata.");
      }
      array_metadata = spec["_mdio_array_metadata"];
      if (array_metadata.contains("attributes")) {
        attrs = array_metadata["attributes"];
      }
      if (array_metadata.contains("data_type")) {
        dtype_name =
            zarr::v3::GetDataTypeName(array_metadata["data_type"]);
      }
    } else {
      if (!spec.contains("_mdio_zarray")) {
        return absl::InvalidArgumentError(
            "V2 header variable spec is missing _mdio_zarray.");
      }
      array_metadata = spec["_mdio_zarray"];
      if (spec.contains("_mdio_zattrs")) {
        attrs = spec["_mdio_zattrs"];
      }
      if (array_metadata.contains("dtype") &&
          array_metadata["dtype"].is_string()) {
        dtype_name = array_metadata["dtype"].get<std::string>();
      }
    }

    std::vector<Index> shape;
    if (array_metadata.contains("shape") &&
        array_metadata["shape"].is_array()) {
      for (const auto& dim : array_metadata["shape"]) {
        shape.push_back(dim.get<Index>());
      }
    }

    MDIO_ASSIGN_OR_RETURN(
        auto domain,
        internal::BuildDomainFromShapeAndLabels(
            shape, internal::ExtractDimensionLabels(array_metadata, attrs)));

    ::nlohmann::json metadata = attrs;
    metadata["variable_name"] = variable_name;
    if (metadata.contains("_ARRAY_DIMENSIONS")) {
      metadata["dimension_names"] = metadata["_ARRAY_DIMENSIONS"];
      metadata.erase("_ARRAY_DIMENSIONS");
    } else if (array_metadata.contains("dimension_names")) {
      metadata["dimension_names"] = array_metadata["dimension_names"];
    }

    std::string long_name;
    if (metadata.contains("long_name") &&
        metadata["long_name"].is_string()) {
      long_name = metadata["long_name"].get<std::string>();
    } else {
      long_name = variable_name;
    }

    ::nlohmann::json attrs_json = metadata;
    if (attrs_json.contains("variable_name")) {
      attrs_json.erase("variable_name");
    }
    if (attrs_json.contains("dimension_names")) {
      attrs_json.erase("dimension_names");
    }
    if (attrs_json.contains("long_name")) {
      attrs_json.erase("long_name");
    }

    ::nlohmann::json user_attrs_wrapper = ::nlohmann::json::object();
    if (!attrs_json.empty()) {
      user_attrs_wrapper["attributes"] = attrs_json;
    }
    auto attrs_res = UserAttributes::FromJson(user_attrs_wrapper);
    if (!attrs_res.ok()) {
      return attrs_res.status();
    }

    HeaderVariable header;
    header.variableName = variable_name;
    header.longName = long_name;
    header.metadata = metadata;
    header.domain_ = domain;
    header.dtypeName_ = dtype_name;
    header.kvstore_spec_ = spec["kvstore"];
    header.zarr_version_ = zarr_version;
    header.array_metadata_ = array_metadata;
    if (spec.contains("_mdio_zattrs")) {
      header.zattrs_ = spec["_mdio_zattrs"];
    }
    header.attributes = std::make_shared<std::shared_ptr<UserAttributes>>(
        std::make_shared<UserAttributes>(attrs_res.value()));
    header.attributesAddress =
        reinterpret_cast<std::uintptr_t>(&(**header.attributes));
    return header;
  }

  template <ArrayOriginKind OriginKind = offset_origin>
  Future<VariableData<T, R, OriginKind>> Read() {
    return absl::UnimplementedError(
        "HeaderVariable '" + variableName +
        "' is metadata-only; array read is not supported.");
  }

  template <ArrayOriginKind OriginKind = offset_origin>
  WriteFutures Write(const VariableData<T, R, OriginKind>& /*source*/) const {
    return absl::UnimplementedError(
        "HeaderVariable '" + variableName +
        "' is metadata-only; array write is not supported.");
  }

  IndexDomainView<R> dimensions() const { return domain_; }

  std::size_t rank() const { return domain_.rank(); }

  /**
   * @brief Reduced metadata for a header variable.
   *
   * Unlike a regular Variable, the long name is meaningful for round-tripping a
   * header variable's attributes, so it is included here.
   */
  const nlohmann::json getReducedMetadata() const {
    nlohmann::json ret = metadata;
    ret["long_name"] = longName;
    return ret;
  }

  /**
   * @brief Builds the flat attribute blob to persist for this header variable.
   *
   * The live user attributes are the source of truth. Structural keys that
   * live alongside the user attributes on disk (e.g. _ARRAY_DIMENSIONS) are
   * re-injected from the original on-disk attributes so the round-trip is
   * lossless.
   */
  nlohmann::json BuildPublishAttrs() const {
    nlohmann::json attrs = nlohmann::json::object();
    auto user_attrs = GetAttributes();
    if (user_attrs.contains("attributes")) {
      attrs = user_attrs["attributes"];
    }

    const nlohmann::json& original =
        (zattrs_.is_object() && !zattrs_.empty())
            ? zattrs_
            : (array_metadata_.contains("attributes")
                   ? array_metadata_["attributes"]
                   : nlohmann::json::object());
    if (original.is_object() && original.contains("_ARRAY_DIMENSIONS") &&
        !attrs.contains("_ARRAY_DIMENSIONS")) {
      attrs["_ARRAY_DIMENSIONS"] = original["_ARRAY_DIMENSIONS"];
    }
    return attrs;
  }

  Future<tensorstore::TimestampedStorageGeneration> PublishMetadata() {
    auto pair = tensorstore::PromiseFuturePair<
        tensorstore::TimestampedStorageGeneration>::Make();

    auto kvs_future = tensorstore::kvstore::Open(kvstore_spec_);
    kvs_future.ExecuteWhenReady(
        [promise = std::move(pair.promise), zarr_version = zarr_version_,
         attrs_to_write = BuildPublishAttrs(),
         this](tensorstore::ReadyFuture<tensorstore::KvStore> ready_kvs) mutable {
          if (!ready_kvs.result().ok()) {
            promise.SetResult(ready_kvs.result().status());
            return;
          }

          if (zarr_version == zarr::ZarrVersion::kV3) {
            auto read_future =
                tensorstore::kvstore::Read(ready_kvs.value(), "/zarr.json");
            read_future.ExecuteWhenReady(
                [promise = std::move(promise),
                 attrs_to_write = std::move(attrs_to_write),
                 kvs = ready_kvs.value(), this](
                    tensorstore::ReadyFuture<tensorstore::kvstore::ReadResult>
                        ready_read) mutable {
                  if (!ready_read.result().ok()) {
                    promise.SetResult(ready_read.result().status());
                    return;
                  }
                  if (!ready_read.value().has_value()) {
                    promise.SetResult(absl::NotFoundError(
                        "Missing zarr.json for header variable."));
                    return;
                  }

                  ::nlohmann::json zarr_json;
                  try {
                    zarr_json = ::nlohmann::json::parse(
                        std::string(ready_read.value().value));
                  } catch (const std::exception& e) {
                    promise.SetResult(absl::InvalidArgumentError(
                        std::string("JSON parse error: ") + e.what()));
                    return;
                  }

                  zarr_json["attributes"] = attrs_to_write;

                  auto write_result = tensorstore::kvstore::Write(
                      kvs, "/zarr.json",
                      absl::Cord(zarr_json.dump(4)));
                  write_result.ExecuteWhenReady(
                      [promise = std::move(promise), this](
                          tensorstore::ReadyFuture<
                              tensorstore::TimestampedStorageGeneration>
                              ready_write) {
                        if (!ready_write.result().ok()) {
                          promise.SetResult(ready_write.result().status());
                          return;
                        }
                        attributesAddress =
                            reinterpret_cast<std::uintptr_t>(&(**attributes));
                        promise.SetResult(ready_write.result().value());
                      });
                });
            return;
          }

          auto write_future = tensorstore::kvstore::Write(
              ready_kvs.value(), "/.zattrs", absl::Cord(attrs_to_write.dump(4)));
          write_future.ExecuteWhenReady(
              [promise = std::move(promise), this](
                  tensorstore::ReadyFuture<
                      tensorstore::TimestampedStorageGeneration>
                      ready_write) {
                if (!ready_write.result().ok()) {
                  promise.SetResult(ready_write.result().status());
                  return;
                }
                attributesAddress =
                    reinterpret_cast<std::uintptr_t>(&(**attributes));
                promise.SetResult(ready_write.result().value());
              });
        });

    return pair.future;
  }

  /**
   * @brief Builds a commit spec for consolidated metadata writes.
   *
   * V2 consolidated metadata must preserve the original string dtype .zarray
   * verbatim because GetZarray rejects those dtypes.
   */
  nlohmann::json ToCommitJson() const {
    nlohmann::json json = getReducedMetadata();
    json["kvstore"] = kvstore_spec_;
    json["_mdio_header_only"] = true;
    json["_mdio_zarray"] = array_metadata_;
    json["attributes"] = BuildPublishAttrs();

    if (zarr_version_ == zarr::ZarrVersion::kV3) {
      json["driver"] = std::string(zarr::v3::kDriverName);
      json["_mdio_array_metadata"] = array_metadata_;
    } else {
      json["driver"] = std::string(zarr::v2::kDriverName);
      json["_mdio_zattrs"] = json["attributes"];
    }

    if (json.contains("long_name")) {
      json["attributes"]["long_name"] = json["long_name"];
      json.erase("long_name");
    }
    if (metadata.contains("dimension_names")) {
      json["attributes"]["dimension_names"] = metadata["dimension_names"];
    }
    return json;
  }

  const std::string& get_dtype_name() const { return dtypeName_; }
  zarr::ZarrVersion zarr_version() const { return zarr_version_; }

 private:
  tensorstore::IndexDomain<> domain_;
  std::string dtypeName_;
  ::nlohmann::json kvstore_spec_;
  zarr::ZarrVersion zarr_version_ = zarr::ZarrVersion::kV2;
  ::nlohmann::json array_metadata_;
  ::nlohmann::json zattrs_;
};

/**
 * @brief A collection of metadata-only header variables.
 */
class HeaderVariableCollection {
 public:
  HeaderVariableCollection() = default;

  void add(const std::string& label, const HeaderVariable<>& variable) {
    variables_[label] = variable;
  }

  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Result<HeaderVariable<T, R, M>> get(const std::string& label) const {
    if (!variables_.count(label)) {
      return absl::NotFoundError("Label '" + label +
                                 "' not found in header variables.");
    }
    return variables_.at(label);
  }

  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Result<HeaderVariable<T, R, M>> at(const std::string& label) const {
    if (!variables_.count(label)) {
      return absl::NotFoundError("Label '" + label +
                                 "' not found in header variables.");
    }
    return variables_.at(label);
  }

  bool contains_key(const std::string& label) const {
    return variables_.count(label) != 0;
  }

  std::vector<std::string> get_keys() const {
    std::vector<std::string> keys;
    for (const auto& [key, _] : variables_) {
      keys.emplace_back(key);
    }
    return keys;
  }

  std::vector<std::string> get_iterable_accessor() const {
    std::vector keys = get_keys();
    std::sort(keys.begin(), keys.end());
    return keys;
  }

 private:
  std::unordered_map<std::string, HeaderVariable<>> variables_;
};

}  // namespace mdio

#endif  // MDIO_HEADER_VARIABLE_H_
