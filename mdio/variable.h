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

#ifndef MDIO_VARIABLE_H_
#define MDIO_VARIABLE_H_

#include <filesystem>
#include <limits>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "mdio/impl.h"
#include "mdio/stats.h"
#include "tensorstore/array.h"
#include "tensorstore/driver/driver.h"
#include "tensorstore/driver/registry.h"
#include "tensorstore/driver/zarr/dtype.h"
#include "tensorstore/index_space/dim_expression.h"
#include "tensorstore/index_space/index_domain_builder.h"
#include "tensorstore/kvstore/kvstore.h"
#include "tensorstore/kvstore/operations.h"
#include "tensorstore/open.h"
#include "tensorstore/stack.h"
#include "tensorstore/tensorstore.h"
#include "tensorstore/util/future.h"

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace mdio {

template <typename T, DimensionIndex R, ReadWriteMode M>
struct Variable;

template <typename T = void, DimensionIndex R = dynamic_rank,
          ArrayOriginKind OriginKind = offset_origin>
struct VariableData;

template <typename T, DimensionIndex R, ArrayOriginKind OriginKind>
struct LabeledArray;

template <typename T>
struct extract_descriptor_Ttype;

template <typename T>
struct outer_type;

/**
 * @brief A descriptor for slicing a Variable or Dataset.
 * @tparam T The type of the range. Default is `Index` for `isel` based slicing.
 * @param label The label of the dimension to slice. The recommended is to use a
 * label instead of an index.
 * @param start The start index or value of the slice.
 * @param stop The stop index or value of the slice.
 * @param step The step index or value of the slice. Default is 1.
 */
template <typename T = Index>
struct RangeDescriptor {
  using type = T;
  /// The label of the dimension to slice. Either a string or an index.
  DimensionIdentifier label;
  /// The start index or value of the slice.
  T start;
  /// The stop index or value of the slice.
  T stop;
  /// The step index or value of the slice. Only 1 is supported currently.
  Index step = 1;
};

/**
 * @brief A descriptor for slicing a Variable.
 * A struct representing how to slice a Variable or Dataset.
 * All slices using this will be performed as half open intervals.
 * @param label The label of the dimension to slice.
 * @param start The start index of the slice.
 * @param stop The stop index of the slice.
 * @param step The step index of the slice.
 * @details \b Usage
 * This provides an example of describing a slice for [0, 100) with a step of 1
 * along the inline dimension and [0, 200) with a step of 1 along the crossline
 * dimension. The depth dimension would remain wholely intact as it is not
 * specified.
 * @code
 * mdio::SliceDescriptor desc1 = {"inline", 0, 100, 1};
 * mdio::SliceDescriptor desc2 = {"crossline", 0, 200, 1};
 * @endcode
 *
 * @deprecated This struct is deprecated in favor of using the
 * `mdio::RangeDescriptor` struct.
 */
struct SliceDescriptor {
  DimensionIdentifier label;
  Index start;
  Index stop;
  Index step;

  // Implicit conversion to RangeDescriptor
  operator RangeDescriptor<Index>() const { return {label, start, stop, step}; }
};

/**
 * @brief A descriptor for slicing a single dimension value from a Dataset.
 * This structure is not supported for index-based slicing.
 * @tparam T The type of the value.
 * @param label The label of the dimension to slice.
 * @param value The value to slice.
 */
template <typename T>
struct ValueDescriptor {
  using type = T;
  /// The label of the dimension to slice.
  DimensionIdentifier label;
  /// The value to slice.
  T value;
};

/**
 * @brief A descriptor for slicing a list of values from a Dataset.
 * This structure is not supported for index-based slicing.
 * @tparam T The type of the values.
 * @param label The label of the dimension to slice.
 * @param values The vector of values to slice.
 */
template <typename T>
struct ListDescriptor {
  using type = T;
  /// The label of the dimension to slice.
  DimensionIdentifier label;
  /// The vector of values to slice.
  std::vector<T> values;
};

template <typename T>
struct outer_type {
  using type = T;
};

template <template <typename> class Outer, typename T>
struct outer_type<Outer<T>> {
  using type = Outer<T>;
};

// Specialization for lvalue references
template <typename T>
struct outer_type<T&> {
  using type = typename outer_type<std::remove_reference_t<T>>::type;
};

// Specialization for rvalue references
template <typename T>
struct outer_type<T&&> {
  using type = typename outer_type<std::remove_reference_t<T>>::type;
};

template <typename T>
struct extract_descriptor_Ttype {
  using type = T;
};

template <typename T>
struct extract_descriptor_Ttype<RangeDescriptor<T>> {
  using type = T;
};

template <typename T>
struct extract_descriptor_Ttype<ValueDescriptor<T>> {
  using type = T;
};

template <typename T>
struct extract_descriptor_Ttype<ListDescriptor<T>> {
  using type = T;
};

// Specialization for lvalue references
template <typename T>
struct extract_descriptor_Ttype<T&> {
  using type =
      typename extract_descriptor_Ttype<std::remove_reference_t<T>>::type;
};

// Specialization for rvalue references
template <typename T>
struct extract_descriptor_Ttype<T&&> {
  using type =
      typename extract_descriptor_Ttype<std::remove_reference_t<T>>::type;
};

namespace internal {

/**
 * @brief Checks a status for a missing driver message and returns an MDIO
 * specific error message.
 * @param status The status to check
 * @return A driver specific message if the status is a missing driver message,
 * otherwise the original status.
 */
absl::Status CheckMissingDriverStatus(const absl::Status& status) {
  std::string error(status.message());
  if (error.find("Error parsing object member \"driver\"") !=
      std::string::npos) {
    if (error.find("is not registered") != std::string::npos) {
      if (error.find("gcs") != std::string::npos) {
        return absl::InvalidArgumentError(
            "A GCS path was detected but the GCS driver was not "
            "registered.\nPlease ensure that your CMake includes the "
            "mdio_INTERNAL_GCS_DRIVER_DEPS variable.");
      } else if (error.find("s3") != std::string::npos) {
        return absl::InvalidArgumentError(
            "An S3 path was detected but the S3 driver was not "
            "registered.\nPlease ensure that your CMake includes the "
            "mdio_INTERNAL_S3_DRIVER_DEPS variable.");
      } else {
        return absl::InvalidArgumentError(
            "An unexpected driver registration error has occured. Please file "
            "a bug report with the error message to "
            "https://github.com/TGSAI/mdio-cpp/issues\n" +
            error);
      }
    }
  }
  return status;
}

/**
 * @brief Validates and processes a JSON specification for a tensorstore
 * variable.
 *
 * This function validates and processes the JSON specification for a zarr V2
 * tensorstore variable. It expects the JSON specification will have a field
 * called attributes that contains a field called dimension_names. This is a
 * specification of MDIO and is not specifically required by zarr V2
 *
 * @tparam Mode The read-write mode to use.
 * @param json_spec The JSON specification to validate and process.
 * @return A tuple containing the processed JSON specification and a new JSON
 * object with the kvstore and updated variable name.
 * @throws absl::InvalidArgumentError if the JSON specification is invalid
 * (according to the MDIO standard).
 */
template <ReadWriteMode Mode = ReadWriteMode::dynamic>
Result<std::tuple<nlohmann::json, nlohmann::json>> ValidateAndProcessJson(
    const nlohmann::json& json_spec) {
  // Check if attributes are in the original JSON
  if (!json_spec.contains("attributes")) {
    return absl::InvalidArgumentError(
        "The json_spec does not contain 'attributes'.");
  }

  // Check if dimensions field exists in attributes
  if (!json_spec["attributes"].contains("dimension_names")) {
    return absl::InvalidArgumentError(
        "The 'attributes' does not contain 'dimension_names'.");
  }

  nlohmann::json json_for_store = json_spec;

  // remove attributes, the v2 store won't parse it.
  json_for_store.erase("attributes");

  // Create a new JSON and add the kvstore
  nlohmann::json new_json = json_spec["attributes"];
  // update the variable name
  new_json["variable_name"] =
      std::filesystem::path(json_spec["kvstore"]["path"]).stem().string();

  return std::make_tuple(json_for_store, new_json);
}

/**
 * @brief Creates a Variable object.
 *
 * Parses the JSON spec for attributes constructs the Variable object according
 * to the MDIO standard.
 *
 * @tparam T The data type of the Variable.
 * @tparam R The rank of the Variable.
 * @tparam M The read-write mode of the Variable.
 * @param spec The JSON specification to parse. `spec` is expected to contain
 * ["attributes"]["variable_name"] and ["attributes"]["dimension_names"].
 * @param store The TensorStore to use for the Variable.
 * @return A Result object containing the created Variable.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic>
Result<Variable<T, R, M>> from_json(
    const ::nlohmann::json spec,
    const tensorstore::TensorStore<T, R, M> store) {
  auto attributes = spec;
  // There seems to be a case where we don't actually strip out the super
  // attributes field by this point.
  if (attributes.contains("attributes") &&
      attributes["attributes"].contains("variable_name")) {
    attributes = attributes["attributes"];
  }

  if (!attributes.contains("variable_name")) {
    return absl::NotFoundError("Could not find Variable's name.");
  }
  std::string name = attributes["variable_name"];

  nlohmann::json scrubbed_spec = attributes;
  scrubbed_spec.erase("variable_name");
  auto specWithoutChunkgrid = spec;
  if (specWithoutChunkgrid.contains("metadata")) {
    if (specWithoutChunkgrid["metadata"].contains("chunkGrid")) {
      specWithoutChunkgrid["metadata"].erase("chunkGrid");
    }
  }
  auto attrsRes = UserAttributes::FromVariableJson(specWithoutChunkgrid);
  if (!attrsRes.ok()) {
    return attrsRes.status();
  }

  // Default case.
  // TODO(BrianMichell): Look into making this optional.
  std::string long_name = "";
  if (scrubbed_spec.contains("long_name")) {
    if (scrubbed_spec["long_name"].is_string() &&
        scrubbed_spec["long_name"].get<std::string>().size() > 0) {
      long_name = scrubbed_spec["long_name"].get<std::string>();
    } else {
      scrubbed_spec.erase("long_name");
    }
  }  // Not having this field is fine. If it doesn't get seralized returning an
     // error status is not necessary.

  // These live in our `UserAttributes` object and are technically mutable. The
  // best kind of mutable!
  if (scrubbed_spec.contains("metadata")) {
    if (scrubbed_spec["metadata"].contains("attributes")) {
      scrubbed_spec["metadata"].erase("attributes");
    }
    if (scrubbed_spec["metadata"].contains("statsV1")) {
      scrubbed_spec["metadata"].erase("statsV1");
    }
    if (scrubbed_spec["metadata"].contains("unitsV1")) {
      scrubbed_spec["metadata"].erase("unitsV1");
    }
  }

  std::shared_ptr<std::shared_ptr<UserAttributes>> attrs =
      std::make_shared<std::shared_ptr<UserAttributes>>(
          std::make_shared<UserAttributes>(attrsRes.value()));
  const void* attributesAddress = static_cast<const void*>(&attributes);

  // clang-format off
    Variable<T, R, M> res = {
        name,
        long_name,
        scrubbed_spec,
        store,
        attrs
    };
  // clang-format on
  return res;
}

/**
 * @brief Creates a new variable with the given attributes and returns a future
 * that will be fulfilled with the created variable. Provide an Open method for
 * a new file ...
 * @tparam T The element type of the variable.
 * @tparam R The rank of the variable.
 * @tparam M The read-write mode of the variable.
 * @param json_store The JSON object representing the TensorStore to create the
 * variable in.
 * @param json_var The JSON object representing the attributes of the variable
 * to create.
 * @param options The transactional open options to use when opening the
 * TensorStore.
 * @return absl::StatusCode::kInvalidArgument if the attributes are empty.
 * @return mdio::Future<Variable<T, R, M>> A future that will be fulfilled with
 * the created variable.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic, typename... Option>
Future<Variable<T, R, M>> CreateVariable(const nlohmann::json& json_spec,
                                         const nlohmann::json& json_var,
                                         Option&&... options) {
  auto spec = tensorstore::MakeReadyFuture<::nlohmann::json>(json_var);

  // FIXME - add schematized validation of the variable.
  if (json_var.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "Expected attributes to be non-empty.");
  }

  if (!json_spec.contains("metadata")) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "Variable spec requires metadata");
  }

  if (!json_spec["metadata"].contains("dtype")) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "Variable metadata requires dtype");
  }

  MDIO_ASSIGN_OR_RETURN(auto zarr_dtype, tensorstore::internal_zarr::ParseDType(
                                             json_spec["metadata"]["dtype"]))

  // Handles the use case of creating a struct array, but intending to open as
  // void.
  auto do_handle_structarray =
      zarr_dtype.has_fields && !json_spec.contains("field");
  auto json_spec_with_field = json_spec;
  if (do_handle_structarray) {
    // pick the first name, it won't effect the .zarray json:
    json_spec_with_field["field"] = zarr_dtype.fields[0].name;
  }

  auto json_spec_without_metadata = json_spec;
  json_spec_without_metadata.erase("metadata");
  auto future_json_store = tensorstore::MakeReadyFuture<::nlohmann::json>(
      json_spec_without_metadata);

  auto publish = [](const ::nlohmann::json& json_var, bool isCloudStore,
                    const tensorstore::TensorStore<T, R, M>& store)
      -> Future<tensorstore::TimestampedStorageGeneration> {
    auto output_json = json_var;
    output_json["_ARRAY_DIMENSIONS"] = output_json["dimension_names"];
    output_json.erase("dimension_names");
    output_json.erase("variable_name");
    if (output_json.contains("metadata")) {
      output_json["metadata"].erase("chunkGrid");
      for (auto& item : output_json["metadata"].items()) {
        output_json[item.key()] = std::move(item.value());
      }
      output_json.erase("metadata");
    }
    if (output_json.contains("long_name")) {
      if (output_json["long_name"] == "") {
        output_json.erase("long_name");
      }
    }
    // Case where empty array of coordinates is provided
    if (output_json.contains("coordinates")) {
      auto coords = output_json["coordinates"];
      if (coords.empty() ||
          (coords.is_string() && coords.get<std::string>() == "")) {
        output_json.erase("coordinates");
      }
    }
    std::string outpath = "/.zattrs";
    if (isCloudStore) {
      outpath = ".zattrs";
    }
    // It's important to use the store's kvstore or else we get a race condition
    // on "mkdir".
    return tensorstore::kvstore::Write(store.kvstore(), outpath,
                                       absl::Cord(output_json.dump(4)));
  };

  // this is intended to handle the struct array where we "reopen" the store
  // but this time as struct ... but we loose the capactity for forward options.
  // FIXME - strip "create/delete existing" and foward other options.
  auto apply_reopen = [](const tensorstore::TensorStore<T, R, M>& store,
                         const ::nlohmann::json& attributes,
                         const ::nlohmann::json& json_spec) {
    return tensorstore::Open<T, R, M>(json_spec);
  };

  auto build = [](const ::nlohmann::json& metadata,
                  const tensorstore::TensorStore<T, R, M>& store)
      -> Future<Variable<T, R, M>> {
    auto labeled_store = store;

    if (metadata.contains("dimension_names")) {
      auto dimension_names =
          metadata["dimension_names"].get<std::vector<std::string>>();
      for (DimensionIndex i = 0; i < dimension_names.size(); ++i) {
        MDIO_ASSIGN_OR_RETURN(
            labeled_store,
            labeled_store | tensorstore::Dims(i).Label(dimension_names[i]));
      }
    }
    return from_json<T, R, M>(metadata, labeled_store);
  };

  // Start by creating a future for the store ...
  auto future_store = tensorstore::Open<T, R, M>(
      json_spec_with_field, std::forward<Option>(options)...);

  auto handled_store = future_store;
  if (do_handle_structarray) {
    handled_store =
        tensorstore::MapFutureValue(tensorstore::InlineExecutor{}, apply_reopen,
                                    future_store, spec, future_json_store);
  } else {
    handled_store = std::move(future_store);
  }

  auto variable_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{}, build, spec, handled_store);

  bool isCloudStore = false;
  std::string driver = json_spec["kvstore"]["driver"].get<std::string>();
  if (driver == "gcs" || driver == "s3") {
    isCloudStore = true;
  }
  auto isCloudStoreRF = tensorstore::MakeReadyFuture<bool>(isCloudStore);
  auto write_metadata_future =
      tensorstore::MapFutureValue(tensorstore::InlineExecutor{}, publish, spec,
                                  isCloudStoreRF, handled_store);

  // wait for the variable to be created and the zattrs to be written.
  auto all_done_future =
      tensorstore::WaitAllFuture(variable_future, write_metadata_future);

  auto pair = tensorstore::PromiseFuturePair<Variable<T, R, M>>::Make();
  all_done_future.ExecuteWhenReady(
      [promise = pair.promise, variable_future = std::move(variable_future)](
          tensorstore::ReadyFuture<void> readyFut) {
        auto ready_result = readyFut.result();
        if (!ready_result.ok()) {
          promise.SetResult(CheckMissingDriverStatus(ready_result.status()));
        } else {
          promise.SetResult(variable_future.result());
        }
      });

  return pair.future;
}

/**
 * @brief Opens a Variable from a JSON specification.
 * Provide an Open method for an existing file...
 * @tparam T The data type of the Variable.
 * @tparam R The rank of the Variable.
 * @tparam M The read-write mode of the Variable.
 * @param json_store The JSON specification of the store.
 * @param options The transactional open options.
 * @return mdio::Future<Variable<T, R, M>> A future that resolves to the opened
 * Variable.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic, typename... Option>
Future<Variable<T, R, M>> OpenVariable(const nlohmann::json& json_store,
                                       Option&&... options) {
  // Infer the name from the path
  std::string variable_name = json_store["kvstore"]["path"].get<std::string>();
  std::vector<std::string> pathComponents = absl::StrSplit(variable_name, "/");
  variable_name = pathComponents.back();

  auto store_spec = json_store;
  // retain attributes if we want to check values ...
  ::nlohmann::json suppliedAttributes;
  if (store_spec.contains("attributes")) {
    suppliedAttributes["attributes"] = store_spec["attributes"];
    store_spec.erase("attributes");
  }
  // attributes is not a valid key for the tensorstore open ...
  // FIXME - resolve opening struct array with field and no metadata
  //         updates to Tensorstore required ...
  if (!store_spec.contains("field") && store_spec.contains("metadata")) {
    store_spec.erase("metadata");
  }
  // the negative of this is valid for tensorstore ...

  auto spec = tensorstore::MakeReadyFuture<::nlohmann::json>(store_spec);

  // open a store:
  auto future_store =
      tensorstore::Open<T, R, M>(store_spec, std::forward<Option>(options)...);

  // start by creating a kvstore future ...
  auto kvs_future = tensorstore::kvstore::Open(store_spec["kvstore"]);

  // go read the metadata return json ...
  auto read = [](const tensorstore::KvStore& kvstore)
      -> Future<tensorstore::kvstore::ReadResult> {
    return tensorstore::kvstore::Read(kvstore, "/.zattrs");
  };

  // go read the attributes return json ...
  auto parse = [](const tensorstore::kvstore::ReadResult& kvs_read,
                  const ::nlohmann::json& spec) {
    auto attributes =
        nlohmann::json::parse(std::string(kvs_read.value), nullptr, false);
    return attributes;
  };

  auto make_variable = [variable_name](
                           const ::nlohmann::json& metadata,
                           const tensorstore::TensorStore<T, R, M>& store,
                           const ::nlohmann::json& suppliedAttributes)
      -> Future<Variable<T, R, M>> {
    // use to load the store
    tensorstore::TensorStore<T, R, M> labeled_store = store;
    ::nlohmann::json updated_metadata = metadata;

    // Create a new JSON object with "attributes" as the parent key
    ::nlohmann::json new_metadata;
    new_metadata = updated_metadata;
    new_metadata["variable_name"] = variable_name;

    if (new_metadata.contains("_ARRAY_DIMENSIONS")) {
      // Move "_ARRAY_DIMENSIONS" to "dimension_names"
      new_metadata["dimension_names"] = new_metadata["_ARRAY_DIMENSIONS"];
      new_metadata.erase("_ARRAY_DIMENSIONS");
    }

    if (new_metadata.contains("dimension_names")) {
      auto dimension_names =
          new_metadata["dimension_names"].get<std::vector<std::string>>();
      for (DimensionIndex i = 0; i < dimension_names.size(); ++i) {
        MDIO_ASSIGN_OR_RETURN(
            labeled_store,
            labeled_store | tensorstore::Dims(i).Label(dimension_names[i]));
      }
    } else {
      return absl::NotFoundError(
          absl::StrCat("Field not found in JSON: ", "metadata"));
    }

    if (new_metadata.contains("attributes")) {
      new_metadata["metadata"]["attributes"] = new_metadata["attributes"];
      new_metadata.erase("attributes");
    }
    if (new_metadata.contains("statsV1")) {
      new_metadata["metadata"]["statsV1"] = new_metadata["statsV1"];
      new_metadata.erase("statsV1");
    }
    if (new_metadata.contains("unitsV1")) {
      new_metadata["metadata"]["unitsV1"] = new_metadata["unitsV1"];
      new_metadata.erase("unitsV1");
    }

    if (!suppliedAttributes.is_null()) {
      // The supplied attributes contain some things that we do not serialize.
      // We need to remove them. This could cause confusion. If the user
      // specifies a different chunkGrid, it will not be used and should
      // actually fail here.
      nlohmann::json correctedSuppliedAttrs = suppliedAttributes;
      if (correctedSuppliedAttrs.contains("attributes")) {
        if (correctedSuppliedAttrs["attributes"].contains("metadata")) {
          if (correctedSuppliedAttrs["attributes"]["metadata"].contains(
                  "chunkGrid")) {
            correctedSuppliedAttrs["attributes"]["metadata"].erase("chunkGrid");
          }
        }
        auto savedAttrs = correctedSuppliedAttrs["attributes"];
        correctedSuppliedAttrs.erase("attributes");
        for (auto& item : savedAttrs.items()) {
          correctedSuppliedAttrs[item.key()] = std::move(item.value());
        }
      }
      // BFS to make sure supplied attributes match stored attributes
      nlohmann::json searchableMetadata = new_metadata;
      if (searchableMetadata.contains("variable_name")) {
        // Since we don't actually want to have to specify the variable name
        searchableMetadata.erase("variable_name");
      }
      std::queue<std::pair<nlohmann::json, nlohmann::json>> queue;
      queue.push({searchableMetadata, correctedSuppliedAttrs});
      while (!queue.empty()) {
        auto [currentMetadata, currentAttributes] = queue.front();
        queue.pop();
        for (auto& [key, value] : currentMetadata.items()) {
          if (!currentAttributes.contains(key)) {
            return absl::NotFoundError(
                absl::StrCat("Field not found in JSON: ", key));
          }
          if (value.is_object() && currentAttributes[key].is_object()) {
            // If the value is a JSON object, add it to the queue for further
            // processing
            queue.push({value, currentAttributes[key]});
          } else if (value != currentAttributes[key]) {
            return absl::InvalidArgumentError(absl::StrCat(
                "Conflicting values for field: ", key, ". ", "Expected: ",
                value.dump(4), ", but got: ", currentAttributes[key].dump(4)));
          }
        }
      }
    }

    return from_json<T, R, M>(new_metadata, labeled_store);
  };

  // a future to the metadata ...
  auto kvs_read_future = tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{}, read, kvs_future);

  auto metadata = tensorstore::MapFutureValue(tensorstore::InlineExecutor{},
                                              parse, kvs_read_future, spec);

  return tensorstore::MapFutureValue(
      tensorstore::InlineExecutor{}, make_variable, metadata, future_store,
      tensorstore::MakeReadyFuture<::nlohmann::json>(suppliedAttributes));
}

/**
 * @brief Opens or creates a Variable object from a JSON specification.
 * Provide an Open method for an existing file...
 * @tparam T The data type of the Variable.
 * @tparam R The rank of the Variable.
 * @tparam M The read/write mode of the Variable.
 * @param json_spec The JSON specification of the Variable.
 * @param options The transactional open options.
 * @return A future that resolves to the opened or created Variable object.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic>
Future<Variable<T, R, M>> Open(const nlohmann::json& json_spec,
                               TransactionalOpenOptions&& options) {
  // situations where we would create new metadata
  if (options.open_mode == constants::kCreateClean ||
      options.open_mode == constants::kCreate) {
    MDIO_ASSIGN_OR_RETURN(auto json_schema, ValidateAndProcessJson(json_spec))
    // extract the json for the store and our metadata
    auto [json_store, metadata] = json_schema;
    // this will write metadata
    return CreateVariable<T, R, M>(json_store, metadata, std::move(options));
  } else {
    return OpenVariable<T, R, M>(json_spec, std::move(options));
  }
}
}  // namespace internal

/**
 * @brief A templated struct representing an MDIO Variable with a tensorstore.
 * This is an MDIO specified zarr V2 tensorstore variable.
 * It represents the non-volitile (on-disk, in-cloud, etc.) data.
 *
 * @tparam T The type of the data stored in the tensorstore.
 * @tparam R The rank of the tensorstore.
 * @tparam M The read-write mode of the tensorstore.
 * @param variableName The name of the variable.
 * @param longName The optional long name of the variable.
 * @param metadata Any metadata associated with the variable.
 * @param store The underlying Tensorstore.
 * @param attributes The user attributes associated with the variable.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ReadWriteMode M = ReadWriteMode::dynamic>
class Variable {
 public:
  Variable() = default;

  Variable(const std::string& variableName, const std::string& longName,
           const ::nlohmann::json& metdata,
           const tensorstore::TensorStore<T, R, M>& store,
           const std::shared_ptr<std::shared_ptr<UserAttributes>> attributes)
      : variableName(variableName),
        longName(longName),
        metadata(metdata),
        store(store),
        attributes(attributes) {
    attributesAddress = reinterpret_cast<std::uintptr_t>((*attributes).get());
  }

  // Allows for conversion to compatible types (SourceElement), which should
  // always be possible to void.
  template <typename SourceElement, DimensionIndex SourceRank,
            ReadWriteMode SourceMode>
  Variable(const Variable<SourceElement, SourceRank, SourceMode>& other)
      : variableName(other.get_variable_name()),
        longName(other.get_long_name()),
        metadata(other.getReducedMetadata()),
        store(other.get_store()),
        attributes(other.attributes),
        attributesAddress(other.get_attributes_address()) {}

  friend std::ostream& operator<<(std::ostream& os, const Variable& obj) {
    os << obj.variableName << "\t" << obj.dimensions() << "\n";
    os << obj.store.dtype() << "\t" << obj.store.rank();
    return os;
  }

  /**
   * @brief Opens a variable with the specified options.
   * Provide an Open method for an existing file...
   * @tparam T The element type of the variable. Defaults to `void`.
   * @tparam R The rank of the variable. Defaults to `mdio::dynamic_rank`.
   * @tparam M The read/write mode of the variable. Defaults to
   * `mdio::ReadWriteMode::dynamic`.
   * @tparam Option The options to use when opening the variable.
   * @param json_spec The JSON specification of the variable to open.
   * @param option The options to use when opening the variable.
   * @return An `mdio::Future` that resolves to a `Variable` object.
   *
   * This function opens an existing Variable with the specified options.
   */
  template <typename... Option>
  static std::enable_if_t<(tensorstore::IsCompatibleOptionSequence<
                              TransactionalOpenOptions, Option...>),
                          Future<Variable<T, R, M>>>
  Open(const nlohmann::json& json_spec, Option&&... option) {
    TENSORSTORE_INTERNAL_ASSIGN_OPTIONS_OR_RETURN(TransactionalOpenOptions,
                                                  options, option)
    return mdio::internal::Open<T, R, M>(json_spec, std::move(options));
  }

  /**
   * @brief Read the data from the variable.
   * Reads the data from the source variable.
   * Provide an Open method for an existing file...
   * @tparam T The type of the data to be read.
   * @tparam R The tensorstore rank of the data to be read.
   * @tparam M The read/write mode of the data to be read.
   * @param variable A Variable object with the source store.
   * @return A future of VariableData that will be ready when the read is
   * complete.
   */
  template <ArrayOriginKind OriginKind = offset_origin>
  Future<VariableData<T, R, OriginKind>> Read() {
    auto data = tensorstore::Read(store);
    // We need to capture this to ensure the Variable doesn't get prematurely
    // destoryed if its parent goes out of scope before the future resolves.
    auto thisVar = std::make_shared<Variable<T, R, M>>(*this);
    auto pair =
        tensorstore::PromiseFuturePair<VariableData<T, R, OriginKind>>::Make();
    data.ExecuteWhenReady(
        [thisVar, promise = pair.promise](
            tensorstore::ReadyFuture<SharedArray<T, R, OriginKind>> readyFut) {
          auto ready_result = readyFut.result();
          if (!ready_result.ok()) {
            promise.SetResult(ready_result.status());
          } else {
            LabeledArray<T, R, OriginKind> labeledArray{thisVar->dimensions(),
                                                        ready_result.value()};
            VariableData<T, R, OriginKind> variableData{
                thisVar->variableName, thisVar->longName,
                thisVar->getMetadata(), labeledArray};
            promise.SetResult(variableData);
          }
        });

    return pair.future;
  }

  /**
   * @brief Write the data to the variable.
   * Writes the data from the source variable data to the target variable.
   * @param source A VariableData object with the data to write. This is the
   * in-memory representation of the data.
   * @param target A Variable object with the target store. This is the
   * non-volitile representation of the data.
   *
   * @details \b Usage
   * This provides an example of writing the data from the source variable data
   * to the target variable.
   * @code
   * MDIO_ASSIGN_OR_RETURN(auto velocity, mdio::Variable<>::Open(velocity_path,
   *                                                  mdio::constants::kOpen));
   * // Get an empty version of the Variable.
   * MDIO_ASSIGN_OR_RETURN(auto velocityData, mdio::from_variable(velocity));
   * // Do some manipulation of velocity here before writing it out.
   * auto velocityWriteFuture = velocity.Write(velocityData);
   * // This is a future. It will be ready when the write is complete.
   * @endcode
   * @return A future that will be ready when the write is complete.
   */
  template <ArrayOriginKind OriginKind = offset_origin>
  WriteFutures Write(const VariableData<T, R, OriginKind> source) const {
    if (source.dtype() != this->dtype()) {
      return absl::InvalidArgumentError(
          "The source and target dtypes do not match.");
    }
    return tensorstore::Write(source.data.data, store);
  }

  /**
   * @brief Returns the index domain view of the variable.
   * Specifies the origin, shape and labels of the domain
   * @return The index domain view of the variable.
   */
  IndexDomainView<R> dimensions() const { return store.domain(); }

  /**
   * @brief Gets the rank of the variable.
   * @return std::size_t The rank of the variable.
   */
  std::size_t rank() const { return store.rank(); }

  /**
   * @brief Returns the number of samples in the variable.
   * @return Index The number of samples in the variable.
   */
  Index num_samples() const {
    // Accessing the shape
    auto shape = dimensions().shape();
    // Calculating the total number of elements
    size_t totalElements = 1;
    for (auto dim_size : shape) {
      totalElements *= dim_size;
    }
    return totalElements;
  }

  /**
   * @brief Get the data type of the tensor store.
   * The type of the store, T ~ void
   * @return The data type of the tensor store.
   */
  DataType dtype() const { return store.dtype(); }

  /**
   * @brief Retrieves the specification of the variable.
   * Includes information about the compressor etc.
   * This function returns an `mdio::Result` object containing the specification
   * of the variable.
   * @return An `mdio::Result` object containing the specification of the
   * variable.
   */
  Result<Spec> spec() const { return store.spec(); }

  /**
   * @brief Checks if the Variable contains a specified label
   * @param labelToCheck The label to check for
   * @return true if the Variable contains the label, false otherwise
   */
  bool hasLabel(const DimensionIdentifier& labelToCheck) const {
    // if it's an index slice, that is always valid ...
    if (!labelToCheck.label().data()) {
      auto rank = store.domain().rank();
      // and we're in bounds
      return (labelToCheck.index() < rank && labelToCheck.index() >= 0);
    }

    // otherwise see if the label is in the domain
    const auto labels = store.domain().labels();
    for (const auto& label : labels) {
      if (label == labelToCheck) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Clamps a slice descriptor to the domain of the Variable.
   * Intended for internal use.
   * @param desc The slice descriptor to be clamped
   * @return A slice descriptor that will not go out-of-bounds for the given
   * Variable.
   */
  RangeDescriptor<Index> sliceInRange(
      const RangeDescriptor<Index>& desc) const {
    auto domain = dimensions();
    const auto labels = domain.labels();

    for (size_t idx = 0; idx < labels.size(); ++idx) {
      if (labels[idx] == desc.label) {
        // Clamp the descriptor to the domain.
        return {desc.label,  // label
                desc.start < domain.origin()[idx] ? domain.origin()[idx]
                                                  : desc.start,  // start
                desc.stop > domain.shape()[idx] ? domain.shape()[idx]
                                                : desc.stop,  // stop
                desc.step};                                   // step
      }
    }
    // We don't slice along a dimension that doesn't exist, so the descriptor is
    // valid
    return desc;
  }

  template <size_t... I>
  struct index_sequence {};

  template <size_t N, size_t... I>
  struct make_index_sequence : make_index_sequence<N - 1, N - 1, I...> {};

  template <size_t... I>
  struct make_index_sequence<0, I...> : index_sequence<I...> {};

  template <std::size_t... I>
  Result<Variable> call_slice_with_vector_impl(
      const std::vector<RangeDescriptor<Index>>& slices,
      std::index_sequence<I...>) {
    return slice(slices[I]...);
  }

  /**
   * @brief An overload of the `slice` method that takes a vector of
   * RangeDescriptors. This method is limited to `internal::kMaxNumSlices`
   * slices. This overload should only ever be used when a runtime number of
   * slices must be generated.
   */
  Result<Variable> slice(const std::vector<RangeDescriptor<Index>>& slices) {
    if (slices.empty()) {
      return absl::InvalidArgumentError("No slices provided.");
    }

    if (slices.size() > internal::kMaxNumSlices) {
      // We are expecting the only entry point for this method to be fro mthe
      // Dataset::isel method. That method should handle the partitioning of the
      // slices.
      return absl::InvalidArgumentError(
          absl::StrCat("Too many slices provided or implicitly generated. "
                       "Maximum number of slices is ",
                       internal::kMaxNumSlices, " but ", slices.size(),
                       " were provided.\n\tUse -DMAX_NUM_SLICES cmake flag to "
                       "increase the maximum number of slices."));
    }

    std::vector<RangeDescriptor<Index>> slicesCopy = slices;
    for (int i = slices.size(); i < internal::kMaxNumSlices; ++i) {
      slicesCopy.emplace_back(
          RangeDescriptor<Index>({internal::kInertSliceKey, 0, 1, 1}));
    }

    return call_slice_with_vector_impl(
        slicesCopy, std::make_index_sequence<internal::kMaxNumSlices>{});
  }

  /**
   * @brief Slices the Variable along the specified dimensions and returns the
   * resulting sub-Variable. This slice is performed as a half open interval.
   * Dimensions that are not described will remain fully intact.
   * @pre The start of the slice descriptor must be less than the stop.
   * @post The resulting Variable will be sliced along the specified dimensions
   * within it's domain. If the slice lay outside of the domain of the Variable,
   * the slice will be clamped to the domain.
   * @param descriptors The descriptors used to specify the slice.
   * @details \b Usage
   * This provides an example of slicing the Variable along the inline and
   * crossline dimensions.
   * @code
   * mdio::RangeDescriptor<Index> desc1 = {"inline", 0, 100, 1};
   * mdio::RangeDescriptor<Index> desc2 = {"crossline", 0, 200, 1};
   * MDIO_ASSIGN_OR_RETURN(auto sliced_velocity, velocity.slice(desc1, desc2));
   * @endcode
   * @return An `mdio::Result` object containing the resulting sub-Variable.
   */
  template <typename... Descriptors>
  Result<Variable> slice(const Descriptors&... descriptors) const {
    // 1) Pack descriptors
    constexpr size_t N = sizeof...(Descriptors);
    std::array<RangeDescriptor<Index>, N> descs = {descriptors...};

    // 2) Clamp + precondition check
    std::vector<DimensionIdentifier> labels;
    std::vector<Index> starts, stops, steps;
    labels.reserve(N);
    starts.reserve(N);
    stops.reserve(N);
    steps.reserve(N);

    int8_t bad_idx = -1;
    for (size_t i = 0; i < N; ++i) {
      auto d = sliceInRange(descs[i]);
      if (d.start > d.stop) {
        bad_idx = static_cast<int8_t>(i);
        break;
      }
      if (this->hasLabel(d.label)) {
        labels.push_back(d.label);
        starts.push_back(d.start);
        stops.push_back(d.stop);
        steps.push_back(d.step);
      }
    }
    if (bad_idx >= 0) {
      auto& err = descs[bad_idx];
      return Result<Variable>{absl::InvalidArgumentError(
          std::string("Slice descriptor for ") +
          std::string(err.label.label()) + " is invalid: start=" +
          std::to_string(err.start) + " > stop=" + std::to_string(err.stop))};
    }

    // 3) Fast path: all labels (or axis indices) are unique
    if (!labels.empty()) {
      std::set<std::string_view> labelSet;
      std::set<DimensionIndex> indexSet;
      for (auto& lab : labels) {
        labelSet.insert(lab.label());
        indexSet.insert(lab.index());
      }
      if (labelSet.size() == labels.size() ||
          indexSet.size() == labels.size()) {
        MDIO_ASSIGN_OR_RETURN(
            auto slice_store,
            store | tensorstore::Dims(labels).HalfOpenInterval(starts, stops,
                                                               steps));
        return Variable{variableName, longName, metadata,
                        std::move(slice_store), attributes};
      }
    }

    // 4) Group by label to find any duplicates
    std::map<std::string_view, std::vector<RangeDescriptor<Index>>> by_label;
    for (auto& d : descs) {
      if (d.label.label() != internal::kInertSliceKey) {
        by_label[d.label.label()].push_back(d);
      }
    }

    // 5) Handle the first label that has >1 descriptor
    for (auto& [label, vec] : by_label) {
      if (vec.size() > 1) {
        // 5a) Unwrap the Spec so we can ask for transform().input_labels()
        MDIO_ASSIGN_OR_RETURN(auto spec, store.spec());
        auto spec_labels = spec.transform().input_labels();

        // find the numeric axis for this label
        auto it = std::find(spec_labels.begin(), spec_labels.end(), label);
        if (it == spec_labels.end()) {
          // no-op if the label isn't in the spec; skip it
          continue;
        }
        int axis = static_cast<int>(std::distance(spec_labels.begin(), it));

        // 5b) Slice each sub‑range in isolation
        std::vector<tensorstore::TensorStore<T, R, M>> pieces;
        pieces.reserve(vec.size());
        for (auto& r : vec) {
          auto sub = slice(r);
          if (!sub.status().ok()) return sub.status();
          pieces.push_back(sub.value().get_store());
        }

        // 5c) Concatenate them along the correct axis
        MDIO_ASSIGN_OR_RETURN(auto cat_store,
                              tensorstore::Concat(pieces, axis));

        return Variable{variableName, longName, metadata, std::move(cat_store),
                        attributes};
      }
    }

    // 6) No descriptors matched → no change
    return *this;
  }

  /**
   * @brief Retrieves the spec of the Variable as a JSON object.
   * @return A JSON object representing the spec of the Variable.
   */
  Result<nlohmann::json> get_spec() const {
    auto spec_res = spec();
    if (!spec_res.ok()) {
      return spec_res.status();
    }
    auto spec = spec_res.value();
    auto json_res = spec.ToJson(mdio::IncludeDefaults{});
    if (!json_res.ok()) {
      return json_res.status();
    }
    return json_res.value();
  }

  /**
   * @brief Retrieves the specified chunk shape of the Variable if it exists.
   * @details \b Usage
   * @code
   *  MDIO_ASSIGN_OR_RETURN(auto chunkShape, velocity.get_chunk_shape());
   *  // Build descriptors to slice out a swath, here we will get every chunk in
   *  // the z-direction
   * mdio::RangeDescriptor<Index> desc1 = {"inline", chunkShape[0],
   * chunkShape[0] * 2, 1}; mdio::RangeDescriptor<Index> desc2 = {"crossline",
   * chunkShape[1], chunkShape[1] * 2, 1};
   * @endcode
   * @return An NotFoundError if the chunk shape could not be retrieved,
   * otherwise a vector of the chunk shape.
   */
  Result<std::vector<DimensionIndex>> get_chunk_shape() const {
    auto spec_res = get_spec();
    if (!spec_res.status().ok()) {
      return spec_res;
    }

    nlohmann::json json = spec_res.value();
    if (!json.contains("metadata")) {
      return absl::NotFoundError("Metadata did not contain key 'metadata'.");
    }
    if (!json["metadata"].contains("chunks")) {
      return absl::NotFoundError(
          "Metadata['attributes'] did not contain key 'chunks'.");
    }
    if (!json["metadata"]["chunks"].is_array()) {
      return absl::InvalidArgumentError("Metadata['chunks'] is not an array.");
    }
    return json["metadata"]["chunks"]
        .get<std::vector<long int>>();  // NOLINT: Tensorstore convention
  }

  /**
   * @brief Retrieves the entire shape of the Variable if it exists.
   * @return A NotFoundError if the shape could not be retrieved, otherwise a
   * vector of the shape.
   */
  Result<std::vector<DimensionIndex>> get_store_shape() const {
    auto spec_res = get_spec();
    if (!spec_res.status().ok()) {
      return spec_res;
    }
    nlohmann::json json = spec_res.value();

    if (!json.contains("metadata")) {
      return absl::NotFoundError("Metadata did not contain key 'metadata'.");
    }
    if (!json["metadata"].contains("shape")) {
      return absl::NotFoundError(
          "Metadata['attributes'] did not contain key 'shape'.");
    }
    if (!json["metadata"]["shape"].is_array()) {
      return absl::InvalidArgumentError("Metadata['shape'] is not an array.");
    }
    return json["metadata"]["shape"]
        .get<std::vector<long int>>();  // NOLINT: Tensorstore convention
  }

  /**
   * @brief Publishes new ".zattrs" metadata to the Variable's durable storage.
   * This method should not be called independantly as it will result in a
   * mismatch between the Variable metadata and Dataset metadata
   * @return A future representing the timestamped storage generation of the
   * updated Variable.
   */
  Future<tensorstore::TimestampedStorageGeneration> PublishMetadata() {
    auto publish = [](const ::nlohmann::json& json_var, bool isCloudStore,
                      const tensorstore::TensorStore<T, R, M>& store)
        -> Future<tensorstore::TimestampedStorageGeneration> {
      auto output_json = json_var;

      output_json["attributes"]["_ARRAY_DIMENSIONS"] =
          output_json["dimension_names"];
      output_json.erase("dimension_names");
      if (output_json.contains("long_name")) {
        output_json["attributes"]["long_name"] = output_json["long_name"];
        output_json.erase("long_name");
      }
      if (output_json.contains("coordinates")) {
        output_json["attributes"]["coordinates"] = output_json["coordinates"];
        output_json.erase("coordinates");
      }
      std::string outpath = "/.zattrs";
      if (isCloudStore) {
        outpath = ".zattrs";
      }

      if (output_json.contains("metadata")) {
        output_json["attributes"]["metadata"] = output_json["metadata"];
        output_json.erase("metadata");
      }

      return tensorstore::kvstore::Write(
          store.kvstore(), outpath,
          absl::Cord(output_json["attributes"].dump(4)));
    };

    bool isCloudStore = false;
    // TODO(BrianMichell): Make more error tolerant
    auto json_spec = store.spec().value().ToJson(IncludeDefaults{}).value();
    auto driver = json_spec["kvstore"]["driver"];
    if (driver == "gcs" || driver == "s3") {
      isCloudStore = true;
    }
    auto isCloudStoreRF = tensorstore::MakeReadyFuture<bool>(isCloudStore);
    auto metadataRF =
        tensorstore::MakeReadyFuture<::nlohmann::json>(this->getMetadata());
    auto storeRF =
        tensorstore::MakeReadyFuture<tensorstore::TensorStore<T, R, M>>(store);
    auto write_metadata_future =
        tensorstore::MapFutureValue(tensorstore::InlineExecutor{}, publish,
                                    metadataRF, isCloudStoreRF, storeRF);
    auto pair = tensorstore::PromiseFuturePair<
        tensorstore::TimestampedStorageGeneration>::Make();
    write_metadata_future.ExecuteWhenReady(
        [this, promise = std::move(pair.promise)](
            tensorstore::ReadyFuture<tensorstore::TimestampedStorageGeneration>
                readyFut) {
          auto ready_result = readyFut.result();
          if (!ready_result.ok()) {
            promise.SetResult(ready_result.status());
          } else {
            _dataset_only_callback_committed();
            promise.SetResult(ready_result.value());
          }
        });
    // return write_metadata_future;
    return pair.future;
  }

  /**
   * @brief Attempts to safely update the user attributes of a Variable
   * (["metadata"]["attributes"] and/or ["metadata"]["statsV1"]).
   * @tparam T_attrs The optional type of the histogram. Must be either int32_t
   * or float (Default: float)
   * @param newAttrs The JSON representation of a UserAttribute.
   * @return An OK status if the attributes were successfully updated, otherwise
   * an error and the attributes remain unchanged.
   * @details \b Intended_Usage
   * @code
   * // Status check ignored for brevity
   * auto var = my_dataset.variables.at("my_variable_key").value();
   * nlohmann::json to_update = var.GetAttributes();
   * to_update["attributes"]["new_attr"] = "new_value";
   * std::vector<int32_t> binCenters;
   * // Populate with your data as needed
   * to_update["statsV1"]["histogram"]["binCenters"] = binCenters;
   * // NOTE: The vector is of type int32_t. We must pass the type as a
   * template argument.
   * auto update_result = var.UpdateAttributes<int32_t>(to_update);
   * if (!update_result.status().ok()) {
   *      // In this case nothing will happen. We output an error and move on
   *      // with what previously existed.
   *      std::cerr << "Failed to update attributes: " <<
   *      update_result.status() << std::endl;
   * }
   * @endcode
   * NOTE: This does not commit changes to durable media.
   * Please see CommitMetadata method in the Dataset to commit changes.
   */
  template <typename T_attrs = float>
  Result<void> UpdateAttributes(const nlohmann::json& newAttrs) {
    auto res = (*attributes)->template FromJson<T_attrs>(newAttrs);
    if (res.status().ok()) {
      // Create a new UserAttributes object and update the inner std::shared_ptr
      *attributes = std::make_shared<UserAttributes>(res.value());
    }
    return res;
  }

  /**
   * @brief Gets the User Attributes of the Variable as a JSON object.
   * Returned object is expected to have a parent key of "attributes".
   * This is expected to be the exact copy of the attributes field.
   * @see
   * https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#mdio.schema.v1.variable.VariableMetadata.attributes
   * @return The User Attributes in JSON form.
   */
  nlohmann::json GetAttributes() const {
    // Dereference the outer std::shared_ptr to get the inner std::shared_ptr
    return (*attributes)->ToJson();
  }

  /**
   * @brief Gets the entire metadata of the Variable.
   * Returned object is expected to have a parent key of "metadata".
   * @return The metadata in JSON form
   */
  nlohmann::json getMetadata() const {
    auto ret = getReducedMetadata();
    auto attrs = GetAttributes();
    // Check for not being a `nlohmann::json::object()`
    if (attrs.is_object() && !attrs.empty()) {
      if (!ret.contains("metadata")) {
        ret["metadata"] = nlohmann::json::object();
      }
      ret["metadata"].merge_patch(attrs);
    }
    return ret;
  }

  /**
   * @brief A reduced version of the metadata
   * NOTE: This may only be useful for internal uses. See `getMetadata()` for
   * the full metadata. This version should lack the mutable UserAttributes
   * portions, if they exist.
   * @return The reduced metadata in JSON form
   */
  const nlohmann::json getReducedMetadata() const {
    nlohmann::json ret = metadata;
    ret["long_name"] = longName;
    return metadata;
  }

  /**
   * @brief Checks if the User Attributes has changed in the Variable.
   * @return true if the User Attributes has changed, false otherwise.
   */
  const bool was_updated() const {
    // We need a double dereference to get the address of the underlying object
    // This works because the UserAttributes object is immutable and can only be
    // replaced.
    std::uintptr_t currentAddress =
        reinterpret_cast<std::uintptr_t>(&(**attributes));
    return attributesAddress != currentAddress;
  }

  /**
   * @brief Sets the flag whether the metadata should get republished.
   * Intended for internal use with the trimming utility.
   *
   * @param shouldPublish True if the metadata should get republished.
   */
  void set_metadata_publish_flag(const bool shouldPublish) {
    if (!toPublish) {
      // This should never be the case, but better safe than sorry
      toPublish = std::make_shared<std::shared_ptr<bool>>(
          std::make_shared<bool>(shouldPublish));
    } else {
      *toPublish = std::make_shared<bool>(shouldPublish);
    }
  }

  /**
   * @brief A struct representing the half open interval of a dimension.
   */
  struct Interval {
    friend std::ostream& operator<<(std::ostream& os, const Interval& obj) {
      os << obj.label.label() << ": [" << obj.inclusive_min << ", "
         << obj.exclusive_max << ")";
      return os;
    }

    /// The string or index label of the dimension.
    mdio::DimensionIdentifier label;
    /// The inclusive minimum of the interval.
    mdio::Index inclusive_min;
    /// The exclusive maximum of the interval.
    mdio::Index exclusive_max;
  };

  /**
   * @brief Gets the domain of the whole Variable or selected dimensions.
   * @param labels The DimensionIdentifier(s) of the dimensions to get.
   * @return A vector of the domain of the selected dimensions, or the whole
   * domain if no labels are provided.
   */
  template <typename... DimensionIdentifier>
  mdio::Result<std::vector<Interval>> get_intervals(
      const DimensionIdentifier&... labels) const {
    constexpr size_t numLabels = sizeof...(labels);
    std::vector<Interval> intervals;
    auto domain = store.domain();

    if (numLabels == 0) {
      const auto labels = domain.labels();
      auto idx(0);
      for (const auto& label : labels) {
        Interval interval = {label, domain[idx].interval().inclusive_min(),
                             domain[idx].interval().exclusive_max()};
        intervals.push_back(interval);
        ++idx;
      }
    } else {
      std::apply(
          [&](const auto&... label) {
            ((
                [&] {
                  if (this->hasLabel(label)) {
                    auto idx(0);
                    for (const auto& l : domain.labels()) {
                      if (l == label) {
                        break;
                      }
                      ++idx;
                    }
                    Interval interval{label,
                                      domain[idx].interval().inclusive_min(),
                                      domain[idx].interval().exclusive_max()};
                    intervals.push_back(interval);
                  }
                }(),
                ...));
          },
          std::make_tuple(labels...));
    }

    if (intervals.empty()) {
      return absl::InvalidArgumentError(
          "Labels were provided, but none were found for Variable " +
          variableName);
    }
    return intervals;
  }

  // ===========================Member data getters===========================
  /**
   * @brief Gets the name of the variable.
   * @return The name of the variable.
   */
  const std::string& get_variable_name() const { return variableName; }

  /**
   * @brief Gets the long name of the variable.
   * @return The long name of the variable.
   */
  const std::string& get_long_name() const { return longName; }

  /**
   * @brief Gets the underlying tensorstore of the variable.
   * @return The tensorstore of the variable.
   */
  const tensorstore::TensorStore<T, R, M>& get_store() const { return store; }

  tensorstore::TensorStore<T, R, M>& get_mutable_store() { return store; }
  void set_store(tensorstore::TensorStore<T, R, M>& new_store) {
    store = new_store;
  }

  // The data that should remain static, but MAY need to be updated.
  std::shared_ptr<std::shared_ptr<UserAttributes>> attributes;

  /**
   * @brief Gets the original address of the User Attributes.
   * This method should NEVER be called by the user.
   * It allows for the copy constructor without re-serializing metadata.
   * @return The original address of the User Attributes.
   */
  const std::uintptr_t get_attributes_address() const {
    return attributesAddress;
  }

  /**
   * @brief Gets whether the metadata should get republished.
   *
   * @return True if the metadata should get republished.
   */
  bool should_publish() const {
    if (toPublish && *toPublish) {
      // Deref the shared_ptr so we're not increasing refcount
      return **toPublish;
    }
    // If the flag was a nullptr, err on the side of caution and republish
    return true;
  }

 private:
  /**
   * This method should NEVER be called by the user.
   * This method is intended to be called as a callback by the Dataset
   * CommitMetadata method after the updated data is committed to durable media.
   * @brief Updates the current address of the User Attributes.
   */
  void _dataset_only_callback_committed() {
    // We only want to update the address if the UserAttributes object has
    // changed location This indicates a new UserAttributes object has taken the
    // place of the existing one.
    if (attributes.get() != nullptr && attributes->get() != nullptr) {
      std::uintptr_t newAddress =
          reinterpret_cast<std::uintptr_t>(&(**attributes));
      attributesAddress = newAddress;
    }
    // It is fine that this will only change in the "collection" instance of the
    // Variable, because that is the only one that will be operated on by the
    // Dataset commit
  }
  // An identifier for the variable
  std::string variableName;
  // optional, default to name
  std::string longName;
  // other metadata
  ::nlohmann::json metadata;
  // delegate the I/O to the tensorstore
  tensorstore::TensorStore<T, R, M> store;
  // The address of the attributes. This MUST NEVER be touched by the user.
  std::uintptr_t attributesAddress;
  // The metadata will need to be updated if the trim util was used on it.
  std::shared_ptr<std::shared_ptr<bool>> toPublish =
      std::make_shared<std::shared_ptr<bool>>(std::make_shared<bool>(false));
};

// Tensorstore Array's don't have an IndexDomain and so they can't be slice with
// labels e.g. "inline", "crossline".
/**
 * @brief A LabeledArray is an underlying data type for a VariableData object
 * and is not intended to be directly interacted with.
 */
template <typename T, DimensionIndex R, ArrayOriginKind OriginKind>
struct LabeledArray {
  using shared_array = SharedArray<T, R, OriginKind>;

  using const_shared_array = SharedArray<const T, R, OriginKind>;

  LabeledArray(const tensorstore::IndexDomain<R>& dom, const shared_array& arr)
      : domain(dom), data(arr) {}

  /**
   * @brief Slices the tensor along the specified dimensions and returns the
   * resulting sub-tensor.
   * @pre The step of the slice descriptor must be 1.
   * @pre The start and stop of the slice descriptor must be in the domain of
   * the tensor.
   * @tparam alloc The allocation constraint to use.
   * @tparam Descriptors The types of the descriptors used to specify the slice.
   * @param descriptors The descriptors used to specify the slice.
   * @return An `mdio::Result` containing a `const_shared_array` representing
   * the sliced tensor on success, or an error status on failure.
   */
  template <MustAllocateConstraint alloc = MustAllocateConstraint::may_allocate,
            typename... Descriptors>
  Result<const_shared_array> slice(Descriptors&... descriptors) {
    constexpr size_t numDescriptors = sizeof...(descriptors);

    auto tuple_descs = std::make_tuple(descriptors...);

    std::vector<DimensionIdentifier> labels(numDescriptors);
    std::vector<Index> start(numDescriptors), stop(numDescriptors),
        step(numDescriptors);
    std::vector<DimensionIndex> dims(numDescriptors);

    tensorstore::DimensionIndexBuffer buffer;

    absl::Status overall_status = absl::OkStatus();
    std::apply(
        [&](const auto&... desc) {
          size_t idx = 0;
          ((
               labels[idx] = desc.label, start[idx] = desc.start,
               stop[idx] = desc.stop, step[idx] = desc.step,
               // Resolve the label and capture any error
               [&]() {
                 auto result =
                     tensorstore::Dims({desc.label}).Resolve(domain, &buffer);
                 if (!result.ok()) {
                   overall_status = result;  // Capture the error status
                   return;                   // Exit lambda on error
                 }
                 dims[idx] = buffer[0];
               }(),
               idx++),
           ...);
        },
        tuple_descs);

    /// could be we can't slice a dimension
    if (!overall_status.ok()) {
      return overall_status;
    }

    return data |
           tensorstore::Dims(dims).TranslateHalfOpenInterval(start, stop,
                                                             step) |
           tensorstore::Materialize(alloc);
  }

  SharedArray<T, R, OriginKind> get_data() { return data; }

  // the domain, can have labels
  tensorstore::IndexDomain<R> domain;
  // The actual array data in memory, Shared means that a reference counted
  // shared_ptr will be copied to make a new array's.
  // We own the information about the layout, this will be copied.
  SharedArray<T, R, OriginKind> data;
};

/**
 * @brief The in-memory representation of the data
 * This object should only be constructed through the Variable::Read() function.
 * This data only exists in memory and should be preserved with the
 * Variable::Write() function.
 */
template <typename T, DimensionIndex R, ArrayOriginKind OriginKind>
struct VariableData {
  VariableData(const std::string& variableName, const std::string& longName,
               const ::nlohmann::json& metdata,
               const LabeledArray<T, R, OriginKind>& data)
      : variableName(variableName),
        longName(longName),
        metadata(metdata),
        data(data) {}

  // Allows for conversion to compatible types (SourceElement), which should
  // always be possible to void.
  template <typename SourceElement, DimensionIndex SourceRank,
            ArrayOriginKind SourceOriginKind>
  VariableData(
      const VariableData<SourceElement, SourceRank, SourceOriginKind>& other)
      : variableName(other.variableName),
        longName(other.longName),
        metadata(other.metadata),
        data(other.data) {}

  friend std::ostream& operator<<(std::ostream& os, const VariableData& obj) {
    os << obj.variableName << "\t" << obj.data.domain << "\n";
    os << obj.dtype() << "\t" << obj.data.data.rank();
    return os;
  }

  using const_shared_array = SharedArray<const T, R, OriginKind>;

  /**
   * @brief Returns the dimensions of the variable.
   * specifies the origin, shape and labels of the domain.
   * @tparam R The rank of the variable.
   * @return mdio::IndexDomainView<R> The dimensions of the variable.
   */
  IndexDomainView<R> dimensions() const { return data.domain; }

  /**
   * @brief Gets the rank of the VariableData
   * @return std::size_t The rank of the VariableData
   */
  std::size_t rank() const { return data.data.rank(); }

  /**
   * @brief Returns the number of samples in the variable.
   * @return Index The number of samples in the variable.
   */
  Index num_samples() const {
    // Accessing the shape
    auto shape = dimensions().shape();
    // Calculating the total number of elements
    size_t totalElements = 1;
    for (auto dim_size : shape) {
      totalElements *= dim_size;
    }
    return totalElements;
  }

  /**
   * @brief Returns the data type of the variable.
   * The type of the store, T ~ void.
   * @return The data type of the variable.
   */
  DataType dtype() const { return data.data.dtype(); }

  /**
   * @brief Slices the LabeledArray along the given dimensions.
   *
   * @tparam alloc mdio::MustAllocateConstraint::may_allocate by default.
   * @tparam Descriptors Variadic template parameter pack for the descriptors.
   * @param descriptors Variadic parameter pack for the descriptors.
   * @return mdio::Result<LabeledArray<T, R, OriginKind>> The sliced
   * LabeledArray.
   */
  template <MustAllocateConstraint alloc = MustAllocateConstraint::may_allocate,
            typename... Descriptors>
  Result<const_shared_array> slice(Descriptors&... descriptors) {
    return data.slice(descriptors...);
  }

  /**
   * @brief Returns the underlying SharedArray
   * This is the in-memory representation of the data. You should be interacting
   * with this object instead of the data held in Variable.
   * @return mdio::SharedArray<T, R, OriginKind, tensorstore::container> The
   * underlying SharedArray that can be manipulated as desired.
   */
  SharedArray<T, R, OriginKind> get_data_accessor() { return data.get_data(); }

  /**
   * @brief Calculates the offset of the flattened data.
   * This is useful when using `get_data_accessor().data()` because the 0'th
   * element may not be the start of the data and will contain garbage values.
   * @tparam T The type of the data.
   * @return The offset of the flattened data.
   * @code
   * // "my_variable" has dimension "Dimension_0" which has shape [10]
   * mdio::RangeDescriptor<Index> dim_zero_slice = {"Dimension_0", 4, 10, 1};
   * MDIO_ASSIGN_OR_RETURN(auto sliced_dataset, dataset.isel(dim_zero_slice));
   * MDIO_ASSIGN_OR_RETURN(auto sliced_variable,
   *       sliced_dataset.variables.get<mdio::dtypes::float32>("my_variable"));
   * MDIO_ASSIGN_OR_RETURN(auto sliced_data, sliced_variable.Read().result());
   * auto flattened_data =
   * reinterpret_cast<mdio::float32_t*>(sliced_data.get_data_accessor().data());
   * auto offset = sliced_data.get_flattened_offset();
   * // We can use the method `sliced_data.get_data_accessor().num_elements()`
   * // in conjunction with `offset` instead of hardcoding 6.
   * for (size_t i=0; i<6; ++i) {
   *      std::cout << flattened_data[i + offset] << std::endl;
   * }
   * @endcode
   */
  ptrdiff_t get_flattened_offset() {
    auto accessor = get_data_accessor();
    // The raw pointer to the data. May not start at 0.
    auto origin_ptr = accessor.data();
    // The raw pointer to the first element of the data.
    auto offset_ptr = accessor.byte_strided_origin_pointer();
    char* origin_addr = reinterpret_cast<char*>(origin_ptr);
    // We have to get the raw pointer
    char* offset_addr = reinterpret_cast<char*>(offset_ptr.get());
    ptrdiff_t byte_diff = offset_addr - origin_addr;
    return byte_diff / dtype().size();
  }

  // An identifier for the variable.
  std::string variableName;
  // optional, default to name
  std::string longName;
  // other metadata
  ::nlohmann::json metadata;
  // the data
  LabeledArray<T, R, OriginKind> data;
};

/**
 * @brief Allocates a VariableData object with the specified dtype and fill
 * value. Intended behavior is to fill the array with default fill values and
 * may overwrite existing data if written to disk.
 * @tparam T The data type of the variable.
 * @tparam R The rank of the variable.
 * @tparam OriginKind The origin kind of the variable.
 * @param variable The variable to allocate from.
 * @return mdio::Result<VariableData<T, R, OriginKind>> The allocated
 * VariableData object.
 */
template <typename T = void, DimensionIndex R = dynamic_rank,
          ArrayOriginKind OriginKind = offset_origin>
Result<VariableData<T, R, OriginKind>> from_variable(
    const Variable<>& variable) {
  auto domain = variable.get_store().domain();

  // There are two steps here, first create a variable with the compile time
  // data type as "void", and the internal data type of the array according to
  // the variable
  auto _array = tensorstore::AllocateArray(
      variable.get_store().domain().box(), mdio::ContiguousLayoutOrder::c,
      tensorstore::value_init, variable.dtype());

  if (variable.dtype() == constants::kFloat32 ||
      variable.dtype() == constants::kFloat64) {
    // Get the size of the array in bytes
    size_t num_elements = variable.num_samples();
    size_t element_size = variable.dtype().size();

    if (variable.dtype() == constants::kFloat32) {
      auto* data = reinterpret_cast<float*>(_array.data());
      std::fill_n(data, num_elements, std::numeric_limits<float>::quiet_NaN());
    } else {  // double
      auto* data = reinterpret_cast<double*>(_array.data());
      std::fill_n(data, num_elements, std::numeric_limits<double>::quiet_NaN());
    }
  }

  // The second step tries to cast the dtype of the array to the supplied
  // templated. this can fail if the types are inconsistent, at which point it
  // will return with a status.
  MDIO_ASSIGN_OR_RETURN(auto array,
                        tensorstore::StaticDataTypeCast<T>(std::move(_array)))

  auto labeled_array = LabeledArray<T, R, OriginKind>{domain, std::move(array)};

  return VariableData<T, R, OriginKind>{
      variable.get_variable_name(), variable.get_long_name(),
      variable.getReducedMetadata(), std::move(labeled_array)};
}
};  // namespace mdio
#endif  // MDIO_VARIABLE_H_
