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

#ifndef MDIO_UTILS_TRIM_H_
#define MDIO_UTILS_TRIM_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "mdio/dataset.h"
#include "tensorstore/kvstore/kvstore.h"

namespace mdio {
namespace utils {

/**
 * @brief Trims the dataset to the specified dimensions.
 * DANGER: This operation will mutate the dataset on disk. Use caution when
 * calling this method! This function is not part of the Dataset class to avoid
 * accidental data destruction. Additionally this function should only be used
 * on a fully written dataset to avoid race conditions and data corruption.
 *
 * @tparam ...Descriptors Expects an mdio::RangeDescriptor<mdio::Index>
 * @param dataset_path The path to the dataset to trim.
 * @param delete_sliced_out_chunks If true, chunks that fall completely outside
 * the slice descriptors will be deleted. If false, chunks that fall completely
 * outside the slice descriptors will remain untouched but inaccessable.
 * @param descriptors The descriptors to use for the slice. Only considers the
 * label and stop value.
 * @return A future of the trim operation.
 */
template <typename... Descriptors>
Future<void> TrimDataset(std::string dataset_path,
                         bool delete_sliced_out_chunks,
                         const Descriptors&... descriptors) {
  // Open the dataset
  auto dsRes = mdio::Dataset::Open(dataset_path, mdio::constants::kOpen);
  if (!dsRes.status().ok()) {
    return dsRes.status();
  }
  mdio::Dataset ds = dsRes.value();
  // Trim the dataset
  std::unordered_map<std::string_view, mdio::Index> shapeDescriptors;
  std::vector<mdio::RangeDescriptor<mdio::Index>> descriptorList = {
      descriptors...};
  if (descriptorList.size() == 0) {
    // No slices = no op
    return absl::OkStatus();
  }
  for (const auto& descriptor : descriptorList) {
    shapeDescriptors[descriptor.label.label()] = descriptor.stop;
  }

  for (auto& varIdentifier : ds.variables.get_iterable_accessor()) {
    MDIO_ASSIGN_OR_RETURN(auto var, ds.variables.at(varIdentifier))
    var.set_metadata_publish_flag(true);

    if (var.dimensions().labels().back() == "") {
      auto spec = var.spec();
      if (!spec.status().ok()) {
        // Something went wrong with Tensorstore retrieving the spec
        return spec.status();
      }
      auto specJsonResult = spec.value().ToJson(IncludeDefaults{});
      if (!specJsonResult.status().ok()) {
        return specJsonResult.status();
      }
      // This will fall over if the first dtype is itself structured data
      nlohmann::json specJson =
          specJsonResult.value()["metadata"]["dtype"][0][0];
      std::string field = specJson.get<std::string>();
      // If the variable is structured data we will pick the first dimension
      // arbitrarially
      auto selection = ds.SelectField(varIdentifier, field);
      if (!selection.status().ok()) {
        return selection.status();
      }
      MDIO_ASSIGN_OR_RETURN(var, ds.variables.at(varIdentifier))
    }

    auto varStore = var.get_store();
    std::vector<tensorstore::Index> implicitDims;
    std::vector<tensorstore::Index> newShape;

    for (size_t i = 0; i < var.dimensions().shape().size(); i++) {
      implicitDims.push_back(tensorstore::kImplicit);
      if (shapeDescriptors.count(var.dimensions().labels()[i]) > 0) {
        newShape.push_back(shapeDescriptors[var.dimensions().labels()[i]]);
      } else {
        newShape.push_back(var.dimensions().shape()[i]);
      }
    }

    tensorstore::ResizeOptions resizeOptions;
    if (delete_sliced_out_chunks) {
      resizeOptions.mode = tensorstore::ResizeMode::resize_tied_bounds;
    } else {
      resizeOptions.mode = tensorstore::ResizeMode::resize_metadata_only;
    }

    auto resizedStatus = tensorstore::Resize(
        varStore, tensorstore::span<const tensorstore::Index>(implicitDims),
        tensorstore::span<const tensorstore::Index>(newShape), resizeOptions);

    if (!resizedStatus.status().ok()) {
      return resizedStatus.status();
    }
  }

  return ds.CommitMetadata();
}

}  // namespace utils
}  // namespace mdio

#endif  // MDIO_UTILS_TRIM_H_
