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

#ifndef MDIO_UTILS_DELETE_H_
#define MDIO_UTILS_DELETE_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "mdio/dataset.h"
#include "tensorstore/kvstore/kvstore.h"

namespace mdio {
namespace utils {

/**
 * @brief A utility to delete an MDIO dataset
 * It will first be checked that the dataset is a valid MDIO dataset before
 * deletion. This is intended to provide a safe interface to delete MDIO
 * datasets.
 * @param dataset_path The path to the dataset
 * @return OK result if the dataset was valid and deleted successfully,
 * otherwise an error result
 */
Result<void> DeleteDataset(const std::string dataset_path) {
  // Open the dataset
  // This is to ensure that what is getting deleted by MDIO is a valid MDIO
  // dataset itself.
  auto dsRes = mdio::Dataset::Open(dataset_path, mdio::constants::kOpen);
  if (!dsRes.status().ok()) {
    return dsRes.status();
  }
  auto ds = dsRes.value();

  // Pick the arbitrarially first Variable in the dataset as the base KVStore
  // template
  MDIO_ASSIGN_OR_RETURN(auto var,
                        ds.variables.at(ds.variables.get_keys().front()))
  MDIO_ASSIGN_OR_RETURN(auto spec, var.get_spec())
  nlohmann::json kvs = spec["kvstore"];

  // Drop the Variable path from the KVStore path. This is will leave us with
  // the full Dataset
  std::size_t pos =
      kvs["path"].get<std::string>().rfind(var.get_variable_name());
  std::string path = kvs["path"].get<std::string>().substr(0, pos - 1);
  if (path.back() == '/') {
    // Handle case where the variable is double slashed
    path.pop_back();
  }
  kvs["path"] = path;

  auto kvsFuture = tensorstore::kvstore::Open(kvs);
  if (!kvsFuture.status().ok()) {
    return kvsFuture.status();
  }
  auto kvstore = kvsFuture.value();

  auto deleteRes = tensorstore::kvstore::DeleteRange(kvstore, {});
  if (!deleteRes.status().ok()) {
    return deleteRes.status();
  }

  return absl::OkStatus();
}

}  // namespace utils
}  // namespace mdio

#endif  // MDIO_UTILS_DELETE_H_
