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

#ifndef MDIO_VIEW_H_
#define MDIO_VIEW_H_

#include <vector>
#include <map>

#include "mdio/impl.h"
#include "mdio/variable.h"
#include "mdio/dataset.h"
#include "tensorstore/array.h"
#include "tensorstore/tensorstore.h"
#include "tensorstore/util/future.h"

namespace mdio {

namespace internal {

/**
 * @brief The sort order of a coordinate.
 * kFlatHomogeneous: When all the coordinate's values are the same.
 * kFlatForward: When flattened the coordinate's values are in ascending order. (LHS <= RHS)
 * kFlatReverse: When flattened the coordinate's values are in descending order. (LHS >= RHS)
 * kChunkHomogeneous: When all the coordinate's values within a chunk are the same.
 * kChunkForward: When reading the fastest dimension the coordinate's values are in ascending order within each chunk. (LHS <= RHS)
 * kChunkReverse: When reading the fastest dimension the coordinate's values are in descending order within each chunk. (LHS >= RHS)
 *    In the event that kChunkForward and kChunkReverse are both true, kChunkForward is preferred.
 * kUnknown: The sort order of the coordinate is unknown.
 * kUnsorted: The coordinate is unsorted or could not be determined.
 * 
 * Due to the nature of the coordinates, it is possible to have multiple equal values.
 * For kFlat arrays we must find every instance to the left and right of the current value.
 * For kChunked arrays we must find every instance within every chunk.
 * For unknown/unsorted arrays we must find every instance anywhere.
*/
enum class sort_order {
  kFlatHomogeneous,
  kFlatForward,
  kFlatReverse,
  kChunkHomogeneous,
  kChunkForward,
  kChunkReverse,
  kUnknown,
  kUnsorted
};

}  // namespace internal

class CoordinateView {
  public:
  /**
   * @brief Constructs a CoordinateView from a JSON specification.
  */
  Result<CoordinateView> FromJson(const nlohmann::json& json) {
    return absl::UnimplementedError("Not yet implemented.");
  }

  /**
   * @brief Gets the data accessor for the CoordinateView.
  */
  SharedArray<void, dynamic_rank, ArrayOriginKind> get_data_accessor() {
    return data;
  }

  /**
   * @brief Gets the flattened offset of the CoordinateView.
  */
  ptrdiff_t get_flattened_offset() const {
    return 0;
  }

  /**
   * @brief Gets the data type of the CoordinateView.
  */
  DataType dtype() const {
    return data.dtype();
  }

  /**
   * @brief Gets the dimensions of the CoordinateView.
  */
  IndexDomainView<dynamic_rank> dimensions() const {
    return data.domain();
  }

  private:
  tensorstore::TensorStore<void, dynamic_rank, tensorstore::ReadWriteMode::dynamic> data;
};

/**
 * @brief The open mode for a View.
 * kNoIndex: The View will never be indexed.
 * This is intended to save memory but will require a full search of the coordinates for every new View.
 *  Fastest open.
 * kLazy: The View will be constructed lazily and minimal data will be read.
 *  Fast open. Slow first access, but faster subsequent access.
 * kEager: The View will be constructed eagerly and all coordinates will be read.
 *  Slow open. Faster first access, and fastest potential subsequent access.
 * kEagerWithSort: `kEager` and indexed for optimal access.
 *  Slowest open. Fastest potential access.
*/
enum class OpenMode {
  kNoIndex,
  kLazy,
  kEager,
  kEagerWithSort
};

/**
 * @brief A size-agnostic coordinate mapping to the data Variable
*/
class Extentanator {
  std::vector<RangeDescriptor<>> indexableExtents;  // The total extents of the data Variable 
  std::vector<std::vector<std::any>> coordinates;  // The coordinate values where each element is found
  std::vector<std::vector<std::any>> values;  // The data values where each element is found

  /**
   * @brief Merges multiple extents into a single Extentanator.
   * @param extents The extents to merge
   * @return The merged Extentanator if successful, or an error if the merge failed.
  */
  static Result<Extentanator> MergeExtents(const std::vector<Extentanator>& extents) {
    return absl::UnimplementedError("Not yet implemented.");
  }
};

/**
 * @brief A virtual view of an MDIO dataset from a coordinate perspective.
 * This class is intended to be used to view a (subset of a) dataset from it's coordinate perspective instead of the dimension persepctive.
 * This class will provide an in-memory view of the Dataset.
 * This class is not intended to provide a read/write interface to the Dataset as the original dimensions are discarded.
 * This class will only operate based on coordinates specified in the Dataset and will not allow for dimension coordinates.
 * For dimension coordinate based views, use the `sel` method on the Dataset.
 */
class DatasetView {

  /**
   * @brief Constructs a DatasetView from an MDIO Dataset.
   * @param dataset The Dataset to be viewed
   * @param mode How to handle the coordinates
   * @return The constructed DatasetView if successful, or an error if the DatasetView failed.
   */
  static Result<DatasetView> FromDataset(const Dataset& dataset, const OpenMode mode) {
    if (dataset.coordinates.empty()) {
      return absl::InvalidArgumentError("Dataset must have coordinates to make a DatasetView.");
    }

    // Set up the DatasetView variables
    std::map<std::string, Variable<>> coordinates;
    std::map<std::string, Variable<>> dataVars;
    std::map<std::string, internal::sort_order> sortOrders;
    std::map<std::string, VariableData<>> coordinatesData;

    // Get all the coordinates and data variables
    for (const auto& [key, value] : dataset.coordinates) {
      MDIO_ASSIGN_OR_RETURN(auto dataVar, dataset.variables.get<>(key));
      dataVars[key] = dataVar;
      auto coordSize = value.size();
      for (size_t i=0; i<coordSize; ++i) {
        MDIO_ASSIGN_OR_RETURN(auto coordVar, dataset.variables.get<>(value[i]));
      }
    }

    switch (mode) {
      case OpenMode::kEagerWithSort:  // fallthrough
      case OpenMode::kEager:          // fallthrough
      case OpenMode::kLazy:           // fallthrough
      case OpenMode::kNoIndex:        // fallthrough
        // std::cout << "Mode: " << mode << std::endl;
        break;
    }

    return DatasetView(coordinates, dataVars, sortOrders, mode, coordinatesData);
  }

  /**
   * @brief Constructs a useful CoordinateView based on a Variable label.
   * @param label The label of the Variable to view
   * @return The constructed CoordinateView if successful, or an error if the CoordinateView failed.
  */
  template <typename... Descriptors>
  Result<CoordinateView> MakeView(const std::string& label, Descriptors... descriptors) {
    bool willBinSearch = true;
    for (const auto& [key, value] : sortOrders) {
      switch (value) {
        case internal::sort_order::kUnknown:   // fallthrough
        case internal::sort_order::kUnsorted:  // fallthrough
          willBinSearch = false;
          break;
        case internal::sort_order::kFlatHomogeneous:   // fallthrough
        case internal::sort_order::kFlatForward:       // fallthrough
        case internal::sort_order::kFlatReverse:       // fallthrough
        case internal::sort_order::kChunkHomogeneous:  // fallthrough
        case internal::sort_order::kChunkForward:      // fallthrough
        case internal::sort_order::kChunkReverse:      // fallthrough
          willBinSearch = false;  // TODO(BrianMichell): Remove this line
          break;
      }
      if (!willBinSearch) break;
    }

    Extentanator extentanator;
    // Result<Extentanator> searchRes;
    if (willBinSearch) {
      MDIO_ASSIGN_OR_RETURN(extentanator, binary_search(label, descriptors...));
      // searchRes = binary_search(label, descriptors...);
    } else {
      MDIO_ASSIGN_OR_RETURN(extentanator, linear_search(label, descriptors...));
      // searchRes = linear_search(label, descriptors...);
    }

    MDIO_ASSIGN_OR_RETURN(auto json, makeJson(extentanator));
    return CoordinateView::FromJson(json);
  }

  /**
   * @brief Attempts to perform a linear search on a single chunk of the Variable.
   * This method is the atomic linear search method.
   * It has been made publically accessible for custom orchestrations.
   * It is recommended to use the MakeView method for default orchestrations.
   * It is recommended to use this method with a `mdio::OpenMode::kNoIndex` constructed object for optimal performance.
   * @param var The data Variable to have a new view created from
   * @param chunkID The ID of the chunk to search
   * @param descriptors The descriptors of the coordinates to search
  */
  // template <typename... Descriptors, typename CoordType, typename DataType>
  // Result<Extentanator<CoordType, DataType>> LinearSearch(const Variable& var, const Index chunkID, Descriptors... descriptors) {
  template <typename... Descriptors>
  Result<Extentanator> LinearSearch(const Variable<>& var, const Index chunkID, Descriptors... descriptors) {
    return absl::UnimplementedError("Not yet implemented.");
  }

  /**
   * @brief Attempts to perform a binary search on a single chunk of the Variable.
   * This method is the atomic binary search method.
   * It has been made publically accessible for custom orchestrations.
   * It is recommended to use the MakeView method for default orchestrations.
   * It is recommended to use this method with a `mdio::OpenMode::kNoIndex` constructed object for optimal performance.
   * @param var The data Variable to have a new view created from
   * @param chunkID The ID of the chunk to search
   * @param descriptors The descriptors of the coordinates to search
  */
  // template <typename... Descriptors, typename CoordType, typename DataType>
  // Result<Extentanator<CoordType, DataType>> BinarySearch(const Variable& var, const Index chunkID, Descriptors... descriptors) {
  template <typename... Descriptors>
  Result<Extentanator> BinarySearch(const Variable<>& var, const Index chunkID, Descriptors... descriptors) {
    return absl::UnimplementedError("Not yet implemented.");
  }

  
  /**
   * @brief Creates a JSON representation of the CoordinateView.
  */
  Result<nlohmann::json> makeJson(const Extentanator& extentanator) {
    return absl::UnimplementedError("Not yet implemented.");
  }

  private:
  DatasetView(std::map<std::string, Variable<>> coordinates, std::map<std::string, Variable<>> dataVars, std::map<std::string, internal::sort_order> sortOrders, OpenMode mode, std::map<std::string, VariableData<>> coordinatesData) : coordinates(coordinates), dataVars(dataVars), sortOrders(sortOrders), mode(mode), coordinatesData(coordinatesData) {}

  /**
   * @brief Constructor helper method for setting the sort order of the coordinates.
   * @return OK status if sort order was set successfully, or an error if something went wrong.
  */
  Result<void> set_sort_order() {
    for (const auto& [key, value] : coordinates) {
      switch (this->mode) {
        case OpenMode::kEagerWithSort:  // fallthrough
        case OpenMode::kEager:          // fallthrough
        case OpenMode::kLazy:
          this->sortOrders[key] = internal::sort_order::kUnknown;
          // break;  // TODO(BrianMichell): Uncomment this line to disable fallthrough
        case OpenMode::kNoIndex:
          this->sortOrders[key] = internal::sort_order::kUnsorted;
          break;
      }
    }
    return absl::OkStatus();
  }
  
  /**
   * @brief Performs a linear search for the given descriptors.
  */
  template <typename... Descriptors>
  Result<Extentanator> linear_search(const Variable<>& var, Descriptors... descriptors) {
    // Calculate chunkID
    // Call LinearSearch with chunkID
    // Append to Extentanator
    // Return merged extentanator
    std::vector<Extentanator> extentanators;

    return absl::UnimplementedError("Not yet implemented.");
  }

  /**
   * @brief Performs a binary search for the given descriptors.
  */
  template<typename... Descriptors>
  Result<Extentanator> binary_search(const Variable<>& var, Descriptors... descriptors) {
    return absl::UnimplementedError("Not yet implemented.");
  }

  /**
   * @brief Validates that the Variable's coordinates are compatible with the DatasetView.
   * They must have the same dimensions of the Variable.
   * Coordinates must match each-other's type.
  */
  Result<void> validateCompatibleCoords(const Variable<>& var) {
    std::vector<std::string> coords_vec = var.get_coordinate_labels();
    if (coords_vec.empty()) {
      return absl::InvalidArgumentError(absl::StrCat("Variable ", var.get_variable_name(), " must have coordinates to be used in a DatasetView."));
    }

    auto dtypeRes = isSameDtype(var, coords_vec);
    if (!dtypeRes.status().ok()) {
      return dtypeRes.status();
    }

    auto dimRes = isSameDimensions(var, coords_vec);
    if (!dimRes.status().ok()) {
      return dimRes.status();
    }

    return absl::OkStatus();
  }

  /**
   * @brief Validates that the Variable's coordinates are all of the same type
  */
  Result<void> isSameDtype(const Variable<>& var, const std::vector<std::string>& coords_vec) {
    DataType dtype;
    bool flag = false;
    for (auto coord : coords_vec) {
      if (!coordinates.contains(coord)) {
        return absl::InvalidArgumentError(absl::StrCat("Variable ", var.get_variable_name(), " was expected to have coordinate ", coord, " but it was not found!"));
      }
      if (!flag) {
        dtype = coordinates[coord].dtype();
      } else if (dtype != coordinates[coord].dtype()) {
        return absl::InvalidArgumentError("Coordinate type mismatch detected. Differing types of coordinates are not yet supported");
      }
    }
    return absl::OkStatus();
  }

  /**
   * @brief Validates that the Variable's coordinates dimensions match its dimensions
   * Coordinates are expected to be <= ND-1.
  */
  Result<void> isSameDimensions(const Variable<>& var, const std::vector<std::string>& coords_vec) {
    MDIO_ASSIGN_OR_RETURN(auto dataIntervals, var.get_intervals());
    std::map<DimensionIdentifier, Variable<>::Interval> intervalMap;
    for (auto& interval : dataIntervals) {
      intervalMap[interval.label] = interval;
    }

    // Get all of the coordinate intervals
    for (const auto& coord : coords_vec) {
      if (!coordinates.contains(coord)) {
        return absl::InvalidArgumentError(absl::StrCat("Variable ", var.get_variable_name(), " was expected to have coordinate ", coord, " but it was not found!"));
      }
      MDIO_ASSIGN_OR_RETURN(auto coordIntervals, coordinates[coord].get_intervals());
      for (const auto& interval : coordIntervals) {
        if (intervalMap.count(interval.label) == 0) {
          return absl::InvalidArgumentError(absl::StrCat("Variable ", var.get_variable_name(), " was expected to have coordinate ", coord, " but it was not found!"));
        }
        if (intervalMap[interval.label].inclusive_min != interval.inclusive_min) {
          return absl::InvalidArgumentError("Mismatch between data Variable and coordinate Variable intervals detected.");
        }
        if (intervalMap[interval.label].exclusive_max != interval.exclusive_max) {
          return absl::InvalidArgumentError("Mismatch between data Variable and coordinate Variable intervals detected.");
        }
      }
    }

    return absl::OkStatus();
  }

  std::map<std::string, Variable<>> coordinates;
  std::map<std::string, Variable<>> dataVars;
  std::map<std::string, internal::sort_order> sortOrders;
  OpenMode mode;

  std::optional<std::map<std::string, VariableData<>>> coordinateData;

};

}  // namespace mdio
#endif  // MDIO_VIEW_H_
