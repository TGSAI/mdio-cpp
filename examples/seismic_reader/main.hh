// Copyright 2025 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "mdio/mdio.h"
#include "utm.hh"
#include "stats.hh"
#include <cmath>
#include <tuple>

/**
 * @brief Opens an existing MDIO dataset.
 * @param path The path to the dataset. May be POSIX, S3, or GCS.
 * @param cache_size_bytes The size of the LRU cache to be used by the dataset. Defaults to 1GiB.
 * @return An async future that resolves to the dataset or an error.
 */
mdio::Future<mdio::Dataset> OpenDataset(const std::string& path, uint64_t cache_size_bytes = 1024ULL * 1024ULL * 1024ULL);

/**
 * @brief Retrieves the UTM coordinate futures from the dataset.
 * @param ds The seismic dataset.
 * @return A pair of futures, the CDP-X and CDP-Y coordinates.
 */
mdio::Result<std::pair<mdio::Future<mdio::VariableData<mdio::dtypes::float64_t>>, mdio::Future<mdio::VariableData<mdio::dtypes::float64_t>>>> GetUTMCoords(mdio::Dataset& ds);

/**
 * @brief Retrieves the inline and crossline futures from the dataset.
 * @param ds The seismic dataset.
 * @return A pair of futures, the inline and crossline coordinates.
 */
mdio::Result<std::pair<mdio::Future<mdio::VariableData<mdio::dtypes::uint16_t>>, mdio::Future<mdio::VariableData<mdio::dtypes::uint16_t>>>> GetInlineCrossline(mdio::Dataset& ds);

/**
 * @brief An example of how to get extents of a VariableData object.
 * This is a simple function that assumes the passed VariableData object holds some coordinate
 * data which may not be sorted. Appropriate uses may be checking dimension coordinates such as inline, crossline, time, etc.
 * or checking UTM coordinates.
 * 
 * This function does not take any possible liveness into account.
 * 
 * @tparam T The type of the coordinate data.
 * @param var The VariableData object to get the extents of.
 * @return A tuple containing the minimum and maximum values of the coordinate data.
 */
template <typename T>
std::tuple<T, T> GetExtents(mdio::VariableData<T>& var) {
    auto dataPtr = var.get_data_accessor().data();
    auto offset = var.get_flattened_offset();
    auto num_samples = var.num_samples();
    
    auto result = std::minmax_element(dataPtr + offset, dataPtr + offset + num_samples);
    return std::make_tuple(*(result.first), *(result.second));
}

// Fold expression operator<< for tuples.
// Allows for easy printing of tuples.
template<class... Args>
std::ostream& operator<<(std::ostream& os, const std::tuple<Args...>& t) {
    os << "(";
    std::apply([&os](const auto&... args) {
        std::size_t n{0};
        ((os << (n++ ? ", " : "") << args), ...);
    }, t);
    os << ")";
    return os;
}

