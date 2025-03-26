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

#include "main.hh"

int main() {
    const std::string path = "s3://tgs-opendata-poseidon/full_stack_agc.mdio";
    const uint64_t cache_size_bytes = 1024ULL * 1024ULL * 1024ULL * 5ULL;  // Use a 5GiB cache.
    mdio::Future<mdio::Dataset> dsFut = OpenDataset(path, cache_size_bytes);

    if (!dsFut.status().ok()) {
        std::cerr << "Failed to open dataset: " << dsFut.status().message() << std::endl;
        return 1;
    }

    mdio::Dataset ds = dsFut.value();
    std::cout << "Dataset opened successfully" << std::endl;
    std::cout << ds << std::endl;

    auto cdpsFut = GetUTMCoords(ds);
    if (!cdpsFut.status().ok()) {
        std::cerr << "Failed to get UTM coordinates: " << cdpsFut.status().message() << std::endl;
        return 1;
    }

    auto cdps = cdpsFut.value();
    auto cdp_x = cdps.first.value();
    auto cdp_y = cdps.second.value();

    auto cdp_x_extents = GetExtents<mdio::dtypes::float64_t>(cdp_x);
    auto cdp_y_extents = GetExtents<mdio::dtypes::float64_t>(cdp_y);

    std::cout << "========CDP Coordinates=========" << std::endl;
    std::cout << "CDP X extents: " << cdp_x_extents << std::endl;
    std::cout << "CDP Y extents: " << cdp_y_extents << std::endl;
    utm::print_corners(cdp_x_extents, cdp_y_extents);
    std::cout << std::endl;
    // This displays the maximum area of the extents on a web map.
    // The surveys actual polygon is not computed in this example.
    utm::web_display(cdp_x_extents, cdp_y_extents);
    std::cout << std::endl;
    std::cout << "=================================" << std::endl << std::endl;

    auto linesFut = GetInlineCrossline(ds);
    if (!linesFut.status().ok()) {
        std::cerr << "Failed to get inline and crossline coordinates: " << linesFut.status().message() << std::endl;
        return 1;
    }

    auto lines = linesFut.value();
    auto il = lines.first.value();
    auto xl = lines.second.value();

    auto il_extents = GetExtents<mdio::dtypes::uint16_t>(il);
    auto xl_extents = GetExtents<mdio::dtypes::uint16_t>(xl);

    std::cout << "Inline extents: " << il_extents << std::endl;
    std::cout << "Crossline extents: " << xl_extents << std::endl;

    auto statsResult = stats::CalculateVolumeStatistics<mdio::dtypes::float32_t, mdio::dtypes::uint16_t>(ds);
    if (!statsResult.status().ok()) {
        std::cerr << "Failed to get volume statistics: " << statsResult.status().message() << std::endl;
        return 1;
    }

    auto stats = statsResult.value();
    std::cout << stats << std::endl;

    // Now that we have the statistics, lets pinpoint where our peak and trough amplitudes are located on the world.
    mdio::ValueDescriptor<mdio::dtypes::uint16_t> il_peak = {"inline", stats.peak_amplitude_inline};
    mdio::ValueDescriptor<mdio::dtypes::uint16_t> xl_peak = {"crossline", stats.peak_amplitude_crossline};
    mdio::ValueDescriptor<mdio::dtypes::uint16_t> il_trough = {"inline", stats.trough_amplitude_inline};
    mdio::ValueDescriptor<mdio::dtypes::uint16_t> xl_trough = {"crossline", stats.trough_amplitude_crossline};

    auto peakSlicedDatasetRes = ds.sel(il_peak, xl_peak);
    if (!peakSlicedDatasetRes.status().ok()) {
        std::cerr << "Failed to slice dataset: " << peakSlicedDatasetRes.status().message() << std::endl;
        return 1;
    }

    auto peakSlicedDataset = peakSlicedDatasetRes.value();
    // We can print the dataset and see that the inline and crossline indices are not the same as their values.
    // std::cout << "Peak sliced dataset: " << peakSlicedDataset << std::endl;

    // We can re-use the same function (and variable) to get the cdp coordinates of the peak amplitude now.
    cdpsFut = GetUTMCoords(peakSlicedDataset);
    if (!cdpsFut.status().ok()) {
        std::cerr << "Failed to get UTM coordinates of peak amplitude: " << cdpsFut.status().message() << std::endl;
        return 1;
    }

    cdps = cdpsFut.value();
    cdp_x = cdps.first.value();
    cdp_y = cdps.second.value();
    cdp_x_extents = GetExtents<mdio::dtypes::float64_t>(cdp_x);
    cdp_y_extents = GetExtents<mdio::dtypes::float64_t>(cdp_y);

    std::cout << "========Peak Amplitude=========" << std::endl;
    utm::print_corners(cdp_x_extents, cdp_y_extents);
    utm::web_display(cdp_x_extents, cdp_y_extents);
    std::cout << std::endl;
    std::cout << "=================================" << std::endl << std::endl;

    auto troughSlicedDatasetRes = ds.sel(il_trough, xl_trough);
    if (!troughSlicedDatasetRes.status().ok()) {
        std::cerr << "Failed to slice dataset: " << troughSlicedDatasetRes.status().message() << std::endl;
        return 1;
    }

    auto troughSlicedDataset = troughSlicedDatasetRes.value();
    // The trough sliced dataset should show a different inline/crossline index pair than the peak sliced dataset.
    // std::cout << "Trough sliced dataset: " << troughSlicedDataset << std::endl;

    cdpsFut = GetUTMCoords(troughSlicedDataset);
    if (!cdpsFut.status().ok()) {
        std::cerr << "Failed to get UTM coordinates of trough amplitude: " << cdpsFut.status().message() << std::endl;
        return 1;
    }

    cdps = cdpsFut.value();
    cdp_x = cdps.first.value();
    cdp_y = cdps.second.value();
    cdp_x_extents = GetExtents<mdio::dtypes::float64_t>(cdp_x);
    cdp_y_extents = GetExtents<mdio::dtypes::float64_t>(cdp_y);

    std::cout << "========Trough Amplitude=========" << std::endl;
    utm::print_corners(cdp_x_extents, cdp_y_extents);
    std::cout << std::endl;
    utm::web_display(cdp_x_extents, cdp_y_extents);
    std::cout << std::endl;
    std::cout << "=================================" << std::endl << std::endl;
    return 0;
}


mdio::Future<mdio::Dataset> OpenDataset(const std::string& path, uint64_t cache_size_bytes) {
    auto cacheJson = nlohmann::json::parse(R"({"cache_pool": {"total_bytes_limit": 1073741824}})");  // 1GiB default cache size.
    cacheJson["cache_pool"]["total_bytes_limit"] = cache_size_bytes;
    auto spec = mdio::Context::Spec::FromJson(cacheJson);
    auto ctx = mdio::Context(spec.value());
    return mdio::Dataset::Open(path, mdio::constants::kOpen, ctx);
}

mdio::Result<std::pair<mdio::Future<mdio::VariableData<mdio::dtypes::float64_t>>, mdio::Future<mdio::VariableData<mdio::dtypes::float64_t>>>> GetUTMCoords(mdio::Dataset& ds) {
    MDIO_ASSIGN_OR_RETURN(auto cdp_x, ds.variables.get<mdio::dtypes::float64_t>("cdp-x"));
    MDIO_ASSIGN_OR_RETURN(auto cdp_y, ds.variables.get<mdio::dtypes::float64_t>("cdp-y"));

    auto cdp_x_fut = cdp_x.Read();
    auto cdp_y_fut = cdp_y.Read();

    return std::make_pair(cdp_x_fut, cdp_y_fut);
}

mdio::Result<std::pair<mdio::Future<mdio::VariableData<mdio::dtypes::uint16_t>>, mdio::Future<mdio::VariableData<mdio::dtypes::uint16_t>>>> GetInlineCrossline(mdio::Dataset& ds) {
    MDIO_ASSIGN_OR_RETURN(auto il, ds.variables.get<mdio::dtypes::uint16_t>("inline"));
    MDIO_ASSIGN_OR_RETURN(auto xl, ds.variables.get<mdio::dtypes::uint16_t>("crossline"));

    auto il_fut = il.Read();
    auto xl_fut = xl.Read();

    return std::make_pair(il_fut, xl_fut);
}
