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

namespace stats {

// Forward declaration
template <typename CoordinateType>
mdio::Result<std::tuple<CoordinateType, CoordinateType>> GetCoordinatePair(mdio::Future<mdio::VariableData<CoordinateType>>& inlineFuture, mdio::Future<mdio::VariableData<CoordinateType>>& crosslineFuture, const mdio::Index inline_idx, const mdio::Index crossline_idx);

/**
 * @brief A struct containing the statistics of a seismic volume.
 * @tparam TraceType The type of the trace data.
 * @tparam CoordinateType The type of the coordinate data.
 */
template <typename TraceType, typename CoordinateType>
struct VolumeStats {
    /// The average amplitude of the seismic volume.
    TraceType avg_amplitude;
    /// The maximum amplitude of the seismic volume.
    TraceType max_amplitude;
    /// The minimum amplitude of the seismic volume.
    TraceType min_amplitude;
    /// The inline index of the peak amplitude.
    CoordinateType peak_amplitude_inline;
    /// The crossline index of the peak amplitude.
    CoordinateType peak_amplitude_crossline;
    /// The inline index of the trough amplitude.
    CoordinateType trough_amplitude_inline;
    /// The crossline index of the trough amplitude.
    CoordinateType trough_amplitude_crossline;
    /// The number of samples in the seismic volume.
    uint64_t num_samples;

    /**
     * @brief A friend function to print the statistics of the seismic volume.
     * @param os The output stream to print the statistics to.
     * @param stats The statistics to print.
     * @return The output stream.
     */
    friend std::ostream& operator<<(std::ostream& os, const VolumeStats& stats) {
        os << "Average amplitude: " << stats.avg_amplitude << std::endl;
        os << "Max amplitude: " << stats.max_amplitude << std::endl;
        os << "Min amplitude: " << stats.min_amplitude << std::endl;
        os << "Peak amplitude inline: " << stats.peak_amplitude_inline << std::endl;
        os << "Peak amplitude crossline: " << stats.peak_amplitude_crossline << std::endl;
        os << "Trough amplitude inline: " << stats.trough_amplitude_inline << std::endl;
        os << "Trough amplitude crossline: " << stats.trough_amplitude_crossline << std::endl;
        os << "Number of samples: " << stats.num_samples << std::endl;
        return os;
    }
};

/**
 * @brief Calculate the statistics of a (subset) seismic volume tracewise.
 * 
 * This function demonstrates an efficient way to access our 3D seismic data in a trace-wise manner.
 * 
 * @tparam TraceType The type of the trace data.
 * @tparam CoordinateType The type of the coordinate data.
 * @param ds The dataset to calculate the statistics of.
 * @return A VolumeStats object containing the statistics.
 */
template <typename TraceType, typename CoordinateType>
mdio::Result<VolumeStats<TraceType, CoordinateType>> CalculateVolumeStatistics(mdio::Dataset& ds) {

    // We could get the chunk sizes from the Variable objects, but for simplicity, we will just use these constants.
    // See the Variable::get_chunk_shape() method for more information.
    const mdio::Index INLINE_CHUNK_SIZE = 128;
    const mdio::Index CROSSLINE_CHUNK_SIZE = 128;
    // These are arbitrary indices that align with the beginning of a chunk.
    const mdio::Index INLINE_START = 512;
    const mdio::Index CROSSLINE_START = 256;

    // Initialize the statistics object.
    VolumeStats<TraceType, CoordinateType> stats = {
        .avg_amplitude = 0,
        .max_amplitude = std::numeric_limits<TraceType>::min(),
        .min_amplitude = std::numeric_limits<TraceType>::max(),
        .peak_amplitude_inline = 0,
        .peak_amplitude_crossline = 0,
        .trough_amplitude_inline = 0,
        .trough_amplitude_crossline = 0,
        .num_samples = 0
    };

    // First, we are going to enter some arbitrary inline and crossline indices that align with the beginning of a chunk.
    mdio::RangeDescriptor<> inlineDesc = {"inline", INLINE_START, INLINE_START + INLINE_CHUNK_SIZE, 1};
    mdio::RangeDescriptor<> crosslineDesc = {"crossline", CROSSLINE_START, CROSSLINE_START + CROSSLINE_CHUNK_SIZE, 1};
    // We will not slice the time dimension, as we want the full trace length.

    // We will set up these range descriptors to hold our local indices as we iterate inside our subset dataset.
    mdio::RangeDescriptor<> inlineIndexDesc = {"inline", 0, INLINE_CHUNK_SIZE, 1};
    mdio::RangeDescriptor<> crosslineIndexDesc = {"crossline", 0, CROSSLINE_CHUNK_SIZE, 1};
    
    // Iterate over the inline and crossline chunks.
    for (mdio::Index inline_idx = INLINE_START; inline_idx<INLINE_START + INLINE_CHUNK_SIZE; inline_idx += INLINE_CHUNK_SIZE) {
        for (mdio::Index crossline_idx = CROSSLINE_START; crossline_idx<CROSSLINE_START + CROSSLINE_CHUNK_SIZE; crossline_idx += CROSSLINE_CHUNK_SIZE) {

            // Set up the slicing descriptors with the current global chunk to process.
            inlineDesc.start = inline_idx;
            inlineDesc.stop = inline_idx + INLINE_CHUNK_SIZE;
            crosslineDesc.start = crossline_idx;
            crosslineDesc.stop = crossline_idx + CROSSLINE_CHUNK_SIZE;

            // Ensure our local indices are reset to the start of the chunk.
            inlineIndexDesc.start = 0;
            crosslineIndexDesc.start = 0;

            // The result of the isel operation is a new Dataset object with the index extents we described above.
            MDIO_ASSIGN_OR_RETURN(auto sliced_ds, ds.isel(inlineDesc, crosslineDesc));

            // Now we get the relevant Variables from the sliced dataset to do our statistics.
            MDIO_ASSIGN_OR_RETURN(auto traceVariable, sliced_ds.variables.get<TraceType>("seismic"));
            MDIO_ASSIGN_OR_RETURN(auto inlineVariable, sliced_ds.variables.get<CoordinateType>("inline"));
            MDIO_ASSIGN_OR_RETURN(auto crosslineVariable, sliced_ds.variables.get<CoordinateType>("crossline"));
            MDIO_ASSIGN_OR_RETURN(auto timeVariable, sliced_ds.variables.get<CoordinateType>("time"));

            // Read is an asynchronous operation which executes immediately and with multiple threads and processes.
            auto traceFuture = traceVariable.Read();
            auto inlineFuture = inlineVariable.Read();
            auto crosslineFuture = crosslineVariable.Read();
            // We don't need to read the time Variable, we just get it from the sliced dataset to get the trace length.

            // Calling the value() and status() methods are blocking operations that wait for the future to complete and resolve.
            if (!traceFuture.status().ok()) {
                return traceFuture.status();
            }

            // We only need to wait for the trace to resolve, as the coordinates won't be needed until after the first trace is processed.
            auto trace = traceFuture.value();  // Resolve the future into a strongly typed VariableData object.
            auto tracePtr = trace.get_data_accessor().data();  // Get a pointer to the flattened trace data.
            auto traceOffset = trace.get_flattened_offset();  // The flattened trace data may have some constant pointer offset at the start.
            auto traceLength = timeVariable.num_samples();  // Notice, we used the time Variable to get the trace length.

            auto numTraceSamples = trace.num_samples();  // This gives us the total number of samples in the chunk.

            // Now, we are going to process the data tracewise.
            for (mdio::Index traceIdx = 0; traceIdx < numTraceSamples; traceIdx += traceLength) {
                auto local_minmax = std::minmax_element(tracePtr + traceOffset + traceIdx, tracePtr + traceOffset + traceIdx + traceLength);
                auto local_min = *(local_minmax.first);
                auto local_max = *(local_minmax.second);

                // Calculate the average amplitude for this trace.
                auto local_avg = std::accumulate(tracePtr + traceOffset + traceIdx, tracePtr + traceOffset + traceIdx + traceLength, 0.0) / traceLength;
                stats.avg_amplitude += local_avg;

                if (local_max > stats.max_amplitude) {
                    stats.max_amplitude = local_max;
                    MDIO_ASSIGN_OR_RETURN(auto coords, GetCoordinatePair(inlineFuture, crosslineFuture, inlineIndexDesc.start, crosslineIndexDesc.start));
                    stats.peak_amplitude_inline = std::get<0>(coords);
                    stats.peak_amplitude_crossline = std::get<1>(coords);
                }
                if (local_min < stats.min_amplitude) {
                    stats.min_amplitude = local_min;
                    MDIO_ASSIGN_OR_RETURN(auto coords, GetCoordinatePair(inlineFuture, crosslineFuture, inlineIndexDesc.start, crosslineIndexDesc.start));
                    stats.trough_amplitude_inline = std::get<0>(coords);
                    stats.trough_amplitude_crossline = std::get<1>(coords);
                }

                // Increment the local indices of the chunk. 
                inlineIndexDesc.start += inlineIndexDesc.step;
                if (inlineIndexDesc.start >= inlineIndexDesc.stop) {
                    inlineIndexDesc.start = 0;
                    crosslineIndexDesc.start += crosslineIndexDesc.step;
                    if (crosslineIndexDesc.start >= crosslineIndexDesc.stop) {
                        crosslineIndexDesc.start = 0;
                    }
                }
            }
            stats.num_samples += numTraceSamples;
        }
    }
    stats.avg_amplitude /= stats.num_samples;
    return stats;
}

/**
 * @brief Gets the inline and crossline coordinate value pairs for the given inline and crossline indices.
 * @tparam CoordinateType The type of the coordinate data.
 * @param inlineFuture The future containing the inline coordinate data.
 * @param crosslineFuture The future containing the crossline coordinate data.
 * @param inline_idx The inline index to get the coordinate value for.
 * @param crossline_idx The crossline index to get the coordinate value for.
 * @return A tuple containing the inline and crossline coordinate value pairs.
 */
template <typename CoordinateType>
mdio::Result<std::tuple<CoordinateType, CoordinateType>> GetCoordinatePair(mdio::Future<mdio::VariableData<CoordinateType>>& inlineFuture, mdio::Future<mdio::VariableData<CoordinateType>>& crosslineFuture, const mdio::Index inline_idx, const mdio::Index crossline_idx) {
    // Ensure that the futures are able to be resolved without error.
    if (!inlineFuture.status().ok()) {
        return inlineFuture.status();
    }
    if (!crosslineFuture.status().ok()) {
        return crosslineFuture.status();
    }
    // Resolve the futures into VariableData objects
    auto inlineData = inlineFuture.value();
    auto crosslineData = crosslineFuture.value();
    // Get the pointers to their data
    auto inlinePtr = inlineData.get_data_accessor().data();
    auto crosslinePtr = crosslineData.get_data_accessor().data();
    // They will likely have some pointer offset before the actual data starts, so we need to add that to the index.
    auto inlineOffset = inlineData.get_flattened_offset();
    auto crosslineOffset = crosslineData.get_flattened_offset();
    // Build the tuple of the inline and crossline coordinate values and return it.
    return std::make_tuple(inlinePtr[inlineOffset + inline_idx], crosslinePtr[crosslineOffset + crossline_idx]);
}

} // namespace stats
