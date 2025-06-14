// Copyright 2025 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MDIO_UTILS_MDIO_TO_SEGY_H_
#define MDIO_UTILS_MDIO_TO_SEGY_H_

#include <cstring>
#include <string>
#include <vector>

#include "mdio/mdio.h"
#include "tensorstore/util/result.h"

#include <string>
#include <vector>
#include <array>
#include <algorithm>

#include "absl/status/status.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "mdio/impl.h"
#include "tensorstore/driver/zarr/dtype.h"

#include "nlohmann/json.hpp"

namespace mdio {
namespace utils {

struct TraceHeaderField {
  std::string name;
  uint8_t offset;  // Byte offset from start of header
  std::string dtype;
};

class TraceHeaderComposer {
 public:
  TraceHeaderComposer();

  // Applies overrides to the default header description.
  // Returns an error status on overlapping offsets or invalid total size.
  Result<void> ApplyOverrides(
      const std::vector<TraceHeaderField>& overrides);

  const std::vector<TraceHeaderField>& fields() const { return fields_; }

 private:
  std::vector<TraceHeaderField> fields_;
  Result<void> Validate() const;
  static tensorstore::Result<tensorstore::internal_zarr::ZarrDType> ParseDType(
      const std::string& dtype);
  static std::size_t DTypeSize(const std::string& dtype);
};

inline tensorstore::Result<tensorstore::internal_zarr::ZarrDType>
TraceHeaderComposer::ParseDType(const std::string& dtype) {
  return tensorstore::internal_zarr::ParseDType(dtype);
}

inline std::size_t TraceHeaderComposer::DTypeSize(const std::string& dtype) {
  // Parse the dtype string directly to get size
  if (dtype.empty()) return 0;
  
  // Handle void types like "|V4"
  if (dtype[0] == '|' && dtype[1] == 'V') {
    return std::stoi(dtype.substr(2));
  }
  
  // Handle standard dtypes like "<i2", "<f4", etc.
  if (dtype.length() >= 3) {
    char type_char = dtype[1];  // i, u, f, c
    std::string size_str = dtype.substr(2);
    
    try {
      int size = std::stoi(size_str);
      return static_cast<std::size_t>(size);
    } catch (const std::exception&) {
      return 0;
    }
  }
  
  return 0;
}

inline TraceHeaderComposer::TraceHeaderComposer() {
  fields_ = {
    {"trace_sequence_line", 1, "<i4"},
    {"trace_sequence_file", 5, "<i4"},
    {"field_record_number", 9, "<i4"},
    {"trace_number_within_field_record", 13, "<i4"},
    {"energy_source_point_number", 17, "<i4"},
    {"cdp_ensemble_number", 21, "<i4"},
    {"trace_number_within_cdp_ensemble", 25, "<i4"},
    {"trace_identification_code", 29, "<i2"},
    {"number_of_vertically_summed_traces", 31, "<i2"},
    {"number_of_horizontally_stacked_traces", 33, "<i2"},
    {"data_use", 35, "<i2"},
    {"distance_from_source_to_receiver", 37, "<i4"},
    {"receiver_group_elevation", 41, "<i4"},
    {"surface_elevation_at_source", 45, "<i4"},
    {"source_depth_below_surface", 49, "<i4"},
    {"datum_elevation_at_receiver_group", 53, "<i4"},
    {"datum_elevation_at_source", 57, "<i4"},
    {"water_depth_at_source", 61, "<i4"},
    {"water_depth_at_group", 65, "<i4"},
    {"scalar_elevation", 69, "<i2"},
    {"scalar_coordinates", 71, "<i2"},
    {"source_coordinate_x", 73, "<i4"},
    {"source_coordinate_y", 77, "<i4"},
    {"group_coordinate_x", 81, "<i4"},
    {"group_coordinate_y", 85, "<i4"},
    {"coordinate_units", 89, "<i2"},
    {"weathering_velocity", 91, "<i2"},
    {"subweathering_velocity", 93, "<i2"},
    {"uphole_time_source", 95, "<i2"},
    {"uphole_time_group", 97, "<i2"},
    {"source_static_correction", 99, "<i2"},
    {"group_static_correction", 101, "<i2"},
    {"total_static_applied", 103, "<i2"},
    {"lag_time_A", 105, "<i2"},
    {"lag_time_B", 107, "<i2"},
    {"delay_recording_time", 109, "<i2"},
    {"mute_time_start", 111, "<i2"},
    {"mute_time_end", 113, "<i2"},
    {"number_of_samples", 115, "<u2"},
    {"sample_interval", 117, "<u2"},
    {"gain_type_of_field_instruments", 119, "<i2"},
    {"instrument_gain_constant", 121, "<i2"},
    {"instrument_early_or_initial_gain", 123, "<i2"},
    {"correlated", 125, "<i2"},
    {"sweep_frequency_at_start", 127, "<u2"},
    {"sweep_frequency_at_end", 129, "<u2"},
    {"sweep_length", 131, "<u2"},
    {"sweep_type", 133, "<i2"},
    {"sweep_trace_taper_length_at_start", 135, "<i2"},
    {"sweep_trace_taper_length_at_end", 137, "<i2"},
    {"taper_type", 139, "<i2"},
    {"correlated_data_traces", 141, "<i2"},
    {"binary_gain_recovered", 143, "<i2"},
    {"amplitude_recovery_method", 145, "<i2"},
    {"measurement_system", 147, "<i2"},
    {"impulse_signal_polarity", 149, "<i2"},
    {"vibratory_polarity_code", 151, "<i2"},

    // # **SEG‑Y Rev 1 additions (bytes 153–160ish)**
    {"segy_format_revision", 153, "<i2"},            // # Format revision (always 1 for Rev 1)
    {"fixed_length_trace_flag", 155, "<i2"},         // # 1 = fixed-length traces present :contentReference[oaicite:1]{index=1}
    {"num_textual_hdr_ext", 157, "<i2"},             // # Number of 3200‑byte Extended Textual File Header records :contentReference[oaicite:2]{index=2}

    // # **Trace identification extras (bytes 159–240)**
    {"cdp_x_coordinate", 181, "<i4"},
    {"cdp_y_coordinate", 185, "<i4"},
    {"inline_3d_position", 189, "<i4"},
    {"crossline_3d_position", 193, "<i4"},
    {"shotpoint_number", 197, "<i4"},
    {"shotpoint_scalar", 201, "<i2"},
    {"trace_value_measurement_unit", 203, "<i2"},
    {"transduction_constant_mantissa", 205, "<i4"},
    {"transduction_constant_exponent", 209, "<i2"},
    {"transduction_unit", 211, "<i2"},
    {"trace_identifier", 213, "<i2"},
    {"scalar_trace_header", 215, "<i2"},
    {"source_type_orientation", 217, "<i2"},
    {"source_energy_dir_mantissa", 219, "<i4"},
    {"source_energy_dir_exponent", 223, "<i2"},
    {"source_measurement_mantissa", 225, "<i4"},
    {"source_measurement_exponent", 229, "<i2"},
    {"source_measurement_unit", 231, "<i2"},
    {"unassigned_byte1", 233, "<i1"},
    {"unassigned_byte2", 234, "<i1"},
    {"unassigned_byte3", 235, "<i1"},
    {"unassigned_byte4", 236, "<i1"},
    {"unassigned_byte5", 237, "<i1"},
    {"unassigned_byte6", 238, "<i1"},
    {"unassigned_byte7", 239, "<i1"},
    {"unassigned_byte8", 240, "<i1"},
};
}

inline Result<void> TraceHeaderComposer::Validate() const {
  std::array<bool, 240> used{};
  for (const auto& f : fields_) {
    auto size = DTypeSize(f.dtype);
    if (size == 0) {
      return absl::InvalidArgumentError("Invalid dtype");
    }
    if (static_cast<std::size_t>(f.offset) + size > 240) {
      return absl::InvalidArgumentError("Field exceeds 240 bytes");
    }
    for (std::size_t i = 0; i < size; ++i) {
      if (used[f.offset + i]) {
        return absl::InvalidArgumentError("Overlapping header bytes");
      }
      used[f.offset + i] = true;
    }
  }
  for (bool b : used) {
    if (!b) return absl::InvalidArgumentError("Header does not cover 240 bytes");
  }
  return absl::OkStatus();
}

inline Result<void> TraceHeaderComposer::ApplyOverrides(
    const std::vector<TraceHeaderField>& overrides) {
  // Check overlaps within overrides
  std::array<bool, 240> coverage{};
  for (const auto& o : overrides) {
    auto size = DTypeSize(o.dtype);
    if (size == 0 || static_cast<std::size_t>(o.offset) + size > 240) {
      return absl::InvalidArgumentError("Invalid override specification");
    }
    for (std::size_t i = 0; i < size; ++i) {
      if (coverage[o.offset + i]) {
        return absl::InvalidArgumentError("Override fields overlap");
      }
      coverage[o.offset + i] = true;
    }
  }

  // Apply overrides
  std::vector<TraceHeaderField> new_fields;
  for (const auto& base : fields_) {
    auto base_size = DTypeSize(base.dtype);
    bool replaced = false;
    for (const auto& o : overrides) {
      auto o_size = DTypeSize(o.dtype);
      auto start = o.offset;
      auto end = static_cast<uint8_t>(o.offset + o_size);
      auto bstart = base.offset;
      auto bend = static_cast<uint8_t>(base.offset + base_size);
      if (start >= bend || end <= bstart) {
        continue;
      }
      // overlapping
      replaced = true;
    }
    if (!replaced) new_fields.push_back(base);
  }

  // Remove any base fields overlapped by overrides
  for (const auto& o : overrides) {
    new_fields.push_back(o);
  }

  // Sort by offset
  std::sort(new_fields.begin(), new_fields.end(),
            [](const TraceHeaderField& a, const TraceHeaderField& b) {
              return a.offset < b.offset;
            });

  // Fill gaps with unassigned
  std::vector<TraceHeaderField> final_fields;
  uint8_t pos = 0;
  for (const auto& f : new_fields) {
    auto size = DTypeSize(f.dtype);
    if (f.offset > pos) {
      final_fields.push_back({"unassigned", pos,
                             "|V" + std::to_string(f.offset - pos)});
    }
    final_fields.push_back(f);
    pos = f.offset + size;
  }
  if (pos < 240) {
    final_fields.push_back({"unassigned", pos,
                           "|V" + std::to_string(240 - pos)});
    pos = 240;
  }

  fields_ = std::move(final_fields);
  return Validate();
}

/**
 * @brief Converts an MDIO dataset to a SEG-Y styled Variable.
 *
 * This utility will walk a seismic cube stored in MDIO and convert the samples
 * into a SEG-Y trace file. The SEG-Y file is represented as a Zarr array of
 * void elements, each element containing the trace header and samples for a
 * single trace.
 *
 * @note The text header, binary header, and trace header format inputs are
 *       currently unused.
 *
 * @param text_header   The SEG-Y text header.
 * @param binary_header The SEG-Y binary header.
 * @param trace_headers Trace header description.
 * @param mdio_path     Path to the input MDIO dataset.
 * @param segy_path     Path where the output SEG-Y variable will be created.
 * @return Status of the conversion.
 */
Result<void> MdioToSegy(
    const std::string& text_header,
    const std::string& binary_header,
    const TraceHeaderComposer& trace_headers,
    const std::string& mdio_path,
    const std::string& segy_path) {
  (void)text_header;
  (void)binary_header;
  (void)trace_headers;

  // 1 GiB cache
  auto cacheJson = nlohmann::json::parse(
      R"({"cache_pool": {"total_bytes_limit": 5000000000}})");
  auto ctxSpec = Context::Spec::FromJson(cacheJson);
  auto ctx     = Context(ctxSpec.value());

  // Open the input MDIO dataset
  MDIO_ASSIGN_OR_RETURN(
      auto ds,
      Dataset::Open(mdio_path, constants::kOpen, ctx).result());

  std::cout << ds << std::endl;

  // Pick the highest-rank (float-prefer) seismic variable
  bool    found      = false;
  Variable<> seismic_var;
  size_t  best_rank  = 0;
  bool    best_float = false;
  for (auto key : ds.variables.get_iterable_accessor()) {
    MDIO_ASSIGN_OR_RETURN(auto var, ds.variables.at(key));
    size_t  r = var.dimensions().rank();
    bool    f = (var.dtype() == constants::kFloat16 ||
                 var.dtype() == constants::kFloat32 ||
                 var.dtype() == constants::kFloat64);
    if (!found || r > best_rank ||
        (r == best_rank && f && !best_float)) {
      found      = true;
      seismic_var = var;
      best_rank  = r;
      best_float = f;
    }
  }
  if (!found) {
    return absl::NotFoundError("No variables found in dataset");
  }

  // Get dims & shape from the seismic variable's ordered domain
  auto domain = seismic_var.dimensions();
  auto labels = domain.labels();  // e.g. {"inline","crossline","time"}
  for (const auto& l : labels) {
    std::cout << l << std::endl;
  }
  auto shape  = domain.shape();   // e.g. { ni,       nc,         nt }
  size_t rank = shape.size();
  if (rank < 2) {
    return absl::InvalidArgumentError(
        "Dataset must be at least 2D (spatial dims + samples)");
  }

  // Compute counts
  Index num_samples  = shape[rank - 1];  // length of last dim
  size_t sample_bytes = seismic_var.dtype().size();
  size_t trace_bytes  = sample_bytes * num_samples + 240u;
  Index num_traces    = 1;
  for (size_t i = 0; i < rank - 1; ++i) {
    num_traces *= shape[i];
  }

  // Use the domain size of the second-fastest growing dimension for chunking
  // This ensures we process complete "slices" along that dimension
  Index output_chunk_size = 1;
  if (rank >= 2) {
    // Use the full domain size of the second-fastest growing spatial dimension
    output_chunk_size = shape[rank - 2];
  } else if (rank >= 1) {
    // Fallback to first spatial dimension size
    output_chunk_size = shape[0];
  }
  
  std::cout << "Using output chunk size: " << output_chunk_size << " (full domain of dimension " 
            << (rank >= 2 ? rank - 2 : 0) << ")" << std::endl;

  // Build Zarr spec for output SEG-Y
  nlohmann::json spec;
  spec["driver"] = "zarr";

  // infer driver from URI
  std::string driver = "file";
  if (absl::StartsWith(segy_path, "gs://")) driver = "gcs";
  else if (absl::StartsWith(segy_path, "s3://")) driver = "s3";
  spec["kvstore"] = {{"driver", driver}, {"path", segy_path}};

  if (driver != "file") {
    // strip "gs://" or "s3://"
    size_t pos = segy_path.find("://");
    std::string tail = segy_path.substr(pos + 3);
    // build a real vector<string>
    std::vector<std::string> file_parts;
    for (auto piece : absl::StrSplit(tail, '/')) {
      file_parts.emplace_back(piece);
    }
    if (file_parts.size() < 2) {
      return absl::InvalidArgumentError(
          "Cloud path requires [gs/s3]://bucket/path format");
    }
    spec["kvstore"]["bucket"] = file_parts[0];
    // rejoin remainder as object path
    spec["kvstore"]["path"] =
        absl::StrJoin(file_parts.begin() + 1, file_parts.end(), "/");
  }

  spec["metadata"] = {
      {"dtype", std::string("|V") + std::to_string(trace_bytes)},
      {"shape", {num_traces}},
      {"chunks", {output_chunk_size}},  // Use the computed chunk size instead of 1
      {"dimension_separator", "."},
      {"compressor", nullptr},
      {"fill_value", nullptr},
      {"order", "C"},
      {"zarr_format", 2}};
  spec["attributes"] = {
      {"dimension_names", {"trace"}},
      {"long_name", ""}};

  MDIO_ASSIGN_OR_RETURN(
      auto out_var,
      Variable<>::Open(spec, constants::kCreateClean, ctx).result());

  // Process traces by chunking along the second-fastest growing dimension
  // This ensures we align with the underlying storage chunks
  size_t chunk_dim = (rank >= 2) ? rank - 2 : 0;  // Index of second-fastest growing spatial dim
  Index chunk_dim_size = shape[chunk_dim];
  
  // Calculate total number of "slices" in all other spatial dimensions
  Index num_outer_slices = 1;
  for (size_t d = 0; d < rank - 1; ++d) {
    if (d != chunk_dim) {
      num_outer_slices *= shape[d];
    }
  }
  
  Index traces_processed = 0;
  
  // Iterate through all combinations of the other spatial dimensions
  std::vector<Index> outer_coords(rank - 1, 0);
  
  for (Index outer_slice = 0; outer_slice < num_outer_slices; ++outer_slice) {
    // Convert outer_slice to coordinates for all dimensions except chunk_dim
    Index temp_slice = outer_slice;
    for (int d = static_cast<int>(rank) - 2; d >= 0; --d) {
      if (static_cast<size_t>(d) != chunk_dim) {
        outer_coords[d] = temp_slice % shape[d];
        temp_slice /= shape[d];
      }
    }
    
    // Now chunk along the chunk_dim
    for (Index chunk_start = 0; chunk_start < chunk_dim_size; chunk_start += output_chunk_size) {
      Index chunk_end = std::min(chunk_start + output_chunk_size, chunk_dim_size);
      Index chunk_size = chunk_end - chunk_start;
      
      // Progress bar (tqdm style)
      double progress = static_cast<double>(traces_processed) / static_cast<double>(num_traces);
      int bar_width = 50;
      int pos = static_cast<int>(bar_width * progress);
      
    //   std::cout << "\r[";
    //   for (int i = 0; i < bar_width; ++i) {
    //     if (i < pos) std::cout << "=";
    //     else if (i == pos) std::cout << ">";
    //     else std::cout << " ";
    //   }
    //   std::cout << "] " << static_cast<int>(progress * 100.0) << "% "
    //             << "(" << traces_processed << "/" << num_traces << " traces)";
    //   std::cout.flush();

      // Build slice descriptors for this specific chunk
      std::vector<RangeDescriptor<Index>> chunk_descs;
      chunk_descs.reserve(rank);
      
      // For each spatial dimension, either use the fixed coordinate or the chunk range
      for (size_t d = 0; d < rank - 1; ++d) {
        if (d == chunk_dim) {
          // This is the dimension we're chunking along
          chunk_descs.push_back(RangeDescriptor<Index>{
              labels[d],
              chunk_start,
              chunk_end,
              1});
        } else {
          // This is a fixed coordinate for this outer slice
          chunk_descs.push_back(RangeDescriptor<Index>{
              labels[d],
              outer_coords[d],
              outer_coords[d] + 1,
              1});
        }
      }
      
      // Full slice on the last dimension (samples/time)
      chunk_descs.push_back(RangeDescriptor<Index>{
          labels[rank - 1],
          0,
          num_samples,
          1});

      // Read only the seismic data we need for this chunk
      MDIO_ASSIGN_OR_RETURN(auto chunk_var, seismic_var.slice(chunk_descs));
      std::cout << chunk_var << std::endl;
      MDIO_ASSIGN_OR_RETURN(auto chunk_data, chunk_var.Read().result());
      std::cout << "Finished reading chunk_var..." << std::endl;
      
      const char* chunk_ptr = reinterpret_cast<const char*>(
          chunk_data.get_data_accessor().data());
      ptrdiff_t chunk_off = chunk_data.get_flattened_offset();
      
      // Allocate buffer for the output chunk
      std::vector<char> chunk_buffer(chunk_size * trace_bytes, 0);

      // Process each trace in the chunk
      for (Index i = 0; i < chunk_size; ++i) {
        // Calculate the offset in the chunk data for this trace
        Index trace_offset_in_chunk = i * num_samples;

        // Copy trace data to chunk buffer (240 bytes header + samples)
        char* trace_buffer = chunk_buffer.data() + (i * trace_bytes);
        std::memset(trace_buffer, 0, 240);  // Zero the header
        std::memcpy(
            trace_buffer + 240,
            chunk_ptr + (chunk_off + trace_offset_in_chunk) * sample_bytes,
            static_cast<size_t>(num_samples) * sample_bytes);
      }

      // Calculate the output trace range for this chunk
      // Convert coordinates back to linear trace indices
      Index output_start = 0;
      Index multiplier = 1;
      
      // Build the linear index from coordinates
      std::vector<Index> trace_coords = outer_coords;
      trace_coords[chunk_dim] = chunk_start;
      
      for (int d = static_cast<int>(rank) - 2; d >= 0; --d) {
        output_start += trace_coords[d] * multiplier;
        multiplier *= shape[d];
      }
      
      Index output_end = output_start + chunk_size;

      // Write the chunk to output
      RangeDescriptor<Index> write_desc{"trace", output_start, output_end, 1};
      MDIO_ASSIGN_OR_RETURN(auto out_slice, out_var.slice(write_desc));
      std::cout << out_slice << std::endl;
      MDIO_ASSIGN_OR_RETURN(auto out_data, out_slice.Read().result());
      std::cout << "Finished reading out_data..." << std::endl;
      char* out_ptr = reinterpret_cast<char*>(
          out_data.get_data_accessor().data());
      ptrdiff_t out_off = out_data.get_flattened_offset();
      
      // Copy the chunk buffer to output
      std::memcpy(out_ptr + out_off, chunk_buffer.data(), chunk_size * trace_bytes);
      
      // Write it back
      std::cout << "Writing back to out_slice..." << std::endl;
      auto fut = out_slice.Write(out_data);
      if (!fut.status().ok()) return fut.status();
      std::cout << "Finished writing back to out_slice..." << std::endl;
      traces_processed += chunk_size;
    }
  }

  // Final progress bar showing 100% completion
  std::cout << "\r[";
  for (int i = 0; i < 50; ++i) {
    std::cout << "=";
  }
  std::cout << "] 100% (" << num_traces << "/" << num_traces << " traces)" << std::endl;
  std::cout << "Conversion completed successfully!" << std::endl;

  return absl::OkStatus();
}

}  // namespace utils
}  // namespace mdio

#endif  // MDIO_UTILS_MDIO_TO_SEGY_H_