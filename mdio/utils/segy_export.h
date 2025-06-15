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

#include <algorithm>     // for std::erase_if
#include <thread>        // for sleep_for
#include <chrono>        // for milliseconds

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
  auto cacheJson = nlohmann::json::parse(R"({
    "cache_pool":           { "total_bytes_limit": 5000000000 },
    "data_copy_concurrency": {"limit": 16},
    "gcs_request_concurrency": {"limit": 16},
    "s3_request_concurrency": {"limit": 16}
  })");
  auto ctxSpec = Context::Spec::FromJson(cacheJson);
  auto ctx     = Context(ctxSpec.value());

  // Open the input MDIO dataset
  MDIO_ASSIGN_OR_RETURN(
      auto ds,
      Dataset::Open(mdio_path, constants::kOpen, ctx).result());

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
  auto shape  = domain.shape();   // e.g. { ni, nc, nt }
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

  // Determine chunk size along second-fastest spatial dim
  Index output_chunk_size = (rank >= 2)
      ? shape[rank - 2]
      : shape[0];

  // Build Zarr spec for output SEG-Y
  nlohmann::json spec;
  spec["driver"] = "zarr";
  std::string driver = "file";
  if (absl::StartsWith(segy_path, "gs://")) driver = "gcs";
  else if (absl::StartsWith(segy_path, "s3://")) driver = "s3";
  spec["kvstore"] = {{"driver", driver}, {"path", segy_path}};
  if (driver != "file") {
    size_t pos = segy_path.find("://");
    std::string tail = segy_path.substr(pos + 3);
    std::vector<std::string> parts;
    for (auto& p : absl::StrSplit(tail, '/')) parts.emplace_back(p);
    spec["kvstore"]["bucket"] = parts[0];
    spec["kvstore"]["path"] = absl::StrJoin(parts.begin()+1, parts.end(), "/");
  }
  spec["metadata"] = {
      {"dtype", std::string("|V") + std::to_string(trace_bytes)},
      {"shape", {num_traces}},
      {"chunks", {output_chunk_size}},
      {"dimension_separator", "."},
      {"compressor", nullptr},
      {"fill_value", nullptr},
      {"order", "C"},
      {"zarr_format", 2}
  };
  spec["attributes"] = {
      {"dimension_names", {"trace"}},
      {"long_name", ""}
  };

  MDIO_ASSIGN_OR_RETURN(
      auto out_var,
      Variable<>::Open(spec, constants::kCreateClean, ctx).result());

  // === Throttle helper ===
  constexpr size_t MAX_IN_FLIGHT = 10;
  std::vector<tensorstore::WriteFutures> futures;
  auto enqueue_write = [&](tensorstore::WriteFutures wf) {
    while (futures.size() >= MAX_IN_FLIGHT) {
      // Remove completed writes using remove-erase
      futures.erase(
        std::remove_if(
          futures.begin(), futures.end(),
          [](auto& wf){ return wf.commit_future.ready(); }),
        futures.end());
      if (futures.size() >= MAX_IN_FLIGHT) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    futures.emplace_back(std::move(wf));
  };

  // Iterate over chunks and submit writes
  size_t chunk_dim = (rank >= 2) ? rank - 2 : 0;
  Index chunk_dim_size = shape[chunk_dim];
  Index num_outer_slices = 1;
  for (size_t d = 0; d < rank - 1; ++d) if (d != chunk_dim) num_outer_slices *= shape[d];
  std::vector<Index> outer_coords(rank - 1, 0);

  Index traces_processed = 0;
  for (Index outer = 0; outer < num_outer_slices; ++outer) {
    // compute outer_coords
    Index tmp = outer;
    for (int d = rank - 2; d >= 0; --d) {
      if ((size_t)d != chunk_dim) {
        outer_coords[d] = tmp % shape[d];
        tmp /= shape[d];
      }
    }

    for (Index start = 0; start < chunk_dim_size; start += output_chunk_size) {
      Index end   = std::min(start + output_chunk_size, chunk_dim_size);
      Index size  = end - start;

      // Progress bar (tqdm style)
      double progress = static_cast<double>(traces_processed) / static_cast<double>(num_traces);
      int bar_width = 50;
      int pos = static_cast<int>(bar_width * progress);
      std::cout << "\r[";
      for (int i = 0; i < bar_width; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
      }
      std::cout << "] " << static_cast<int>(progress * 100.0) << "% "
                << "(" << traces_processed << "/" << num_traces << " traces)";
      std::cout.flush();

      // Build slice descriptors
      std::vector<RangeDescriptor<Index>> descs;
      descs.reserve(rank);
      for (size_t d = 0; d < rank - 1; ++d) {
        if (d == chunk_dim) {
          descs.push_back({labels[d], start, end, 1});
        } else {
          descs.push_back({labels[d], outer_coords[d], outer_coords[d]+1, 1});
        }
      }
      descs.push_back({labels[rank-1], 0, num_samples, 1});

      MDIO_ASSIGN_OR_RETURN(auto var_slice, seismic_var.slice(descs));
      MDIO_ASSIGN_OR_RETURN(auto data,      var_slice.Read().result());

      const char* ptr = reinterpret_cast<const char*>(data.get_data_accessor().data());
      ptrdiff_t off  = data.get_flattened_offset();
      std::vector<char> buffer(size * trace_bytes, 0);
      for (Index i = 0; i < size; ++i) {
        char* tb = buffer.data() + i * trace_bytes;
        std::memset(tb, 0, 240);
        std::memcpy(tb + 240, ptr + (off + i*num_samples)*sample_bytes,
                    size_t(num_samples)*sample_bytes);
      }

      // Compute write range
      Index out_start = 0, mul = 1;
      std::vector<Index> coords = outer_coords;
      coords[chunk_dim] = start;
      for (int d = rank-2; d >= 0; --d) {
        out_start += coords[d] * mul;
        mul *= shape[d];
      }
      RangeDescriptor<Index> write_desc{"trace", out_start, out_start+size, 1};

      MDIO_ASSIGN_OR_RETURN(auto out_slice, out_var.slice(write_desc));
    //   MDIO_ASSIGN_OR_RETURN(auto out_data,  out_slice.Read().result());
    MDIO_ASSIGN_OR_RETURN(auto out_data, from_variable(out_slice));

      char* out_ptr = reinterpret_cast<char*>(out_data.get_data_accessor().data());
      ptrdiff_t out_off = out_data.get_flattened_offset();
      std::memcpy(out_ptr + out_off, buffer.data(), size * trace_bytes);

      // Submit write with throttle
      auto wf = out_slice.Write(out_data);
      enqueue_write(std::move(wf));

      traces_processed += size;
    }
  }

  // Drain remaining writes and check errors
  for (auto& wf : futures) {
    if (!wf.commit_future.result().ok()) {
      return wf.commit_future.status();
    }
  }

  std::cout << "\r[";
  for (int i = 0; i < 50; ++i) std::cout << "=";
  std::cout << "] 100% (" << num_traces << "/" << num_traces << ")\n"
            << "Conversion completed successfully!\n";

  return absl::OkStatus();
}

}  // namespace utils
}  // namespace mdio

#endif  // MDIO_UTILS_MDIO_TO_SEGY_H_