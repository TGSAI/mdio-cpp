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
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <fstream>

#include <algorithm>     // for std::erase_if
#include <thread>        // for sleep_for
#include <chrono>        // for milliseconds
#include <set>           // for std::set
#include <iostream>      // for std::cerr

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

// Structure to hold information about how to populate a trace header field
struct TraceHeaderMapping {
  std::string field_name;
  uint8_t offset;
  std::string dtype;
  size_t size;
  
  // Source information
  enum class SourceType { STRUCTURED_VARIABLE, DIMENSION_VARIABLE } source_type;
  std::string variable_name;  // For structured vars, this is the main variable name
  std::string field_name_in_var;  // For structured vars, this is the field name
  
  TraceHeaderMapping(const std::string& fn, uint8_t off, const std::string& dt, size_t sz,
                     SourceType st, const std::string& vn, const std::string& fin = "")
    : field_name(fn), offset(off), dtype(dt), size(sz),
      source_type(st), variable_name(vn), field_name_in_var(fin) {}
};

class TraceHeaderComposer {
 public:
  TraceHeaderComposer();

  // Applies overrides to the default header description.
  // Returns an error status on overlapping offsets or invalid total size.
  Result<void> ApplyOverrides(
      const std::vector<TraceHeaderField>& overrides);

  const std::vector<TraceHeaderField>& fields() const { return fields_; }

  // Make DTypeSize public so TraceHeaderMapper can access it
  static std::size_t DTypeSize(const std::string& dtype);

 private:
  std::vector<TraceHeaderField> fields_;
  Result<void> Validate() const;
  static tensorstore::Result<tensorstore::internal_zarr::ZarrDType> ParseDType(
      const std::string& dtype);
};

class TraceHeaderMapper {
 public:
  TraceHeaderMapper(const Dataset& dataset, const TraceHeaderComposer& composer);
  
  // Analyze dataset and create mappings for available header fields
  Result<void> CreateMappings();
  
  // Get all the mappings
  const std::vector<TraceHeaderMapping>& GetMappings() const { return mappings_; }
  
  // Populate trace header buffer for a specific trace location
  Result<void> PopulateTraceHeader(char* header_buffer, 
                                   const std::vector<Index>& trace_coords) const;
  
 private:
  const Dataset& dataset_;
  const TraceHeaderComposer& composer_;
  std::vector<TraceHeaderMapping> mappings_;
  
  Result<void> AnalyzeStructuredVariables();
  Result<void> AnalyzeDimensionVariables();
  
  // Check if a dtype string matches between trace header and variable
  bool DTypesMatch(const std::string& header_dtype, const std::string& var_dtype) const;
  
  // Convert MDIO dtype to SEG-Y trace header dtype
  std::string ConvertToTraceHeaderDType(const std::string& mdio_dtype) const;
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

    // # **SEG‑Y Rev 1 additions (bytes 153–160ish)**
    {"segy_format_revision", 153, "<i2"},            // # Format revision (always 1 for Rev 1)
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
    
    // SEG-Y uses 1-based indexing, so valid byte positions are 1-240
    // Convert to 0-based for array indexing
    if (f.offset < 1 || static_cast<std::size_t>(f.offset - 1) + size > 240) {
      return absl::InvalidArgumentError("Field exceeds 240 bytes");
    }
    
    for (std::size_t i = 0; i < size; ++i) {
      size_t array_index = f.offset - 1 + i;  // Convert 1-based to 0-based
      if (used[array_index]) {
        return absl::InvalidArgumentError("Overlapping header bytes");
      }
      used[array_index] = true;
    }
  }
  
  // Check for gaps
  for (size_t i = 0; i < 240; ++i) {
    if (!used[i]) {
      return absl::InvalidArgumentError("Header does not cover 240 bytes");
    }
  }
  
  return absl::OkStatus();
}

inline Result<void> TraceHeaderComposer::ApplyOverrides(
    const std::vector<TraceHeaderField>& overrides) {
  
  // Check overlaps within overrides
  std::array<bool, 240> coverage{};
  for (const auto& o : overrides) {
    auto size = DTypeSize(o.dtype);
    
    if (size == 0) {
      return absl::InvalidArgumentError("Invalid override specification");
    }
    
    // SEG-Y uses 1-based indexing, so valid byte positions are 1-240
    if (o.offset < 1 || static_cast<std::size_t>(o.offset - 1) + size > 240) {
      return absl::InvalidArgumentError("Invalid override specification");
    }
    
    for (std::size_t i = 0; i < size; ++i) {
      size_t array_index = o.offset - 1 + i;  // Convert 1-based to 0-based
      if (coverage[array_index]) {
        return absl::InvalidArgumentError("Override fields overlap");
      }
      coverage[array_index] = true;
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
      replaced = true;
      break;
    }
    
    if (!replaced) {
      new_fields.push_back(base);
    }
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
  uint8_t pos = 1;  // Start at 1 for SEG-Y 1-based indexing
  for (const auto& f : new_fields) {
    auto size = DTypeSize(f.dtype);
    if (f.offset > pos) {
      final_fields.push_back({"unassigned", pos,
                             "|V" + std::to_string(f.offset - pos)});
    }
    final_fields.push_back(f);
    pos = f.offset + size;
  }
  
  if (pos < 241) {  // SEG-Y goes from 1-240, so next position after 240 is 241
    final_fields.push_back({"unassigned", pos,
                           "|V" + std::to_string(241 - pos)});
    pos = 241;
  }

  fields_ = std::move(final_fields);
  return Validate();
}

inline TraceHeaderMapper::TraceHeaderMapper(const Dataset& dataset, 
                                           const TraceHeaderComposer& composer)
  : dataset_(dataset), composer_(composer) {}

inline Result<void> TraceHeaderMapper::CreateMappings() {
  // First analyze structured variables (they take precedence)
  auto status1 = AnalyzeStructuredVariables();
  if (!status1.ok()) return status1;
  
  // Then analyze dimension variables for fields not yet mapped
  auto status2 = AnalyzeDimensionVariables();
  if (!status2.ok()) return status2;
  
  return absl::OkStatus();
}

inline Result<void> TraceHeaderMapper::AnalyzeStructuredVariables() {
  // Get all variable names from the dataset
  auto variable_keys = dataset_.variables.get_iterable_accessor();
  
  for (const auto& var_name : variable_keys) {
    MDIO_ASSIGN_OR_RETURN(auto var, dataset_.variables.at(var_name));
    
    // Check if this variable has a structured dtype
    auto spec_result = var.spec();
    if (!spec_result.ok()) {
      continue;
    }
    
    auto spec_json_result = spec_result.value().ToJson(IncludeDefaults{});
    if (!spec_json_result.ok()) {
      continue;
    }
    
    auto spec_json = spec_json_result.value();
    if (!spec_json["metadata"]["dtype"].is_array()) {
      continue;
    }
    
    // This is a structured variable - check each field
    for (const auto& field : spec_json["metadata"]["dtype"]) {
      if (!field.is_array() || field.size() < 2) continue;
      
      std::string field_name = field[0].get<std::string>();
      std::string field_dtype = field[1].get<std::string>();
      
      // Look for matching trace header field
      for (const auto& header_field : composer_.fields()) {
        if (header_field.name == field_name && 
            DTypesMatch(header_field.dtype, field_dtype)) {
          
          // Create mapping for this field
          size_t field_size = TraceHeaderComposer::DTypeSize(header_field.dtype);
          mappings_.emplace_back(
            field_name, header_field.offset, header_field.dtype, field_size,
            TraceHeaderMapping::SourceType::STRUCTURED_VARIABLE, 
            var_name, field_name
          );
          break;
        }
      }
    }
  }
  
  return absl::OkStatus();
}

inline Result<void> TraceHeaderMapper::AnalyzeDimensionVariables() {
  // Get all variable names from the dataset
  auto variable_keys = dataset_.variables.get_iterable_accessor();
  
  // Create a set of already mapped field names to avoid duplicates
  std::set<std::string> mapped_fields;
  for (const auto& mapping : mappings_) {
    mapped_fields.insert(mapping.field_name);
  }
  
  for (const auto& var_name : variable_keys) {
    MDIO_ASSIGN_OR_RETURN(auto var, dataset_.variables.at(var_name));
    
    // Skip if this variable name is already mapped by a structured variable
    if (mapped_fields.count(var_name) > 0) {
      continue;
    }
    
    // Get variable dtype
    auto dtype = var.dtype();
    std::string dtype_str;
    
    // Convert tensorstore dtype to string representation
    if (dtype == constants::kInt8) dtype_str = "<i1";
    else if (dtype == constants::kInt16) dtype_str = "<i2";
    else if (dtype == constants::kInt32) dtype_str = "<i4";
    else if (dtype == constants::kInt64) dtype_str = "<i8";
    else if (dtype == constants::kUint8) dtype_str = "<u1";
    else if (dtype == constants::kUint16) dtype_str = "<u2";
    else if (dtype == constants::kUint32) dtype_str = "<u4";
    else if (dtype == constants::kUint64) dtype_str = "<u8";
    else if (dtype == constants::kFloat16) dtype_str = "<f2";
    else if (dtype == constants::kFloat32) dtype_str = "<f4";
    else if (dtype == constants::kFloat64) dtype_str = "<f8";
    else {
      continue; // Unsupported dtype
    }
    
    // Look for matching trace header field by name and dtype
    for (const auto& header_field : composer_.fields()) {
      if (header_field.name == var_name && 
          DTypesMatch(header_field.dtype, dtype_str)) {
        
        // Create mapping for this field
        size_t field_size = TraceHeaderComposer::DTypeSize(header_field.dtype);
        mappings_.emplace_back(
          var_name, header_field.offset, header_field.dtype, field_size,
          TraceHeaderMapping::SourceType::DIMENSION_VARIABLE, 
          var_name, ""
        );
        mapped_fields.insert(var_name);
        break;
      }
    }
  }
  
  return absl::OkStatus();
}

inline bool TraceHeaderMapper::DTypesMatch(const std::string& header_dtype, 
                                          const std::string& var_dtype) const {
  // Direct match
  if (header_dtype == var_dtype) return true;
  
  // Convert and compare
  std::string converted = ConvertToTraceHeaderDType(var_dtype);
  return header_dtype == converted;
}

inline std::string TraceHeaderMapper::ConvertToTraceHeaderDType(const std::string& mdio_dtype) const {
  // MDIO dtype strings should already be in the correct format for most cases
  return mdio_dtype;
}

inline Result<void> TraceHeaderMapper::PopulateTraceHeader(
    char* header_buffer, const std::vector<Index>& trace_coords) const {
  for (const auto& mapping : mappings_) {
    try {
      if (mapping.source_type == TraceHeaderMapping::SourceType::DIMENSION_VARIABLE) {
        // Read from dimension variable - this is the simpler case
        MDIO_ASSIGN_OR_RETURN(auto var, dataset_.variables.at(mapping.variable_name));
        
        // For dimension variables, we need to find the coordinate index that matches this variable
        auto domain = var.dimensions();
        auto labels = domain.labels();
        
        // Find the dimension index that corresponds to this variable
        for (size_t dim_idx = 0; dim_idx < labels.size() && dim_idx < trace_coords.size(); ++dim_idx) {
          if (labels[dim_idx] == mapping.variable_name) {
            // Create a slice descriptor to get the specific coordinate value
            std::vector<RangeDescriptor<Index>> descs;
            descs.push_back({labels[dim_idx], trace_coords[dim_idx], trace_coords[dim_idx] + 1, 1});
            
            MDIO_ASSIGN_OR_RETURN(auto var_slice, var.slice(descs));
            MDIO_ASSIGN_OR_RETURN(auto data, var_slice.Read().result());
            
            // Debug: Print the actual value being copied
            const char* src_ptr = reinterpret_cast<const char*>(data.get_data_accessor().data());
            ptrdiff_t src_offset = data.get_flattened_offset();
            
            if (mapping.dtype == "<i4") {
              int32_t value;
              std::memcpy(&value, src_ptr + src_offset, sizeof(value));
            } else if (mapping.dtype == "<i2") {
              int16_t value;
              std::memcpy(&value, src_ptr + src_offset, sizeof(value));
            } else if (mapping.dtype == "<u2") {
              uint16_t value;
              std::memcpy(&value, src_ptr + src_offset, sizeof(value));
            } else if (mapping.dtype == "<u4") {
              uint32_t value;
              std::memcpy(&value, src_ptr + src_offset, sizeof(value));
            } else if (mapping.dtype == "<f4") {
              float value;
              std::memcpy(&value, src_ptr + src_offset, sizeof(value));
            }
            
            // Copy the data to trace header buffer
            std::memcpy(header_buffer + mapping.offset - 1, src_ptr + src_offset, mapping.size);
            
            break;
          }
        }
      }
      
    } catch (const std::exception& e) {
      // Log error but continue with other fields
      std::cerr << "Warning: Failed to populate trace header field '" 
                << mapping.field_name << "': " << e.what() << std::endl;
    }
  }
  
  return absl::OkStatus();
}

// Forward declaration
Result<void> CreateSegyTextHeader(
    const std::string& text_header,
    const Dataset& dataset,
    const std::string& mdio_path,
    const std::string& segy_path,
    const std::vector<TraceHeaderField>& overrides,
    const Context& ctx);

// Forward declaration for binary header
Result<void> CreateSegyBinaryHeader(
    const std::string& binary_header,
    const Dataset& dataset,
    const std::string& segy_path,
    Index num_traces,
    Index num_samples,
    uint16_t sample_interval_us,
    const Variable<>& seismic_var,
    const Context& ctx);

// Forward declaration for SEG-Y file finalization
Result<void> FinalizeSegyFile(
    const std::string& segy_path,
    Index output_chunk_size,
    Index num_traces,
    const Context& ctx);

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

  // Create and initialize trace header mapper
  TraceHeaderMapper header_mapper(ds, trace_headers);
  auto mapper_status = header_mapper.CreateMappings();
  if (!mapper_status.ok()) return mapper_status;
  
  // Print mapping information
  const auto& mappings = header_mapper.GetMappings();
  std::cout << "Found " << mappings.size() << " trace header field mappings:" << std::endl;
  for (const auto& mapping : mappings) {
    std::cout << "  - " << mapping.field_name << " (offset " << static_cast<int>(mapping.offset) 
              << ") -> ";
    if (mapping.source_type == TraceHeaderMapping::SourceType::STRUCTURED_VARIABLE) {
      std::cout << "structured variable '" << mapping.variable_name 
                << "' field '" << mapping.field_name_in_var << "'" << std::endl;
    } else {
      std::cout << "dimension variable '" << mapping.variable_name << "'" << std::endl;
    }
  }

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

  // Early validation: test if we can write to the output location
  std::cout << "Validating write access to output location..." << std::endl;
  try {
    // Try to write a small test chunk to validate permissions and connectivity
    RangeDescriptor<Index> test_desc{"trace", 0, 1, 1};
    MDIO_ASSIGN_OR_RETURN(auto test_slice, out_var.slice(test_desc));
    MDIO_ASSIGN_OR_RETURN(auto test_data, from_variable(test_slice));
    
    // Write a small test pattern
    char* test_ptr = reinterpret_cast<char*>(test_data.get_data_accessor().data());
    std::memset(test_ptr, 0, trace_bytes);
    
    auto test_write = test_slice.Write(test_data);
    auto test_result = test_write.commit_future.result();
    if (!test_result.ok()) {
      return absl::PermissionDeniedError("Cannot write to output location: " + test_result.status().ToString());
    }
    std::cout << "Write access validated successfully." << std::endl;
  } catch (const std::exception& e) {
    return absl::PermissionDeniedError("Cannot write to output location: " + std::string(e.what()));
  }

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
        
        // Initialize trace header to zeros
        std::memset(tb, 0, 240);
        
        // Populate trace header fields from mapped data
        if (!mappings.empty()) {
          // Calculate the coordinate for this trace
          std::vector<Index> trace_coords = outer_coords;
          trace_coords[chunk_dim] = start + i;
          
          // Use the header mapper to populate the trace header
          auto populate_result = header_mapper.PopulateTraceHeader(tb, trace_coords);
          if (!populate_result.ok()) {
            std::cerr << "Warning: Failed to populate trace header for trace at coordinates [";
            for (size_t c = 0; c < trace_coords.size(); ++c) {
              std::cerr << trace_coords[c];
              if (c < trace_coords.size() - 1) std::cerr << ", ";
            }
            std::cerr << "]: " << populate_result.status() << std::endl;
          }
        }
        
        // Copy seismic data
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

  // Calculate sample interval in microseconds
  uint16_t sample_interval_us = 1000; // Default 1ms if not found
  try {
    // Try to get sample interval from dataset metadata or dimensions
    auto time_dim_labels = domain.labels();
    if (!time_dim_labels.empty()) {
      std::string time_dim = time_dim_labels.back(); // Last dimension is usually time
      // Try to find time-related metadata
      auto metadata = ds.getMetadata();
      if (metadata.contains("attributes")) {
        auto attrs = metadata["attributes"];
        if (attrs.contains("sample_rate")) {
          float sample_rate = attrs["sample_rate"].get<float>();
          sample_interval_us = static_cast<uint16_t>(1000000.0f / sample_rate); // Convert Hz to microseconds
        } else if (attrs.contains("sample_interval")) {
          sample_interval_us = static_cast<uint16_t>(attrs["sample_interval"].get<float>() * 1000); // Convert ms to microseconds
        }
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Warning: Could not determine sample interval, using default 1ms: " << e.what() << std::endl;
  }

  // Create applied overrides for text header generation
  std::vector<TraceHeaderField> applied_overrides;
  for (const auto& field : trace_headers.fields()) {
    // Check if this field was overridden by comparing with default names
    if (field.name == "inline" || field.name == "crossline" || 
        field.name == "cdp-x" || field.name == "cdp-y") {
      applied_overrides.push_back(field);
    }
  }
  
  // Create and write SEG-Y text header (deferred until after trace processing)
  auto text_header_result = CreateSegyTextHeader(text_header, ds, mdio_path, segy_path, applied_overrides, ctx);
  if (!text_header_result.ok()) {
    return text_header_result.status(); // Early failure - stop processing
  }

  // Create and write SEG-Y binary header (deferred until after trace processing)
  auto binary_header_result = CreateSegyBinaryHeader(binary_header, ds, segy_path, 
                                                    num_traces, num_samples, sample_interval_us, 
                                                    seismic_var, ctx);
  if (!binary_header_result.ok()) {
    return binary_header_result.status(); // Early failure - stop processing
  }

  // Finalize SEG-Y file
  auto finalize_result = FinalizeSegyFile(segy_path, output_chunk_size, num_traces, ctx);
  if (!finalize_result.ok()) {
    return finalize_result.status(); // Early failure - stop processing
  }

  return absl::OkStatus();
}

/**
 * @brief Creates and writes a SEG-Y text header.
 *
 * This function handles the creation of a SEG-Y text header with fallback logic:
 * 1. Use provided text_header if it's valid (3200 bytes)
 * 2. Check dataset metadata for "text_header" field
 * 3. Generate default header with MDIO path, date, and override info
 *
 * @param text_header   Input text header string
 * @param dataset       MDIO dataset for metadata lookup
 * @param mdio_path     Path to input MDIO dataset
 * @param segy_path     Path where SEG-Y output is being written
 * @param overrides     Applied trace header overrides
 * @param ctx           TensorStore context
 * @return Status of the text header creation
 */
Result<void> CreateSegyTextHeader(
    const std::string& text_header,
    const Dataset& dataset,
    const std::string& mdio_path,
    const std::string& segy_path,
    const std::vector<TraceHeaderField>& overrides,
    const Context& ctx) {
  
  std::string final_text_header;
  
  // Helper function to validate and fix SEG-Y text header format
  auto validate_and_fix_header = [](const std::string& header) -> std::pair<bool, std::string> {
    if (header.size() != 3200) {
      return {false, ""};
    }
    
    // Check if it's properly formatted as 40 lines of 80 characters
    std::string fixed_header;
    for (int line = 0; line < 40; ++line) {
      size_t start = line * 80;
      if (start >= header.size()) break;
      
      std::string line_content = header.substr(start, 80);
      
      // Ensure line is exactly 80 characters
      if (line_content.size() < 80) {
        line_content.resize(80, ' ');
      } else if (line_content.size() > 80) {
        line_content = line_content.substr(0, 80);
      }
      
      fixed_header += line_content;
    }
    
    // Ensure exactly 3200 bytes
    if (fixed_header.size() < 3200) {
      fixed_header.resize(3200, ' ');
    } else if (fixed_header.size() > 3200) {
      fixed_header = fixed_header.substr(0, 3200);
    }
    
    return {true, fixed_header};
  };
  
  // Check if provided text header is valid (3200 bytes)
  if (text_header.size() == 3200) {
    auto [is_valid, fixed_header] = validate_and_fix_header(text_header);
    if (is_valid) {
      std::cout << "Using provided text header (3200 bytes, validated format)" << std::endl;
      final_text_header = fixed_header;
    } else {
      std::cout << "Provided text header has invalid format, checking dataset metadata..." << std::endl;
    }
  } else if (!text_header.empty()) {
    std::cout << "Provided text header invalid size (" << text_header.size() 
              << " bytes), checking dataset metadata..." << std::endl;
  } else {
    std::cout << "No text header provided, checking dataset metadata..." << std::endl;
  }
  
  // Try to get text header from dataset metadata if not already set
  if (final_text_header.empty()) {
    bool found_in_metadata = false;
    try {
      auto metadata = dataset.getMetadata();
      if (metadata.contains("attributes") && 
          metadata["attributes"].contains("text_header")) {
        std::string metadata_header = metadata["attributes"]["text_header"].get<std::string>();
        if (metadata_header.size() == 3200) {
          auto [is_valid, fixed_header] = validate_and_fix_header(metadata_header);
          if (is_valid) {
            std::cout << "Using text header from dataset metadata (validated format)" << std::endl;
            final_text_header = fixed_header;
            found_in_metadata = true;
          } else {
            std::cout << "Text header in metadata has invalid format" << std::endl;
          }
        } else {
          std::cout << "Text header in metadata has invalid size (" 
                    << metadata_header.size() << " bytes)" << std::endl;
        }
      }
    } catch (const std::exception& e) {
      std::cout << "Could not read text header from metadata: " << e.what() << std::endl;
    }
    
    // Generate default text header if not found
    if (!found_in_metadata) {
      std::cout << "Generating default text header..." << std::endl;
      
      // Get current date
      auto now = std::chrono::system_clock::now();
      auto time_t = std::chrono::system_clock::to_time_t(now);
      auto tm = *std::localtime(&time_t);
      char date_str[32];
      std::strftime(date_str, sizeof(date_str), "%Y-%m-%d %H:%M:%S", &tm);
      
      // Helper function to format a line to exactly 80 characters
      auto format_line = [](int line_num, const std::string& content) -> std::string {
        std::string line = "C" + std::to_string(line_num);
        if (line_num < 10) line = "C " + std::to_string(line_num);  // Add space for single digits
        line += " " + content;
        
        // Pad or truncate to exactly 80 characters
        if (line.length() < 80) {
          line.resize(80, ' ');
        } else if (line.length() > 80) {
          line = line.substr(0, 80);
        }
        return line;
      };
      
      // Create header lines
      std::vector<std::string> lines;
      lines.push_back(format_line(1, "SEG-Y file created from MDIO dataset"));
      lines.push_back(format_line(2, ""));
      
      // Handle potentially long MDIO path - split if necessary
      std::string path_prefix = "Source MDIO path: ";
      std::string full_path_line = path_prefix + mdio_path;
      if (full_path_line.length() <= 76) { // 80 - "C# " = 76 chars max
        lines.push_back(format_line(3, full_path_line));
        lines.push_back(format_line(4, "Created: " + std::string(date_str)));
        lines.push_back(format_line(5, ""));
      } else {
        // Split long path across multiple lines
        lines.push_back(format_line(3, path_prefix));
        
        // Find a good break point in the path
        std::string remaining_path = mdio_path;
        int current_line = 4;
        while (!remaining_path.empty() && current_line <= 39) {
          int max_path_chars = 74; // 76 - 2 for indentation
          if (remaining_path.length() <= max_path_chars) {
            lines.push_back(format_line(current_line++, "  " + remaining_path));
            break;
          } else {
            // Find break point (prefer slash or dash)
            int break_point = max_path_chars;
            for (int i = max_path_chars - 1; i >= max_path_chars - 20 && i >= 0; --i) {
              if (remaining_path[i] == '/' || remaining_path[i] == '-') {
                break_point = i + 1; // Include the separator
                break;
              }
            }
            
            std::string path_part = remaining_path.substr(0, break_point);
            lines.push_back(format_line(current_line++, "  " + path_part));
            remaining_path = remaining_path.substr(break_point);
          }
        }
        
        lines.push_back(format_line(current_line++, "Created: " + std::string(date_str)));
        lines.push_back(format_line(current_line++, ""));
      }
      
      int line_num = lines.size() + 1;
      if (!overrides.empty()) {
        lines.push_back(format_line(line_num++, "Applied trace header overrides:"));
        
        // Calculate how many overrides we can fit in remaining lines
        int available_lines = 40 - line_num;
        int overrides_to_show = std::min(static_cast<int>(overrides.size()), available_lines - 1); // -1 for potential "..." line
        
        for (int i = 0; i < overrides_to_show && line_num <= 39; ++i) {
          const auto& override = overrides[i];
          std::string override_info = "- " + override.name + " at byte " + std::to_string(override.offset);
          
          // Check if this override description fits in one line (76 chars max)
          if (override_info.length() <= 76) {
            lines.push_back(format_line(line_num++, override_info));
          } else {
            // Truncate long override descriptions
            std::string truncated = override_info.substr(0, 73) + "..."; // 76 - 3 for "..."
            lines.push_back(format_line(line_num++, truncated));
          }
        }
        
        // Add indication if there are more overrides
        if (overrides.size() > overrides_to_show && line_num <= 40) {
          int remaining_overrides = overrides.size() - overrides_to_show;
          std::string more_info = "... and " + std::to_string(remaining_overrides) + " more override(s)";
          lines.push_back(format_line(line_num++, more_info));
        }
      }
      
      // Fill remaining lines up to 40
      for (int i = line_num; i <= 40; ++i) {
        lines.push_back(format_line(i, ""));
      }
      
      // Combine all lines into final header
      std::string header_content;
      for (const auto& line : lines) {
        header_content += line;
      }
      
      // Verify we have exactly 3200 bytes (40 lines * 80 chars)
      if (header_content.size() != 3200) {
        std::cerr << "Warning: Generated text header size is " << header_content.size() 
                  << " bytes, expected 3200. Adjusting..." << std::endl;
        if (header_content.size() < 3200) {
          header_content.resize(3200, ' ');
        } else {
          header_content = header_content.substr(0, 3200);
        }
      }
      
      final_text_header = header_content;
    }
  }
  
  // Write text header to file
  std::string text_header_path = segy_path + "/text_header";
  
  // Determine storage backend for header writing
  std::string header_driver = "file";
  if (absl::StartsWith(text_header_path, "gs://")) header_driver = "gcs";
  else if (absl::StartsWith(text_header_path, "s3://")) header_driver = "s3";
  
  if (header_driver == "gcs" || header_driver == "s3") {
    // For cloud storage, write to local temp file then upload
    std::string temp_file = "/tmp/segy_text_header_" + std::to_string(std::time(nullptr));
    
    // Write to local temp file
    std::ofstream temp_stream(temp_file, std::ios::binary);
    if (!temp_stream.is_open()) {
      return absl::InternalError("Failed to create temporary text header file");
    }
    temp_stream.write(final_text_header.data(), 3200);
    temp_stream.close();
    
    // Upload to cloud storage
    std::string upload_cmd;
    if (header_driver == "gcs") {
      // Use application default credentials by temporarily unsetting the service account
      upload_cmd = "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage cp " + temp_file + " " + text_header_path;
    } else {
      upload_cmd = "aws s3 cp " + temp_file + " " + text_header_path;
    }
    
    std::cout << "Uploading text header: " << upload_cmd << std::endl;
    int result = std::system(upload_cmd.c_str());
    
    // Clean up temp file
    std::system(("rm -f " + temp_file).c_str());
    
    if (result != 0) {
      return absl::InternalError("Failed to upload text header with exit code " + std::to_string(result));
    }
    
  } else {
    // For local files, write directly
    std::ofstream file_stream(text_header_path, std::ios::binary);
    if (!file_stream.is_open()) {
      return absl::InternalError("Failed to create text header file: " + text_header_path);
    }
    file_stream.write(final_text_header.data(), 3200);
    file_stream.close();
  }
  
  std::cout << "Text header written to: " << text_header_path << std::endl;
  return absl::OkStatus();
}

/**
 * @brief Creates and writes a SEG-Y binary header.
 *
 * The binary header is a 400-byte structure containing file metadata.
 * This function creates a Rev 1 compliant binary header with the essential fields.
 *
 * @param binary_header Input binary header string (currently unused, generates default)
 * @param dataset       MDIO dataset for metadata lookup
 * @param segy_path     Path where SEG-Y output is being written
 * @param num_traces    Total number of traces in the file
 * @param num_samples   Number of samples per trace
 * @param sample_interval_us Sample interval in microseconds
 * @param seismic_var   The seismic variable for dtype information
 * @param ctx           TensorStore context
 * @return Status of the binary header creation
 */
Result<void> CreateSegyBinaryHeader(
    const std::string& binary_header,
    const Dataset& dataset,
    const std::string& segy_path,
    Index num_traces,
    Index num_samples,
    uint16_t sample_interval_us,
    const Variable<>& seismic_var,
    const Context& ctx) {
  
  (void)binary_header; // Currently unused, we generate a default header
  
  std::cout << "Creating SEG-Y binary header..." << std::endl;
  
  // Create 400-byte binary header buffer, initialized to zero
  std::vector<char> header_buffer(400, 0);
  
  // Helper function to write little-endian values to buffer
  auto write_int16 = [&](size_t offset, int16_t value) {
    if (offset + 1 < header_buffer.size()) {
      header_buffer[offset] = static_cast<char>(value & 0xFF);
      header_buffer[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
    }
  };
  
  auto write_int32 = [&](size_t offset, int32_t value) {
    if (offset + 3 < header_buffer.size()) {
      header_buffer[offset] = static_cast<char>(value & 0xFF);
      header_buffer[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
      header_buffer[offset + 2] = static_cast<char>((value >> 16) & 0xFF);
      header_buffer[offset + 3] = static_cast<char>((value >> 24) & 0xFF);
    }
  };
  
  // SEG-Y Binary Header Fields (1-based byte positions, converted to 0-based for array access)
  
  // Bytes 1-4: Job identification number
  write_int32(0, 1);
  
  // Bytes 5-8: Line number
  write_int32(4, 1);
  
  // Bytes 9-12: Reel number
  write_int32(8, 1);
  
  // Bytes 13-14: Number of data traces per ensemble (set to 1 for post-stack)
  write_int16(12, 1);
  
  // Bytes 15-16: Number of auxiliary traces per ensemble
  write_int16(14, 0);
  
  // Bytes 17-18: Sample interval in microseconds
  write_int16(16, static_cast<int16_t>(sample_interval_us));
  
  // Bytes 19-20: Sample interval of original field recording (same as above)
  write_int16(18, static_cast<int16_t>(sample_interval_us));
  
  // Bytes 21-22: Number of samples per data trace
  write_int16(20, static_cast<int16_t>(std::min(num_samples, static_cast<Index>(32767))));
  
  // Bytes 23-24: Number of samples per data trace for original field recording
  write_int16(22, static_cast<int16_t>(std::min(num_samples, static_cast<Index>(32767))));
  
  // Bytes 25-26: Data sample format code
  int16_t format_code = 1; // Default to 4-byte IBM floating point
  if (seismic_var.dtype() == constants::kFloat32) {
    format_code = 5; // 4-byte IEEE floating point
  } else if (seismic_var.dtype() == constants::kInt32) {
    format_code = 2; // 4-byte two's complement integer
  } else if (seismic_var.dtype() == constants::kInt16) {
    format_code = 3; // 2-byte two's complement integer
  } else if (seismic_var.dtype() == constants::kInt8) {
    format_code = 8; // 1-byte two's complement integer
  }
  write_int16(24, format_code);
  
  // Bytes 27-28: CDP fold (set to 1 for post-stack)
  write_int16(26, 1);
  
  // Bytes 29-30: Trace sorting code (1 = as recorded, 2 = CDP ensemble, 4 = single fold continuous profile)
  write_int16(28, 4); // Single fold continuous profile for post-stack
  
  // Bytes 31-32: Vertical sum code (1 = no sum, 2 = two sum, ..., N = N sum)
  write_int16(30, 1);
  
  // Bytes 33-34: Sweep frequency at start (Hz)
  write_int16(32, 0);
  
  // Bytes 35-36: Sweep frequency at end (Hz)
  write_int16(34, 0);
  
  // Bytes 37-38: Sweep length (ms)
  write_int16(36, 0);
  
  // Bytes 39-40: Sweep type code (1 = linear, 2 = parabolic, 3 = exponential, 4 = other)
  write_int16(38, 1);
  
  // Bytes 41-42: Trace number of sweep channel
  write_int16(40, 0);
  
  // Bytes 43-44: Sweep trace taper length at start (ms)
  write_int16(42, 0);
  
  // Bytes 45-46: Sweep trace taper length at end (ms)
  write_int16(44, 0);
  
  // Bytes 47-48: Taper type (1 = linear, 2 = cos^2, 3 = other)
  write_int16(46, 1);
  
  // Bytes 49-50: Correlated data traces (1 = no, 2 = yes)
  write_int16(48, 1);
  
  // Bytes 51-52: Binary gain recovered (1 = yes, 2 = no)
  write_int16(50, 1);
  
  // Bytes 53-54: Amplitude recovery method (1 = none, 2 = spherical divergence, 3 = AGC, 4 = other)
  write_int16(52, 1);
  
  // Bytes 55-56: Measurement system (1 = meters, 2 = feet)
  write_int16(54, 1); // Meters
  
  // Bytes 57-58: Impulse signal polarity (1 = increase in pressure/upward geophone movement = negative, 2 = positive)
  write_int16(56, 1);
  
  // Bytes 59-60: Vibratory polarity code
  write_int16(58, 1);
  
  // SEG-Y Rev 1 specific fields
  
  // Bytes 301-302: SEG-Y format revision number (always 0x0100 for Rev 1)
  write_int16(300, 0x0100);
  
  // Bytes 303-304: Fixed length trace flag (1 = all traces same length)
  write_int16(302, 1);
  
  // Bytes 305-306: Number of 3200-byte Extended Textual File Header records
  write_int16(304, 0);
  
  // Write binary header to file
  std::string binary_header_path = segy_path + "/binary_header";
  
  // Determine storage backend for header writing
  std::string header_driver = "file";
  if (absl::StartsWith(binary_header_path, "gs://")) header_driver = "gcs";
  else if (absl::StartsWith(binary_header_path, "s3://")) header_driver = "s3";
  
  if (header_driver == "gcs" || header_driver == "s3") {
    // For cloud storage, write to local temp file then upload
    std::string temp_file = "/tmp/segy_binary_header_" + std::to_string(std::time(nullptr));
    
    // Write to local temp file
    std::ofstream temp_stream(temp_file, std::ios::binary);
    if (!temp_stream.is_open()) {
      return absl::InternalError("Failed to create temporary binary header file");
    }
    temp_stream.write(header_buffer.data(), 400);
    temp_stream.close();
    
    // Upload to cloud storage
    std::string upload_cmd;
    if (header_driver == "gcs") {
      // Use application default credentials by temporarily unsetting the service account
      upload_cmd = "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage cp " + temp_file + " " + binary_header_path;
    } else {
      upload_cmd = "aws s3 cp " + temp_file + " " + binary_header_path;
    }
    
    std::cout << "Uploading binary header: " << upload_cmd << std::endl;
    int result = std::system(upload_cmd.c_str());
    
    // Clean up temp file
    std::system(("rm -f " + temp_file).c_str());
    
    if (result != 0) {
      return absl::InternalError("Failed to upload binary header with exit code " + std::to_string(result));
    }
    
  } else {
    // For local files, write directly
    std::ofstream file_stream(binary_header_path, std::ios::binary);
    if (!file_stream.is_open()) {
      return absl::InternalError("Failed to create binary header file: " + binary_header_path);
    }
    file_stream.write(header_buffer.data(), 400);
    file_stream.close();
  }
  
  std::cout << "Binary header written to: " << binary_header_path << std::endl;
  std::cout << "Binary header info:" << std::endl;
  std::cout << "  - Number of traces: " << num_traces << std::endl;
  std::cout << "  - Samples per trace: " << num_samples << std::endl;
  std::cout << "  - Sample interval: " << sample_interval_us << " microseconds" << std::endl;
  std::cout << "  - Data format code: " << format_code << " (";
  switch (format_code) {
    case 1: std::cout << "4-byte IBM floating point"; break;
    case 2: std::cout << "4-byte two's complement integer"; break;
    case 3: std::cout << "2-byte two's complement integer"; break;
    case 5: std::cout << "4-byte IEEE floating point"; break;
    case 8: std::cout << "1-byte two's complement integer"; break;
    default: std::cout << "unknown"; break;
  }
  std::cout << ")" << std::endl;
  std::cout << "  - SEG-Y Revision: 1" << std::endl;
  
  return absl::OkStatus();
}

/**
 * @brief Finalizes SEG-Y file by concatenating headers and trace data chunks.
 *
 * This function concatenates the text header, binary header, and all trace data chunks
 * into a single SEG-Y file. The approach varies by storage backend:
 * - GCS: Uses hierarchical log2 compose for Google Cloud Storage to handle 32-object limit
 * - Local Unix/Linux: Uses cat command
 * - S3/Windows: Returns error with manual instructions
 *
 * @param segy_path         Path where SEG-Y output was written
 * @param output_chunk_size Chunk size used for trace data
 * @param num_traces        Total number of traces
 * @param ctx               TensorStore context
 * @return Status of the finalization
 */
Result<void> FinalizeSegyFile(
    const std::string& segy_path,
    Index output_chunk_size,
    Index num_traces,
    const Context& ctx) {
  
  (void)ctx; // Currently unused
  
  std::cout << "Finalizing SEG-Y file..." << std::endl;
  
  // Determine storage backend
  std::string driver = "file";
  if (absl::StartsWith(segy_path, "gs://")) {
    driver = "gcs";
  } else if (absl::StartsWith(segy_path, "s3://")) {
    driver = "s3";
  }
  
  // Calculate number of chunks
  Index num_chunks = (num_traces + output_chunk_size - 1) / output_chunk_size;
  
  if (driver == "gcs") {
    // Use hierarchical log2 compose for Google Cloud Storage to handle 32-object limit
    std::cout << "Using hierarchical compose for GCS (log2 approach)..." << std::endl;
    
    // Create list of all files to compose (text_header, binary_header, chunks 0..n-1)
    std::vector<std::string> files_to_compose;
    files_to_compose.push_back(segy_path + "/text_header");
    files_to_compose.push_back(segy_path + "/binary_header");
    for (Index chunk = 0; chunk < num_chunks; ++chunk) {
      files_to_compose.push_back(segy_path + "/" + std::to_string(chunk));
    }
    
    std::cout << "Total files to compose: " << files_to_compose.size() << std::endl;
    
    // If we have 32 or fewer files, use direct compose
    if (files_to_compose.size() <= 32) {
      std::cout << "Using direct compose (≤32 files)..." << std::endl;
      
      std::ostringstream cmd;
      cmd << "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage objects compose";
      for (const auto& file : files_to_compose) {
        cmd << " " << file;
      }
      cmd << " " << segy_path << ".sgy";
      
      std::cout << "Executing: " << cmd.str() << std::endl;
      int result = std::system(cmd.str().c_str());
      if (result != 0) {
        return absl::InternalError("gcloud storage objects compose command failed with exit code " + std::to_string(result));
      }
      
    } else {
      // Use hierarchical log2 compose approach
      std::cout << "Using hierarchical compose (>32 files)..." << std::endl;
      
      std::vector<std::string> current_level = files_to_compose;
      int level = 0;
      
      while (current_level.size() > 1) {
        std::vector<std::string> next_level;
        std::cout << "Level " << level << ": composing " << current_level.size() << " files..." << std::endl;
        
        // Process files in groups of up to 32
        for (size_t i = 0; i < current_level.size(); i += 32) {
          size_t end = std::min(i + 32, current_level.size());
          size_t group_size = end - i;
          
          // Create intermediate file name
          std::string intermediate_file = segy_path + "/intermediate_L" + std::to_string(level) + "_G" + std::to_string(i/32);
          
          // Build compose command for this group
          std::ostringstream cmd;
          cmd << "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage objects compose";
          for (size_t j = i; j < end; ++j) {
            cmd << " " << current_level[j];
          }
          cmd << " " << intermediate_file;
          
          std::cout << "  Group " << (i/32) << ": composing " << group_size << " files -> " << intermediate_file << std::endl;
          
          int result = std::system(cmd.str().c_str());
          if (result != 0) {
            return absl::InternalError("gcloud storage objects compose command failed at level " + std::to_string(level) + 
                                     " group " + std::to_string(i/32) + " with exit code " + std::to_string(result));
          }
          
          next_level.push_back(intermediate_file);
          
          // Immediately clean up the source files for this group (except on first level where we keep originals until the end)
          if (level > 0) {
            std::ostringstream cleanup_cmd;
            cleanup_cmd << "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage rm";
            for (size_t j = i; j < end; ++j) {
              cleanup_cmd << " " << current_level[j];
            }
            
            std::cout << "  Cleaning up level " << level << " group " << (i/32) << " source files..." << std::endl;
            int cleanup_result = std::system(cleanup_cmd.str().c_str());
            if (cleanup_result != 0) {
              std::cout << "  Warning: Cleanup failed for level " << level << " group " << (i/32) << std::endl;
            }
          }
        }
        
        current_level = next_level;
        level++;
      }
      
      // Final step: rename the last intermediate file to the final output
      if (!current_level.empty()) {
        std::string final_intermediate = current_level[0];
        std::ostringstream rename_cmd;
        rename_cmd << "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage mv " << final_intermediate << " " << segy_path << ".sgy";
        
        std::cout << "Renaming final intermediate to output file..." << std::endl;
        int result = std::system(rename_cmd.str().c_str());
        if (result != 0) {
          return absl::InternalError("Failed to rename final intermediate file with exit code " + std::to_string(result));
        }
      }
    }
    
    std::cout << "SEG-Y file created: " << segy_path << ".sgy" << std::endl;
    
    // Clean up original files (text_header, binary_header, and all chunks)
    std::cout << "Cleaning up original files..." << std::endl;
    std::ostringstream cleanup_cmd;
    cleanup_cmd << "GOOGLE_APPLICATION_CREDENTIALS= gcloud storage rm " << segy_path << "/text_header " << segy_path << "/binary_header";
    for (Index chunk = 0; chunk < num_chunks; ++chunk) {
      cleanup_cmd << " " << segy_path << "/" << chunk;
    }
    
    std::cout << "Executing cleanup: " << cleanup_cmd.str() << std::endl;
    int cleanup_result = std::system(cleanup_cmd.str().c_str());
    if (cleanup_result != 0) {
      std::cout << "Warning: Cleanup of original files failed, some intermediate files may remain" << std::endl;
    }
    
  } else if (driver == "file") {
    // Check if we're on Windows
#ifdef _WIN32
    return absl::UnimplementedError(
        "SEG-Y finalization not implemented for Windows. "
        "Please manually concatenate the following files in order:\n"
        "1. " + segy_path + "/text_header\n"
        "2. " + segy_path + "/binary_header\n"
        "3. " + segy_path + "/0, " + segy_path + "/1, ... " + segy_path + "/" + std::to_string(num_chunks - 1) + "\n"
        "Use: copy /b text_header+binary_header+0+1+...+" + std::to_string(num_chunks - 1) + " output.sgy");
#else
    // Use cat command for Unix/Linux
    std::cout << "Using cat command for local file concatenation..." << std::endl;
    
    // Build cat command
    std::ostringstream cmd;
    cmd << "cat ";
    
    // Add text header
    cmd << "\"" << segy_path << "/text_header\" ";
    
    // Add binary header
    cmd << "\"" << segy_path << "/binary_header\" ";
    
    // Add all trace data chunks in order
    for (Index chunk = 0; chunk < num_chunks; ++chunk) {
      cmd << "\"" << segy_path << "/" << chunk << "\" ";
    }
    
    // Output file
    cmd << "> \"" << segy_path << ".sgy\"";
    
    std::cout << "Executing: " << cmd.str() << std::endl;
    
    int result = std::system(cmd.str().c_str());
    if (result != 0) {
      return absl::InternalError("cat command failed with exit code " + std::to_string(result));
    }
    
    std::cout << "SEG-Y file created: " << segy_path << ".sgy" << std::endl;
    
    // Optionally clean up intermediate files
    std::cout << "Cleaning up intermediate files..." << std::endl;
    std::ostringstream cleanup_cmd;
    cleanup_cmd << "rm ";
    cleanup_cmd << "\"" << segy_path << "/text_header\" ";
    cleanup_cmd << "\"" << segy_path << "/binary_header\" ";
    for (Index chunk = 0; chunk < num_chunks; ++chunk) {
      cleanup_cmd << "\"" << segy_path << "/" << chunk << "\" ";
    }
    
    std::cout << "Executing cleanup: " << cleanup_cmd.str() << std::endl;
    int cleanup_result = std::system(cleanup_cmd.str().c_str());
    if (cleanup_result != 0) {
      std::cout << "Warning: Cleanup command failed, intermediate files may remain" << std::endl;
    }
#endif
    
  } else if (driver == "s3") {
    // S3 doesn't have a simple compose operation like GCS
    return absl::UnimplementedError(
        "SEG-Y finalization not implemented for S3. "
        "Please use AWS CLI to manually concatenate the following files in order:\n"
        "1. Download all files: aws s3 cp " + segy_path + "/ . --recursive\n"
        "2. Concatenate locally: cat text_header binary_header 0 1 ... " + std::to_string(num_chunks - 1) + " > output.sgy\n"
        "3. Upload result: aws s3 cp output.sgy " + segy_path + ".sgy\n"
        "4. Clean up: aws s3 rm " + segy_path + "/ --recursive");
        
  } else {
    return absl::UnimplementedError("Unknown storage driver: " + driver);
  }
  
  return absl::OkStatus();
}

}  // namespace utils
}  // namespace mdio

#endif  // MDIO_UTILS_MDIO_TO_SEGY_H_