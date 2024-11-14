#pragma once

#include <absl/status/status.h>
#include <mdio/mdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "tensorstore/tensorstore.h"

std::string GetNumpyDtypeJson(const tensorstore::DataType& dtype) {
  using namespace tensorstore;

  // Mapping of DataTypeId to NumPy dtype JSON-like string format
  static const std::unordered_map<DataTypeId, std::string> dtype_to_numpy_json{
      {DataTypeId::int8_t, "|i1"},    {DataTypeId::uint8_t, "|u1"},
      {DataTypeId::int16_t, "<i2"},   {DataTypeId::uint16_t, "<u2"},
      {DataTypeId::int32_t, "<i4"},   {DataTypeId::uint32_t, "<u4"},
      {DataTypeId::int64_t, "<i8"},   {DataTypeId::uint64_t, "<u8"},
      {DataTypeId::float32_t, "<f4"}, {DataTypeId::float64_t, "<f8"},
  };

  // Find the matching NumPy dtype string or return "unknown" if not found
  auto it = dtype_to_numpy_json.find(dtype.id());
  if (it != dtype_to_numpy_json.end()) {
    return it->second;
  } else {
    return "unknown dtype";
  }
}

template <typename T, mdio::DimensionIndex R, mdio::ArrayOriginKind OriginKind>
absl::Status WriteNumpy(const mdio::SharedArray<T, R, OriginKind>& accessor,
                        const std::string& filename) {
  auto domain = accessor.domain();
  auto type_id = GetNumpyDtypeJson(accessor.dtype());

  if (domain.rank() != 3) {
    return absl::InvalidArgumentError("Expected a 3D array");
  }

  auto inline_inclusive_min = domain[0].inclusive_min();
  auto inline_exclusive_max = domain[0].exclusive_max();

  auto xline_inclusive_min = domain[1].inclusive_min();
  auto xline_exclusive_max = domain[1].exclusive_max();

  auto depth_inclusive_min = domain[2].inclusive_min();
  auto depth_exclusive_max = domain[2].exclusive_max();

  const int width = inline_exclusive_max - inline_inclusive_min;
  const int height = xline_exclusive_max - xline_inclusive_min;
  const int depth = depth_exclusive_max - depth_inclusive_min;

  std::ofstream outfile(filename, std::ios::binary);
  if (!outfile) {
    return absl::InvalidArgumentError("Could not open numpy file for writing");
  }

  // Write the numpy header
  // Format described at:
  // https://numpy.org/doc/stable/reference/generated/numpy.lib.format.html
  const char magic_string[] = "\x93NUMPY\x01\x00";  // Magic string and version
  outfile.write(magic_string, sizeof(magic_string) - 1);  // Write as byte array

  // Construct the header
  std::stringstream header;
  header << "{'descr': '" << type_id << "', 'fortran_order': False, 'shape': ("
         << width << ", " << height << ", " << depth << ")}";

  // Pad header to multiple of 64 bytes
  int header_len = header.str().length() + 1;  // +1 for newline
  int pad_len = 64 - (header_len % 64);
  if (pad_len < 1) pad_len += 64;
  std::string padding(pad_len - 1, ' ');
  header << padding << "\n";

  // Write header length and header
  uint16_t header_size = header.str().length();
  outfile.write(reinterpret_cast<char*>(&header_size), sizeof(header_size));
  outfile.write(header.str().c_str(), header_size);

  // Write the data
  for (int il = inline_inclusive_min; il < inline_exclusive_max; ++il) {
    for (int xl = xline_inclusive_min; xl < xline_exclusive_max; ++xl) {
      for (int zl = depth_inclusive_min; zl < depth_exclusive_max; ++zl) {
        T value = accessor(il, xl, zl);
        outfile.write(reinterpret_cast<const char*>(&value), sizeof(T));
      }
    }
  }

  outfile.close();
  return absl::OkStatus();
}