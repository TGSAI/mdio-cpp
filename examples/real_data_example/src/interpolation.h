#pragma once

#include <mdio/mdio.h>

using Index = mdio::Index;

template <typename T, mdio::DimensionIndex R, mdio::ArrayOriginKind OriginKind>
float bilinear_interpolate(const mdio::SharedArray<T, R, OriginKind>& accessor,
                           float x, float y, Index slice_index, Index x_min,
                           Index x_max, Index y_min, Index y_max) {
  // Clamp coordinates to valid range
  x = std::max(float(x_min), std::min(float(x_max - 1), x));
  y = std::max(float(y_min), std::min(float(y_max - 1), y));

  Index x0 = std::floor(x);
  Index y0 = std::floor(y);
  Index x1 = std::min(x0 + 1, x_max - 1);
  Index y1 = std::min(y0 + 1, y_max - 1);

  float fx = x - x0;
  float fy = y - y0;

  float c00 = accessor({slice_index, x0, y0});
  float c10 = accessor({slice_index, x1, y0});
  float c01 = accessor({slice_index, x0, y1});
  float c11 = accessor({slice_index, x1, y1});

  return (c00 * (1 - fx) * (1 - fy) + c10 * fx * (1 - fy) +
          c01 * (1 - fx) * fy + c11 * fx * fy);
}