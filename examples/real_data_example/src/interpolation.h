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