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

#include <mdio/mdio.h>

#include <indicators/cursor_control.hpp>
#include <indicators/indeterminate_progress_bar.hpp>
#include <indicators/progress_bar.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"
#include "interpolation.h"
#include "progress.h"
#include "seismic_numpy.h"
#include "seismic_png.h"
#include "tensorstore/tensorstore.h"

#define MDIO_RETURN_IF_ERROR(...) TENSORSTORE_RETURN_IF_ERROR(__VA_ARGS__)

using Index = mdio::Index;

ABSL_FLAG(std::string, inline_range, "{inline,700,701,1}",
          "Inline range in format {inline,start,end,step}");
ABSL_FLAG(std::string, xline_range, "{crossline,500,700,1}",
          "Crossline range in format {crossline,start,end,step}");
ABSL_FLAG(std::string, depth_range, "",
          "Optional depth range in format {depth,start,end,step}");
ABSL_FLAG(std::string, variable_name, "seismic",
          "Name of the seismic variable");
ABSL_FLAG(bool, print_dataset, false, "Print the dataset URL and return");
ABSL_FLAG(std::string, dataset_path,
          "s3://tgs-opendata-poseidon/full_stack_agc.mdio",
          "The path to the dataset");

// Make Run a template function for both the type and descriptors
template <typename T, typename... Descriptors>
absl::Status Run(const Descriptors... descriptors) {
  // New feature to print the dataset if the flag is set

  MDIO_ASSIGN_OR_RETURN(
      auto dataset,
      mdio::Dataset::Open(std::string(absl::GetFlag(FLAGS_dataset_path)),
                          mdio::constants::kOpen)
          .result())

  if (absl::GetFlag(FLAGS_print_dataset)) {
    std::cout << dataset << std::endl;
    return absl::OkStatus();  // Return early if just printing the dataset
  }

  // slice the dataset
  MDIO_ASSIGN_OR_RETURN(auto inline_slice, dataset.isel(descriptors...));

  // Get variable with template type T using the variable name from the CLI
  MDIO_ASSIGN_OR_RETURN(auto variable, inline_slice.variables.template get<T>(
                                           absl::GetFlag(FLAGS_variable_name)));

  if (variable.rank() != 3) {
    return absl::InvalidArgumentError("Seismic data must be 3D");
  }

  MDIO_ASSIGN_OR_RETURN(auto seismic_data, ReadWithProgress(variable).result())

  auto seismic_accessor = seismic_data.get_data_accessor();

  // Write numpy file
  MDIO_RETURN_IF_ERROR(WriteNumpy(seismic_accessor, "seismic_slice.npy"));

  return absl::OkStatus();
}

mdio::RangeDescriptor<> ParseRange(std::string_view range) {
  // Remove leading/trailing whitespace and braces
  if (range.empty()) {
    // FIXME - we need a better way to handle this
    return {"ignore_me", 0, 1, 1};
  }

  range.remove_prefix(range.find_first_not_of(" {"));
  range.remove_suffix(range.length() - range.find_last_not_of("} ") - 1);

  // Split by comma into parts
  std::vector<std::string_view> parts = absl::StrSplit(range, ',');

  if (parts.size() != 4) {
    throw std::runtime_error(
        "Invalid range format. Expected {label,start,end,step}");
  }

  // Clean up the label (first part) by removing quotes and spaces
  auto label = parts[0];
  label.remove_prefix(std::min(label.find_first_not_of(" \""), label.size()));
  label.remove_suffix(
      label.length() -
      std::min(label.find_last_not_of(" \"") + 1, label.size()));

  return {
      label,                             // dimension name
      std::stoi(std::string(parts[1])),  // start
      std::stoi(std::string(parts[2])),  // end
      std::stoi(std::string(parts[3]))   // step
  };
}

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  // keep me in memory
  auto inline_range = absl::GetFlag(FLAGS_inline_range);
  auto crossline_range = absl::GetFlag(FLAGS_xline_range);
  auto depth_range = absl::GetFlag(FLAGS_depth_range);

  auto desc1 = ParseRange(inline_range);
  auto desc2 = ParseRange(crossline_range);
  auto desc3 = ParseRange(depth_range);

  return Run<float>(desc1, desc2, desc3).ok() ? 0 : 1;
}