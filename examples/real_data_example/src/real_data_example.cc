#include <mdio/mdio.h>

#include <indicators/cursor_control.hpp>
#include <indicators/indeterminate_progress_bar.hpp>
#include <indicators/progress_bar.hpp>

#include "interpolation.h"
#include "seismic_numpy.h"
#include "seismic_png.h"
#include "tensorstore/tensorstore.h"

#define MDIO_RETURN_IF_ERROR(...) TENSORSTORE_RETURN_IF_ERROR(__VA_ARGS__)

using Index = mdio::Index;

absl::Status Run() {
  MDIO_ASSIGN_OR_RETURN(
      auto dataset,
      mdio::Dataset::Open(
          std::string("s3://tgs-opendata-poseidon/full_stack_agc.mdio"),
          mdio::constants::kOpen)
          .result())
  std::cout << dataset << std::endl;

  auto inline_index = 700;

  // Select a inline slice
  mdio::SliceDescriptor desc1 = {"inline", inline_index, inline_index + 1, 1};
  mdio::SliceDescriptor desc2 = {"crossline", 500, 700, 1};

  MDIO_ASSIGN_OR_RETURN(auto inline_slice, dataset.isel({desc1, desc2}))

  // Add seismic data reading example
  MDIO_ASSIGN_OR_RETURN(auto seismic_var,
                        inline_slice.variables.get<float>("seismic"))

  // Create and configure the progress bar
  indicators::IndeterminateProgressBar bar{
      indicators::option::BarWidth{40},
      indicators::option::Start{"["},
      indicators::option::Fill{"·"},
      indicators::option::Lead{"<==>"},
      indicators::option::End{"]"},
      indicators::option::PostfixText{"Loading seismic data..."},
      indicators::option::ForegroundColor{indicators::Color::yellow},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}},
  };

  // Clear any existing output and hide cursor
  std::cout << "\033[2K\r" << std::flush;
  indicators::show_console_cursor(false);

  // Start the async read operation
  auto future = seismic_var.Read();
  auto start_time = std::chrono::steady_clock::now();

  // Show progress bar while waiting for data
  while (!future.ready()) {
    auto current_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       current_time - start_time)
                       .count();
    std::stringstream time_str;
    time_str << "Loading seismic data... " << elapsed << "s";
    bar.set_option(indicators::option::PostfixText{time_str.str()});
    bar.tick();
    // Reduce update frequency slightly
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  // Get the result and stop the bar
  MDIO_ASSIGN_OR_RETURN(auto seismic_data, future.result())
  bar.mark_as_completed();
  bar.set_option(indicators::option::ForegroundColor{indicators::Color::green});
  bar.set_option(indicators::option::PostfixText{"Seismic data loaded"});

  // Show cursor again
  indicators::show_console_cursor(true);

  auto seismic_accessor = seismic_data.get_data_accessor();

  std::cout << seismic_data.dimensions() << std::endl;

  // Get the domain of the variable
  auto xline_inclusive_min =
      seismic_var.get_store().domain()[1].interval().inclusive_min();
  auto xline_exclusive_max =
      seismic_var.get_store().domain()[1].interval().exclusive_max();

  auto depth_inclusive_min =
      seismic_var.get_store().domain()[2].interval().inclusive_min();
  auto depth_exclusive_max =
      seismic_var.get_store().domain()[2].interval().exclusive_max();

  // Write numpy file
  MDIO_RETURN_IF_ERROR(WriteNumpy(seismic_accessor, inline_index,
                                  xline_inclusive_min, xline_exclusive_max,
                                  depth_inclusive_min, depth_exclusive_max));

  // Write PNG file
  MDIO_RETURN_IF_ERROR(WritePNG(seismic_accessor, inline_index,
                                xline_inclusive_min, xline_exclusive_max,
                                depth_inclusive_min, depth_exclusive_max));

  return absl::OkStatus();
}

int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cout << "Task failed.\n" << status << std::endl;
  } else {
    std::cout << "MDIO Example Complete.\n";
  }
  return status.ok() ? 0 : 1;
}