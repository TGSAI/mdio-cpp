#include <mdio/mdio.h>
#include <png.h>

#include <algorithm>
#include <indicators/cursor_control.hpp>
#include <indicators/indeterminate_progress_bar.hpp>
#include <indicators/progress_bar.hpp>
#include <sstream>
#include <thread>

#include "interpolation.h"

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

  MDIO_ASSIGN_OR_RETURN(auto inline_slice, dataset.isel(desc1))

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
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}};

  // Hide cursor while bar is active
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
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
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

  // Define these variables once at the start of the function, before both PNG
  // and numpy sections
  const int width = xline_exclusive_max - xline_inclusive_min;
  const int height = depth_exclusive_max - depth_inclusive_min;

  //

  // Dump seismic data to numpy .npy format
  std::ofstream outfile("seismic_data.npy", std::ios::binary);
  if (!outfile) {
    return absl::InvalidArgumentError("Could not open numpy file for writing");
  }

  // Write the numpy header
  // Format described at:
  // https://numpy.org/doc/stable/reference/generated/numpy.lib.format.html
  const char magic[] = "\x93NUMPY";
  const char version[] = "\x01\x00";

  // Create the header string
  std::stringstream header;
  header << "{'descr': '<f4', 'fortran_order': False, 'shape': (" << width
         << ", " << height << "), }";

  // Pad header to multiple of 16 bytes
  int header_len = header.str().length() + 1;  // +1 for null terminator
  int padding =
      16 - ((header_len + 10) % 16);  // 10 is magic + version + header_len
  for (int i = 0; i < padding; i++) {
    header << " ";
  }
  header << "\n";

  // Write header length (little endian)
  uint16_t header_size = header.str().length();

  // Write all the header components
  outfile.write(magic, 6);
  outfile.write(version, 2);
  outfile.write(reinterpret_cast<char*>(&header_size), 2);
  outfile.write(header.str().c_str(), header_size);

  // Write the actual data
  for (Index i = xline_inclusive_min; i < xline_exclusive_max; ++i) {
    for (Index j = depth_inclusive_min; j < depth_exclusive_max; ++j) {
      float val = seismic_accessor({inline_index, i, j});
      outfile.write(reinterpret_cast<const char*>(&val), sizeof(float));
    }
  }
  outfile.close();

  // For PNG section, keep using width/height
  const float upsample = 2.0f;
  const int output_width = width * upsample;
  const int output_height = height * upsample;

  // Create PNG file
  FILE* fp = fopen("seismic_slice.png", "wb");
  if (!fp) return absl::InvalidArgumentError("Could not open file for writing");

  png_structp png =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, nullptr, nullptr, nullptr);
  if (!png) return absl::InternalError("Could not create PNG write struct");

  png_infop info = png_create_info_struct(png);
  if (!info) return absl::InternalError("Could not create PNG info struct");

  if (setjmp(png_jmpbuf(png))) return absl::InternalError("PNG error occurred");

  png_init_io(png, fp);

  // Set image attributes with 8-bit depth instead of 16
  png_set_IHDR(png, info, output_width, output_height, 8, PNG_COLOR_TYPE_GRAY,
               PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT,
               PNG_FILTER_TYPE_DEFAULT);
  png_write_info(png, info);

  // Update the row vector to use 8-bit values
  std::vector<uint8_t> row(output_width);

  // seismic wavelet has a zero mean ....
  double mean = 0.0;

  // Calculate standard deviation
  double sum_squared_diff = 0.0;
  double count = 0.0;
  for (Index i = xline_inclusive_min; i < xline_exclusive_max; ++i) {
    for (Index j = depth_inclusive_min; j < depth_exclusive_max; ++j) {
      float val = seismic_accessor({inline_index, i, j});
      double diff = val - mean;
      sum_squared_diff += diff * diff;
      count += 1.0;
    }
  }
  double std_dev = std::sqrt(sum_squared_diff / count);

  // Set clipping bounds to ±2 standard deviations from mean
  float min_val = mean - 3 * std_dev;
  float max_val = mean + 3 * std_dev;

  // Update the scale factor for 8-bit range (0-255)
  float scale = 255.0f / (max_val - min_val);

  for (int j = 0; j < output_height; ++j) {
    float y = depth_inclusive_min + (j / upsample);

    for (int i = 0; i < output_width; ++i) {
      float x = xline_inclusive_min + (i / upsample);

      float val = bilinear_interpolate(
          seismic_accessor, x, y, inline_index, xline_inclusive_min,
          xline_exclusive_max, depth_inclusive_min, depth_exclusive_max);
      val = std::max(min_val, std::min(max_val, val));
      row[i] = static_cast<uint8_t>((val - min_val) * scale);
    }
    png_write_row(png, reinterpret_cast<png_bytep>(row.data()));
  }

  // Cleanup
  png_write_end(png, nullptr);
  png_destroy_write_struct(&png, &info);
  fclose(fp);

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