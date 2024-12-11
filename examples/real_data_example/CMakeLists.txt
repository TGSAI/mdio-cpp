cmake_minimum_required(VERSION 3.24)
project(real_data_example_project VERSION 1.0.0 LANGUAGES CXX)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include FetchContent module
include(FetchContent)

# Fetch the mdio-cpp library from the specified branch
FetchContent_Declare(
  mdio
  GIT_REPOSITORY https://github.com/TGSAI/mdio-cpp.git
  GIT_TAG main
)
FetchContent_MakeAvailable(mdio)

# Fetch the indicators library
FetchContent_Declare(
  indicators
  GIT_REPOSITORY https://github.com/p-ranav/indicators.git
  GIT_TAG v2.3
)
FetchContent_MakeAvailable(indicators)

# Create an executable target
add_executable(real_data_example src/real_data_example.cc)

# Link the mdio and indicators libraries to the executable
target_link_libraries(real_data_example PRIVATE 
  mdio
  ${mdio_INTERNAL_DEPS}
  ${mdio_INTERNAL_S3_DRIVER_DEPS}
  ${mdio_INTERNAL_GCS_DRIVER_DEPS}
  indicators
)