cmake_minimum_required(VERSION 3.24)
project(mdio VERSION 1.0.0 LANGUAGES CXX)

include(FetchContent)

# Find Tensorstore using the custom find module
find_package(EXT_TENSORSTORE REQUIRED)
get_target_property(TENSORSTORE_INCLUDE_DIRS tensorstore::tensorstore INTERFACE_INCLUDE_DIRECTORIES)

# Ensure the nlohmann_json target is available
if (TARGET nlohmann_json)
  set(NLOHMANN_JSON_TARGET nlohmann_json)
elseif (TARGET nlohmann_json::nlohmann_json)
  set(NLOHMANN_JSON_TARGET nlohmann_json::nlohmann_json)
else()
  message(FATAL_ERROR "nlohmann_json target not found. Ensure Tensorstore provides it.")
endif()

# Include the custom find module for the JSON Schema Validator
include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/Findnlohmann_json_schema_validator.cmake)

# ============ Installable library ============

# Create mdio target as an interface library
add_library(mdio INTERFACE)

# Include directories for the mdio target
target_include_directories(mdio INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
  $<INSTALL_INTERFACE:include>
  ${TENSORSTORE_INCLUDE_DIRS}
)

# Link dependencies
target_link_libraries(mdio INTERFACE
  EXT_TENSORSTORE::tensorstore
  nlohmann_json_schema_validator::nlohmann_json_schema_validator
)

# Install the mdio target
install(TARGETS mdio EXPORT mdioTargets)

# Install include directories
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ DESTINATION include/mdio)

# Ensure the original nlohmann_json target is part of the export set
if (TARGET nlohmann_json)
  install(TARGETS nlohmann_json EXPORT mdioTargets)
endif()
# Ensure the original nlohmann_json target is part of the export set
if (TARGET nlohmann_json_json)
  install(TARGETS nlohmann_json_json EXPORT mdioTargets)
endif()

# Export the targets to be used with FetchContent
export(EXPORT mdioTargets FILE "${CMAKE_CURRENT_BINARY_DIR}/mdioTargets.cmake" NAMESPACE mdio::)

# Create a package configuration file for find_package
include(CMakePackageConfigHelpers)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/mdioConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/mdioConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/mdioConfig.cmake"
  INSTALL_DESTINATION lib/cmake/mdio
)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/mdioConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/mdioConfigVersion.cmake"
  DESTINATION lib/cmake/mdio
)

install(EXPORT mdioTargets
  FILE mdioTargets.cmake
  NAMESPACE mdio::
  DESTINATION lib/cmake/mdio
)

# ============ End installable library ============


mdio_cc_test(
  NAME
    gcs_test
  SRCS
    gcs_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
    tensorstore::kvstore_gcs
)

mdio_cc_test(
  NAME
    variable_collection_test
  SRCS
    variable_collection_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
)

mdio_cc_test(
  NAME
    dataset_validator_test
  SRCS
    dataset_validator_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::tensorstore
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    dataset_factory_test
  SRCS
    dataset_factory_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
  GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    acceptance_test
  SRCS
    acceptance_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    dataset_test
  SRCS
    dataset_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    variable_test
  SRCS
    variable_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
)

mdio_cc_test(
  NAME
    utils_test
  SRCS
    utils_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    tensorstore::kvstore_gcs
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    stats_test
  SRCS
    stats_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)
