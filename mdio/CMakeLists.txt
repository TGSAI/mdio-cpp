# Copyright 2026 TGS

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

# Propagate the MAX_NUM_SLICES definition to any consumers of mdio
target_compile_definitions(mdio INTERFACE MAX_NUM_SLICES=${MAX_NUM_SLICES})

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
# Tensorstore's bazel_to_cmake (post-bzlmod rename) exposes nlohmann_json as
# `nlohmann_json_nlohmann_json` plus a companion `.alwayslink` static lib that
# the INTERFACE target links against. Both must be in the export set.
if (TARGET nlohmann_json_nlohmann_json)
  install(TARGETS nlohmann_json_nlohmann_json EXPORT mdioTargets)
endif()
if (TARGET nlohmann_json_nlohmann_json.alwayslink)
  install(TARGETS nlohmann_json_nlohmann_json.alwayslink EXPORT mdioTargets)
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

# ============ Optional monolithic shared library ============
#
# By default mdio is consumed as a header-only INTERFACE target and each
# consumer statically links its own copy of tensorstore + Abseil. That is fine
# for a single executable, but a process that dynamically loads more than one
# such consumer (e.g. several plugins via dlopen) ends up with multiple copies
# of Abseil's global state and aborts at runtime ("ODR violation in Cord").
#
# When MDIO_BUILD_MONOLITHIC_SHARED is enabled we additionally emit a single
# shared object that bundles tensorstore (and its vendored Abseil) once.
# Consumers that need ODR-safe co-loading link `mdio::monolith` instead of
# `mdio` + the tensorstore::* internal deps, so the dynamic linker maps
# tensorstore/Abseil exactly once per process.
option(MDIO_BUILD_MONOLITHIC_SHARED
  "Also build libmdio_monolith.so bundling tensorstore+Abseil for ODR-safe co-loading of multiple mdio-linked plugins in one process"
  OFF)

# Recursively walk the link closure of the given targets and collect every
# concrete STATIC_LIBRARY target. The tensorstore::* entries are INTERFACE
# aggregators, so $<LINK_LIBRARY:WHOLE_ARCHIVE,...> applied to them is a no-op;
# we need the real archives underneath so that explicit template instantiations
# (e.g. the tensorstore Spec / Zarr metadata JSON binders) and the driver
# self-registration objects are all force-included into the shared object.
# Dependency entries can be plain names, namespaced aliases (foo::bar), or
# genex-wrapped libraries such as
# $<LINK_LIBRARY:WHOLE_ARCHIVE,tensorstore_driver_zarr_driver>; we tokenize on
# genex punctuation (protecting :: in namespaced names) and keep whatever
# resolves to a real target.
function(_mdio_collect_static_archives outvar)
  set(_result "")
  set(_seen "")
  set(_stack ${ARGN})
  while(_stack)
    list(POP_FRONT _stack _t)
    if(NOT TARGET ${_t})
      continue()
    endif()
    if(_t IN_LIST _seen)
      continue()
    endif()
    list(APPEND _seen ${_t})
    get_target_property(_type ${_t} TYPE)
    if(_type STREQUAL "STATIC_LIBRARY")
      list(APPEND _result ${_t})
    endif()
    set(_deps "")
    get_target_property(_il ${_t} INTERFACE_LINK_LIBRARIES)
    if(_il)
      list(APPEND _deps ${_il})
    endif()
    if(NOT _type STREQUAL "INTERFACE_LIBRARY")
      get_target_property(_ll ${_t} LINK_LIBRARIES)
      if(_ll)
        list(APPEND _deps ${_ll})
      endif()
    endif()
    foreach(_d ${_deps})
      string(REPLACE "::" "@@NS@@" _d "${_d}")
      string(REGEX REPLACE "[$<>:,]" ";" _toks "${_d}")
      foreach(_tok ${_toks})
        string(REPLACE "@@NS@@" "::" _tok "${_tok}")
        if(_tok AND TARGET ${_tok})
          list(APPEND _stack "${_tok}")
        endif()
      endforeach()
    endforeach()
  endwhile()
  list(REMOVE_DUPLICATES _result)
  set(${outvar} ${_result} PARENT_SCOPE)
endfunction()

if(MDIO_BUILD_MONOLITHIC_SHARED)
  # The tensorstore drivers self-register through static initializers, so they
  # must be whole-archived into the shared object or the registrations get
  # stripped (and zarr/s3/gcs stores fail to open at runtime). The remaining
  # Abseil objects are pulled in transitively by normal symbol resolution, so
  # they only appear once -- which is the whole point.
  set(mdio_MONOLITH_DEPS
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_s3
    tensorstore::kvstore_gcs
    tensorstore::stack
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator::nlohmann_json_schema_validator
  )

  add_library(mdio_monolith SHARED ${CMAKE_CURRENT_SOURCE_DIR}/monolith.cc)
  set_target_properties(mdio_monolith PROPERTIES
    OUTPUT_NAME mdio_monolith
    POSITION_INDEPENDENT_CODE ON
  )
  # Resolve the interface deps down to the concrete static archives and
  # whole-archive every one of them so the shared object is self-contained.
  _mdio_collect_static_archives(mdio_MONOLITH_ARCHIVES ${mdio_MONOLITH_DEPS})
  list(LENGTH mdio_MONOLITH_ARCHIVES _n_arch)
  message(STATUS "MDIO monolith: whole-archiving ${_n_arch} static archive(s)")

  # The archives are whole-archived into this .so (PRIVATE link) so it is
  # self-contained. The interface deps are ALSO re-exposed to consumers as
  # COMPILE_ONLY usage requirements ($<COMPILE_ONLY:...> requires CMake >= 3.27):
  # this propagates the transitive tensorstore/Abseil/nlohmann include
  # directories and compile definitions to anything linking mdio::monolith,
  # WITHOUT pulling the static archives back into the consumer (which would
  # re-duplicate Abseil globals and defeat the purpose).
  target_link_libraries(mdio_monolith
    PRIVATE
      "$<LINK_LIBRARY:WHOLE_ARCHIVE,${mdio_MONOLITH_ARCHIVES}>"
    PUBLIC
      "$<COMPILE_ONLY:${mdio_MONOLITH_DEPS}>"
  )
  target_include_directories(mdio_monolith PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<INSTALL_INTERFACE:include>
    ${TENSORSTORE_INCLUDE_DIRS}
  )
  target_compile_definitions(mdio_monolith PUBLIC MAX_NUM_SLICES=${MAX_NUM_SLICES})
  target_compile_features(mdio_monolith PUBLIC cxx_std_17)

  # mdio::monolith is the public alias consumers link against.
  add_library(mdio::monolith ALIAS mdio_monolith)

  install(TARGETS mdio_monolith EXPORT mdioTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )

  message(STATUS "MDIO monolithic shared library enabled --> mdio::monolith")
endif()

# ============ End optional monolithic shared library ============


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
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
    tensorstore::kvstore_gcs
    absl::log_initialize
    absl::log_globals
)

mdio_cc_test(
  NAME
    s3_test
  SRCS
    s3_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
    tensorstore::kvstore_s3
    absl::log_initialize
    absl::log_globals
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
    tensorstore::stack
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
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
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
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
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
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::tensorstore
    tensorstore::stack
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
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
)

mdio_cc_test(
  NAME
    utils_trim_test
  SRCS
    utils/trim_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    tensorstore::kvstore_gcs
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    utils_delete_test
  SRCS
    utils/delete_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::stack
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
    tensorstore::driver_json
    tensorstore::tensorstore
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    coordinate_selector_test
  SRCS
    coordinate_selector_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::tensorstore
    tensorstore::stack
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    header_variable_test
  SRCS
    header_variable_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS}
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::tensorstore
    tensorstore::stack
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    zarr_test
  SRCS
    zarr/zarr_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_zarr3
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::tensorstore
    tensorstore::stack
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

