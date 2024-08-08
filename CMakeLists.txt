# Copyright 2024 TGS

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
project(mdio LANGUAGES CXX)

# Compiler id for Apple Clang is now AppleClang.
if (POLICY CMP0025)
  cmake_policy(SET CMP0025 NEW)
endif (POLICY CMP0025)

# Project version variables are the empty string if version is unspecified
if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

# if command can use IN_LIST
if (POLICY CMP0057)
  cmake_policy(SET CMP0057 NEW)
endif (POLICY CMP0057)

# if command can use TEST
if (POLICY CMP0064)
  cmake_policy(SET CMP0064 NEW)
endif (POLICY CMP0064)

if (POLICY CMP0077)
  cmake_policy(SET CMP0077 NEW)
endif (POLICY CMP0077)

# option() honor variables
if (POLICY CMP0079)
  cmake_policy(SET CMP0079 NEW)
endif (POLICY CMP0079)

# Allow the user to specify the MSVC runtime
if (POLICY CMP0091)
  cmake_policy(SET CMP0091 NEW)
endif (POLICY CMP0091)

# This makes sure the FetchContent sets the timestamp to the 
# time of extraction. If we change the url it will rebuild, even
# if we change to an older version of the library.
if (POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif (POLICY CMP0135)

SET(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_STANDARD_REQUIRED ON )
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
    SET(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/rel)
endif()

list(APPEND CMAKE_MODULE_PATH
  ${CMAKE_CURRENT_LIST_DIR}/cmake
)
# generally speaking, tests are good:
SET(BUILD_TESTING ON)

include(CTest)
include(CMakePackageConfigHelpers)
include(CMakeDependentOption)
# provides a method to download dependencies:
include(FetchContent)
include(CMakeHelpers/MdioHelpers)

list(APPEND mdio_DEFAULT_COPTS
    "-Wno-deprecated-declarations"
    "-Wno-sign-compare"
    "-Wno-unused-but-set-parameter"
    "-Wno-maybe-uninitialized"
    "-Wno-unknown-warning-option")

list(APPEND mdio_TEST_COPTS
    "-Wno-deprecated-declarations"
    "-Wno-sign-compare"
    "-Wno-unused-but-set-parameter"
    "-Wno-maybe-uninitialized"
    "-Wno-unknown-warning-option")

# Define the internal dependencies that should be linked
if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  # This is not the top-level project
  set(mdio_INTERNAL_DEPS
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
    tensorstore::driver_array
    PARENT_SCOPE
  )
endif()

# Define internal deps for cloud specific drivers
set(mdio_INTERNAL_GCS_DRIVER_DEPS
  tensorstore::kvstore_gcs
  PARENT_SCOPE  
)

set(mdio_INTERNAL_S3_DRIVER_DEPS
  tensorstore::kvstore_s3
  PARENT_SCOPE
)

list(APPEND mdio_COMMON_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(mdio)

