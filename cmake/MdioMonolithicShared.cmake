# Copyright 2026 TGS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Optional monolithic shared library (MDIO_BUILD_MONOLITHIC_SHARED).
# Requires CMake 3.27+ for $<COMPILE_ONLY:...> link propagation. The archive
# de-duplication needed for the whole-archive link is handled by our own link
# wrapper (MdioWholeArchiveLink.cmake), which works with the default GNU ld /
# gold toolchain -- CMake's built-in CMP0156/CMP0179 de-duplication only applies
# to symbol-recording linkers such as LLD, so we cannot depend on it here.
cmake_minimum_required(VERSION 3.27)

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

# Route the .so link through MdioWholeArchiveLink.cmake so the entire static
# dependency closure is whole-archived into a single self-contained .so. See
# that file for the full rationale.
set(CMAKE_CXX_CREATE_SHARED_LIBRARY
  "\"${CMAKE_COMMAND}\" -P \"${CMAKE_CURRENT_LIST_DIR}/MdioWholeArchiveLink.cmake\" -- <CMAKE_CXX_COMPILER> <CMAKE_SHARED_LIBRARY_CXX_FLAGS> <LANGUAGE_COMPILE_FLAGS> <LINK_FLAGS> <CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS> <SONAME_FLAG><TARGET_SONAME> -o <TARGET> <OBJECTS> <LINK_LIBRARIES>")

add_library(mdio_monolith SHARED ${CMAKE_CURRENT_SOURCE_DIR}/monolith.cc)
set_target_properties(mdio_monolith PROPERTIES
  OUTPUT_NAME mdio_monolith
  POSITION_INDEPENDENT_CODE ON
)
# Link the full tensorstore closure into the .so. tensorstore INTERFACE targets
# already carry $<LINK_LIBRARY:WHOLE_ARCHIVE,...> for their .alwayslink driver
# objects; the blanket whole-archive above subsumes that (harmless overlap).
target_link_libraries(mdio_monolith PRIVATE ${mdio_MONOLITH_DEPS})
target_include_directories(mdio_monolith PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
  $<INSTALL_INTERFACE:include>
  ${TENSORSTORE_INCLUDE_DIRS}
)
target_compile_definitions(mdio_monolith PUBLIC MAX_NUM_SLICES=${MAX_NUM_SLICES})
target_compile_features(mdio_monolith PUBLIC cxx_std_17)

# Re-expose tensorstore/Abseil/nlohmann compile usage to consumers without
# pulling static archives back into them (ODR-safe co-loading). Kept on a
# separate INTERFACE target so COMPILE_ONLY deps never share a target with the
# .so's PRIVATE link closure.
#
# The COMPILE_ONLY deps reference tensorstore::* targets, which only exist in
# this build tree -- an installed consumer resolves headers from the vendored
# trees below instead. Wrap them in BUILD_INTERFACE so the exported target does
# not reference targets the consumer's project has never defined.
add_library(mdio_monolith_interface INTERFACE)
target_link_libraries(mdio_monolith_interface INTERFACE
  mdio_monolith
  "$<BUILD_INTERFACE:$<COMPILE_ONLY:${mdio_MONOLITH_DEPS}>>"
)

# mdio::monolith is the public alias consumers link against in-tree; the
# installed package re-creates it in mdioConfig.cmake (aliases are not exported).
add_library(mdio::monolith ALIAS mdio_monolith_interface)

# ---- Vendor the third-party headers needed to compile against mdio.h --------
# mdio's public headers include tensorstore/absl/riegeli/half/nlohmann headers
# directly, so a consumer of the installed monolith needs those header trees.
# They are laid out under include/<dep>-src[/...] (matching the historical
# mdio-cpp-installer layout) and wired onto the target's INSTALL_INTERFACE so
# find_package(mdio) consumers get them automatically (no manual -I needed).
set(_mdio_deps_dir "${FETCHCONTENT_BASE_DIR}")
if(NOT _mdio_deps_dir)
  set(_mdio_deps_dir "${CMAKE_BINARY_DIR}/_deps")
endif()

install(DIRECTORY "${_mdio_deps_dir}/tensorstore-src/tensorstore"
  DESTINATION include/tensorstore-src
  FILES_MATCHING PATTERN "*.h" PATTERN "*.inc")
install(DIRECTORY "${_mdio_deps_dir}/absl-src/absl"
  DESTINATION include/absl-src
  FILES_MATCHING PATTERN "*.h" PATTERN "*.inc")
install(DIRECTORY "${_mdio_deps_dir}/riegeli-src/riegeli"
  DESTINATION include/riegeli-src
  FILES_MATCHING PATTERN "*.h" PATTERN "*.inc")
install(DIRECTORY "${_mdio_deps_dir}/nlohmann_json-src/include/nlohmann"
  DESTINATION include/nlohmann_json-src/include
  FILES_MATCHING PATTERN "*.hpp")
install(FILES "${_mdio_deps_dir}/half-src/include/half.hpp"
  DESTINATION include/half-src/include)

target_include_directories(mdio_monolith INTERFACE
  $<INSTALL_INTERFACE:include/tensorstore-src>
  $<INSTALL_INTERFACE:include/absl-src>
  $<INSTALL_INTERFACE:include/riegeli-src>
  $<INSTALL_INTERFACE:include/nlohmann_json-src/include>
  $<INSTALL_INTERFACE:include/half-src/include>
)

install(TARGETS mdio_monolith mdio_monolith_interface EXPORT mdioTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

message(STATUS "MDIO monolithic shared library enabled --> mdio::monolith")
