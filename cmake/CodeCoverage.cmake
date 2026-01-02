# Copyright 2024 TGS
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

# ==============================================================================
# Code Coverage Support (gcov + lcov)
# ==============================================================================
#
# Coverage instrumentation is applied ONLY to mdio test targets, not to
# dependencies like Tensorstore. This keeps build times and test execution fast.
#
# REQUIRED CMAKE FLAGS:
#   -DMDIO_ENABLE_COVERAGE=ON    Enable coverage instrumentation
#   -DCMAKE_BUILD_TYPE=Debug     Recommended for meaningful line coverage
#
# REQUIRED SYSTEM TOOLS:
#   - gcov   (usually bundled with GCC)
#   - lcov   (install via: apt install lcov / brew install lcov)
#   - genhtml (included with lcov)
#
# USAGE:
#   1. Configure and build with coverage enabled:
#        cd build
#        cmake .. -DMDIO_ENABLE_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug
#        make
#
#   2. Run tests to generate coverage data (coverage accumulates across runs):
#        ./mdio/mdio_variable_test                              # single test
#        ./mdio/mdio_variable_test && ./mdio/mdio_dataset_test  # multiple tests
#        ctest                                                  # all registered tests
#
#   3. Generate HTML coverage report:
#        make coverage-capture
#
#   4. View the report:
#        Open build/coverage_report/index.html in a browser
#
# AVAILABLE TARGETS:
#   make coverage         - Reset counters, capture data, generate report
#   make coverage-capture - Capture current data and generate report (no reset)
#   make coverage-reset   - Zero out all coverage counters
#
# ==============================================================================

option(MDIO_ENABLE_COVERAGE "Enable code coverage instrumentation (requires GCC or Clang)" OFF)

if(MDIO_ENABLE_COVERAGE)
  message(STATUS "Code coverage enabled")

  # Check for supported compiler
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(FATAL_ERROR "Code coverage requires GCC or Clang compiler")
  endif()

  # Coverage compiler/linker flags - exported for use in MdioHelpers.cmake
  # These are applied only to mdio targets, not to dependencies like Tensorstore
  set(MDIO_COVERAGE_COMPILE_FLAGS "-fprofile-arcs;-ftest-coverage" CACHE INTERNAL "Coverage compile flags")
  set(MDIO_COVERAGE_LINK_FLAGS "--coverage" CACHE INTERNAL "Coverage link flags")

  # Find required tools
  find_program(LCOV_PATH lcov)
  find_program(GENHTML_PATH genhtml)

  if(NOT LCOV_PATH)
    message(WARNING "lcov not found - coverage report generation will not be available")
  endif()

  if(NOT GENHTML_PATH)
    message(WARNING "genhtml not found - coverage report generation will not be available")
  endif()

  # Create coverage report target if tools are available
  if(LCOV_PATH AND GENHTML_PATH)
    # Custom target to generate coverage report
    add_custom_target(coverage
      COMMENT "Generating code coverage report..."

      # Clear previous coverage data
      COMMAND ${LCOV_PATH} --directory ${CMAKE_BINARY_DIR} --zerocounters

      # Run all tests (ctest must be run separately or you can uncomment below)
      # COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure

      # Capture coverage data
      COMMAND ${LCOV_PATH} 
        --directory ${CMAKE_BINARY_DIR}
        --capture
        --output-file ${CMAKE_BINARY_DIR}/coverage.info
        --ignore-errors mismatch,negative

      # Remove coverage data for external dependencies
      COMMAND ${LCOV_PATH}
        --remove ${CMAKE_BINARY_DIR}/coverage.info
        '/usr/*'
        '${CMAKE_BINARY_DIR}/_deps/*'
        '*/googletest/*'
        '*/gtest/*'
        '*/gmock/*'
        '*_test.cc'
        --output-file ${CMAKE_BINARY_DIR}/coverage.info
        --ignore-errors unused,negative

      # Generate HTML report
      COMMAND ${GENHTML_PATH}
        ${CMAKE_BINARY_DIR}/coverage.info
        --output-directory ${CMAKE_BINARY_DIR}/coverage_report
        --title "MDIO Code Coverage"
        --legend
        --show-details

      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )

    # Target to just capture coverage (without zeroing first)
    add_custom_target(coverage-capture
      COMMENT "Capturing coverage data..."

      COMMAND ${LCOV_PATH} 
        --directory ${CMAKE_BINARY_DIR}
        --capture
        --output-file ${CMAKE_BINARY_DIR}/coverage.info
        --ignore-errors mismatch,negative

      COMMAND ${LCOV_PATH}
        --remove ${CMAKE_BINARY_DIR}/coverage.info
        '/usr/*'
        '${CMAKE_BINARY_DIR}/_deps/*'
        '*/googletest/*'
        '*/gtest/*'
        '*/gmock/*'
        '*_test.cc'
        --output-file ${CMAKE_BINARY_DIR}/coverage.info
        --ignore-errors unused,negative

      COMMAND ${GENHTML_PATH}
        ${CMAKE_BINARY_DIR}/coverage.info
        --output-directory ${CMAKE_BINARY_DIR}/coverage_report
        --title "MDIO Code Coverage"
        --legend
        --show-details

      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )

    # Target to reset coverage counters
    add_custom_target(coverage-reset
      COMMENT "Resetting coverage counters..."
      COMMAND ${LCOV_PATH} --directory ${CMAKE_BINARY_DIR} --zerocounters
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )

    message(STATUS "Coverage targets available: 'coverage', 'coverage-capture', 'coverage-reset'")
    message(STATUS "Coverage report will be generated at: ${CMAKE_BINARY_DIR}/coverage_report/index.html")
  endif()
endif()

