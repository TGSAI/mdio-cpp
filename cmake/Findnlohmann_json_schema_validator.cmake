include(FetchContent)

# Check if nlohmann_json target is available
if (NOT TARGET nlohmann_json::nlohmann_json)
  message(FATAL_ERROR "nlohmann_json target not found. Ensure Tensorstore provides it.")
endif()

# Fetch nlohmann_json_schema_validator if not already defined
if (NOT TARGET nlohmann_json_schema_validator)
  FetchContent_Declare(
    nlohmann_json_schema_validator
    GIT_REPOSITORY https://github.com/pboettch/json-schema-validator.git
    GIT_TAG 2.4.0
  )

  if(NOT BUILD_VALIDATOR)
    set(JSON_VALIDATOR_INSTALL OFF CACHE BOOL "Disable json validator install" FORCE)
  endif()

  FetchContent_MakeAvailable(nlohmann_json_schema_validator)

  add_library(nlohmann_json_schema_validator::nlohmann_json_schema_validator ALIAS nlohmann_json_schema_validator)

  target_include_directories(nlohmann_json_schema_validator INTERFACE
    $<BUILD_INTERFACE:${nlohmann_json_schema_validator_SOURCE_DIR}/include>
  )

  target_link_libraries(nlohmann_json_schema_validator INTERFACE nlohmann_json::nlohmann_json)

  # The upstream target carries a PUBLIC_HEADER property set to the relative
  # path "nlohmann/json-schema.hpp". When we call install(TARGETS ...) from the
  # mdio subdirectory, CMake resolves that relative path against the mdio source
  # dir (.../mdio/nlohmann/json-schema.hpp, which does not exist) and would also
  # flatten it to include/json-schema.hpp, breaking #include <nlohmann/...>.
  # Clear it and install the header explicitly, preserving the nlohmann/ prefix.
  set_target_properties(nlohmann_json_schema_validator PROPERTIES PUBLIC_HEADER "")

  # Install the validator target
  install(TARGETS nlohmann_json_schema_validator
    EXPORT mdioTargets
  )

  install(FILES
    "${nlohmann_json_schema_validator_SOURCE_DIR}/src/nlohmann/json-schema.hpp"
    DESTINATION include/nlohmann
  )

  message(STATUS "Found json schema validator library")
endif()
